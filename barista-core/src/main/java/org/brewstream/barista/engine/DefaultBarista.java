/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brewstream.barista.engine;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import org.brewstream.barista.Barista;
import org.brewstream.barista.Brew;
import org.brewstream.barista.BrewListener;
import org.brewstream.barista.BrewState;
import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.NodeInfo;
import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.Endpoint;
import org.brewstream.barista.spec.OutputEndpoint;
import org.brewstream.barista.spec.OutputSpec;
import org.brewstream.barista.spec.RtpReceiveEndpoint;
import org.brewstream.barista.spec.SourceEndpoint;
import org.brewstream.barista.spec.SourceId;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.barista.spi.BrewRepository;
import org.brewstream.barista.spi.NodeIdentity;
import org.brewstream.barista.spi.PortAllocator;
import org.brewstream.barista.spi.PortKind;
import org.brewstream.press.net.PressTransport;
import org.brewstream.roast.socket.SrtTransport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The engine: runs this node's brews on a shared Netty event loop group, keeps
 * their specs in a {@link BrewRepository}, and takes their ports from a
 * {@link PortAllocator}.
 *
 * <p>The group is lent, never owned: whoever made it shuts it down after
 * {@link #close()}. Every change runs on one management thread; listeners are
 * called on another; callers dial on a third; and data never touches any of
 * them.
 *
 * <p>Call {@link #start()} once to bring back the brews stored for this node.
 */
public final class DefaultBarista implements Barista {

    private static final Logger LOG = Logger.getLogger(DefaultBarista.class.getName());

    private final BaristaSettings settings;
    private final EventLoopGroup group;
    private final Class<? extends DatagramChannel> channelType;
    private final BrewRepository repository;
    private final PortAllocator ports;
    private final NodeIdentity identity;
    private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

    private final Map<BrewId, RunningBrew> brews = new ConcurrentHashMap<>();
    /** The port reservations each brew actually holds. Touched only on the management thread. */
    private final Map<BrewId, List<Reservation>> owned = new ConcurrentHashMap<>();
    private final Map<EventLoop, SrtTransport> srtTransports = new ConcurrentHashMap<>();
    private final Map<EventLoop, PressTransport> pressTransports = new ConcurrentHashMap<>();
    private final List<BrewListener> listeners = new CopyOnWriteArrayList<>();

    private final ExecutorService management;
    private final ExecutorService notifications;
    private final ScheduledExecutorService dialer;
    private volatile Thread managementThread;
    private volatile boolean closed;

    public DefaultBarista(BaristaSettings settings, EventLoopGroup group,
            Class<? extends DatagramChannel> channelType, BrewRepository repository, PortAllocator ports,
            NodeIdentity identity) {
        this.settings = settings;
        this.group = group;
        this.channelType = channelType;
        this.repository = repository;
        this.ports = ports;
        this.identity = identity;
        this.management = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "barista-management");
            thread.setDaemon(true);
            managementThread = thread;
            return thread;
        });
        this.notifications = Executors.newSingleThreadExecutor(daemon("barista-notify"));
        this.dialer = Executors.newSingleThreadScheduledExecutor(daemon("barista-dialer"));
    }

    /**
     * Restores this node's stored brews: their ports are reserved as stored, and
     * the enabled ones are started. A brew whose port can no longer be had comes
     * back FAILED, with the reason, holding no ports, and the others start
     * normally. {@link #start(BrewId)} tries its ports again.
     */
    public void start() {
        onManagement(() -> {
            for (BrewSpec spec : repository.load(identity.nodeId())) {
                RunningBrew brew = new RunningBrew(spec, engine, events);
                brews.put(spec.id(), brew);
                owned.put(spec.id(), List.of());
                Placement placement;
                try {
                    placement = place(spec, List.of());
                } catch (IllegalArgumentException e) {
                    LOG.warning("brew " + spec.id() + " cannot have its stored ports: " + e.getMessage());
                    brew.fail("stored ports unavailable: " + e.getMessage());
                    continue;
                }
                owned.put(spec.id(), placement.held());
                if (spec.enabled()) {
                    brew.start();
                }
            }
            return null;
        });
    }

    @Override
    public NodeInfo node() {
        int running = (int) brews.values().stream()
                .filter(b -> b.state() == BrewState.RUNNING || b.state() == BrewState.SOURCE_LOST).count();
        return new NodeInfo(identity.nodeId(), identity.publishedHost(), brews.size(), running);
    }

    @Override
    public Brew create(BrewSpec spec) {
        return onManagement(() -> {
            if (brews.containsKey(spec.id())) {
                throw new IllegalArgumentException("brew " + spec.id() + " already exists");
            }
            Placement placement = place(spec, List.of());
            save(placement);
            owned.put(spec.id(), placement.held());
            RunningBrew brew = new RunningBrew(placement.spec(), engine, events);
            brews.put(spec.id(), brew);
            if (spec.enabled()) {
                brew.start();
            }
            return brew;
        });
    }

    @Override
    public Optional<Brew> brew(BrewId id) {
        return Optional.ofNullable(brews.get(id));
    }

    @Override
    public Collection<Brew> brews() {
        return List.copyOf(brews.values());
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@code enabled} is honoured too: an update that enables a stopped brew
     * starts it, and one that disables a running brew stops it, so the stored
     * spec always says what is running.
     */
    @Override
    public Brew update(BrewSpec spec) {
        return onManagement(() -> {
            RunningBrew brew = existing(spec.id());
            Placement placement = place(keepPorts(brew.spec(), spec), owned.getOrDefault(spec.id(), List.of()));
            save(placement);
            commit(spec.id(), placement);
            boolean running = brew.state() != BrewState.STOPPED && brew.state() != BrewState.FAILED;
            if (!placement.spec().enabled() && running) {
                brew.stop();
            }
            brew.update(placement.spec());
            if (placement.spec().enabled() && !running) {
                brew.start();
            }
            return brew;
        });
    }

    @Override
    public void stop(BrewId id) {
        onManagement(() -> {
            RunningBrew brew = existing(id);
            BrewSpec disabled = brew.spec().withEnabled(false);
            repository.save(identity.nodeId(), disabled);
            brew.stop();
            brew.update(disabled);
            return null;
        });
    }

    /** Starts a stopped or failed brew, first reserving again any ports it does not hold. */
    @Override
    public void start(BrewId id) {
        onManagement(() -> {
            RunningBrew brew = existing(id);
            BrewSpec enabled = brew.spec().withEnabled(true);
            Placement placement = place(enabled, owned.getOrDefault(id, List.of()));
            save(placement);
            commit(id, placement);
            brew.update(placement.spec());
            brew.start();
            return null;
        });
    }

    @Override
    public void activate(BrewId id, SourceId source) {
        onManagement(() -> {
            existing(id).activate(source);
            return null;
        });
    }

    /**
     * Stops the brew, forgets its spec, then releases its ports, in that order: if
     * the repository refuses, the spec is still stored and its ports are still
     * held, so nothing can be handed out twice.
     */
    @Override
    public void delete(BrewId id) {
        onManagement(() -> {
            RunningBrew brew = existing(id);
            brew.stop();
            repository.delete(identity.nodeId(), id);
            brews.remove(id);
            release(owned.remove(id));
            return null;
        });
    }

    @Override
    public void addListener(BrewListener listener) {
        listeners.add(listener);
    }

    /** Stops every brew, leaving their stored specs untouched, so a restart brings them back. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            onManagement(() -> {
                brews.values().forEach(RunningBrew::stop);
                return null;
            });
        } finally {
            closed = true;
            dialer.shutdownNow();
            management.shutdown();
            notifications.shutdown();
            try {
                notifications.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * A port a brew actually holds. Ports are released by reservation, never by
     * what a spec names: a spec can name a port another brew holds (after a failed
     * restore), and releasing by spec would hand it out twice. Changes are
     * transactional around the repository: reserve new ports, save, and only then
     * release the ones no longer needed; a refused save gives the new ones back.
     */
    private record Reservation(PortKind kind, int port) {
    }

    /**
     * A spec with every listening port filled in, the reservations newly taken
     * for it, and every reservation it holds once committed (new plus kept).
     */
    private record Placement(BrewSpec spec, List<Reservation> acquired, List<Reservation> held) {
    }

    /**
     * Fills in ports of 0, reserves named ports the brew does not already hold,
     * and keeps those it does. On failure, returns whatever it took and throws
     * {@link IllegalArgumentException}.
     */
    private Placement place(BrewSpec spec, List<Reservation> alreadyHeld) {
        List<Reservation> acquired = new ArrayList<>();
        List<Reservation> held = new ArrayList<>();
        try {
            List<SourceSpec> sources = new ArrayList<>();
            for (SourceSpec source : spec.sources()) {
                sources.add(source.withEndpoint(
                        (SourceEndpoint) place(source.endpoint(), alreadyHeld, acquired, held)));
            }
            List<OutputSpec> outputs = new ArrayList<>();
            for (OutputSpec output : spec.outputs()) {
                outputs.add(output.withEndpoint(
                        (OutputEndpoint) place(output.endpoint(), alreadyHeld, acquired, held)));
            }
            return new Placement(spec.withSources(sources).withOutputs(outputs), acquired, held);
        } catch (RuntimeException e) {
            release(acquired);
            throw e instanceof IllegalStateException ? new IllegalArgumentException(e.getMessage(), e) : e;
        }
    }

    private Endpoint place(Endpoint endpoint, List<Reservation> alreadyHeld, List<Reservation> acquired,
            List<Reservation> held) {
        PortKind kind = kind(endpoint);
        if (kind == null) {
            return endpoint;
        }
        int port = port(endpoint);
        if (port == 0) {
            Reservation taken = new Reservation(kind, ports.allocate(kind));
            acquired.add(taken);
            held.add(taken);
            return withPort(endpoint, taken.port());
        }
        Reservation wanted = new Reservation(kind, port);
        if (!alreadyHeld.contains(wanted) && !held.contains(wanted)) {
            ports.reserve(kind, port);
            acquired.add(wanted);
        }
        if (!held.contains(wanted)) {
            held.add(wanted);
        }
        return endpoint;
    }

    /** Saves a placement's spec, returning its new reservations if the repository refuses. */
    private void save(Placement placement) {
        try {
            repository.save(identity.nodeId(), placement.spec());
        } catch (RuntimeException e) {
            release(placement.acquired());
            throw e;
        }
    }

    /** After a successful save: releases what the brew no longer needs, and records what it holds. */
    private void commit(BrewId id, Placement placement) {
        List<Reservation> before = owned.getOrDefault(id, List.of());
        release(before.stream().filter(r -> !placement.held().contains(r)).toList());
        owned.put(id, placement.held());
    }

    private void release(List<Reservation> reservations) {
        if (reservations != null) {
            reservations.forEach(r -> ports.release(r.kind(), r.port()));
        }
    }

    /** An update keeps the ports a leg already has when it does not name one itself. */
    private static BrewSpec keepPorts(BrewSpec previous, BrewSpec next) {
        List<SourceSpec> sources = new ArrayList<>();
        for (SourceSpec source : next.sources()) {
            SourceSpec old = previous.sources().stream().filter(s -> s.id().equals(source.id())).findFirst()
                    .orElse(null);
            sources.add(old == null ? source
                    : source.withEndpoint((SourceEndpoint) inheritPort(old.endpoint(), source.endpoint())));
        }
        List<OutputSpec> outputs = new ArrayList<>();
        for (OutputSpec output : next.outputs()) {
            OutputSpec old = previous.outputs().stream().filter(o -> o.id().equals(output.id())).findFirst()
                    .orElse(null);
            outputs.add(old == null ? output
                    : output.withEndpoint((OutputEndpoint) inheritPort(old.endpoint(), output.endpoint())));
        }
        return next.withSources(sources).withOutputs(outputs);
    }

    private static Endpoint inheritPort(Endpoint old, Endpoint next) {
        if (kind(next) != null && port(next) == 0 && kind(old) == kind(next)) {
            return withPort(next, port(old));
        }
        return next;
    }

    private static PortKind kind(Endpoint endpoint) {
        return switch (endpoint) {
            case SrtListenerEndpoint srt -> PortKind.SRT;
            case RtpReceiveEndpoint rtp -> PortKind.RTP_BLOCK;
            default -> null;
        };
    }

    private static int port(Endpoint endpoint) {
        return switch (endpoint) {
            case SrtListenerEndpoint srt -> srt.port();
            case RtpReceiveEndpoint rtp -> rtp.port();
            default -> 0;
        };
    }

    private static Endpoint withPort(Endpoint endpoint, int port) {
        return switch (endpoint) {
            case SrtListenerEndpoint srt -> srt.withPort(port);
            case RtpReceiveEndpoint rtp -> rtp.withPort(port);
            default -> endpoint;
        };
    }


    private RunningBrew existing(BrewId id) {
        RunningBrew brew = brews.get(id);
        if (brew == null) {
            throw new IllegalArgumentException("no brew " + id);
        }
        return brew;
    }

    /**
     * Runs a change on the management thread and waits for it, unwrapping its
     * exception. Refuses to wait on one of Barista's own event loops: the change
     * may need that loop to bind or close a socket, and the loop would be stuck
     * waiting for the change.
     */
    private <T> T onManagement(Callable<T> change) {
        if (closed) {
            throw new IllegalStateException("Barista is closed");
        }
        if (onOwnEventLoop()) {
            throw new IllegalStateException("Barista cannot be changed from one of its own event loops: the change "
                    + "may need this loop and would wait for it forever. Hand the call to another thread.");
        }
        if (Thread.currentThread() == managementThread) {
            try {
                return change.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        try {
            return management.submit(change).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for a change", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    private boolean onOwnEventLoop() {
        for (io.netty.util.concurrent.EventExecutor executor : group) {
            if (executor.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    private void notifyListeners(Consumer<BrewListener> event) {
        if (listeners.isEmpty() || notifications.isShutdown()) {
            return;
        }
        notifications.execute(() -> {
            for (BrewListener listener : listeners) {
                try {
                    event.accept(listener);
                } catch (RuntimeException e) {
                    LOG.log(Level.WARNING, "brew listener threw", e);
                }
            }
        });
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private final RunningBrew.Events events = new RunningBrew.Events() {
        @Override
        public void brewStateChanged(BrewId brew, BrewState from, BrewState to) {
            notifyListeners(l -> l.onBrewStateChanged(brew, from, to));
        }

        @Override
        public void endpointStateChanged(BrewId brew, String endpointId, EndpointState from, EndpointState to) {
            notifyListeners(l -> l.onEndpointStateChanged(brew, endpointId, from, to));
        }

        @Override
        public void sourceActivated(BrewId brew, SourceId source, String reason) {
            notifyListeners(l -> l.onSourceActivated(brew, source, reason));
        }

        @Override
        public void endpointEvent(BrewId brew, String endpointId, EndpointEvent event) {
            notifyListeners(l -> l.onEndpointEvent(brew, endpointId, event));
        }
    };

    private final RunningBrew.Engine engine = new RunningBrew.Engine() {
        @Override
        public BaristaSettings settings() {
            return settings;
        }

        @Override
        public String publishedHost() {
            return identity.publishedHost();
        }

        @Override
        public ByteBufAllocator allocator() {
            return allocator;
        }

        @Override
        public EventLoop nextLoop() {
            return group.next();
        }

        @Override
        public void manage(Runnable change) {
            if (closed) {
                return;
            }
            try {
                management.execute(() -> {
                    try {
                        change.run();
                    } catch (RuntimeException e) {
                        LOG.log(Level.WARNING, "a change Barista made by itself failed", e);
                    }
                });
            } catch (RejectedExecutionException e) {
                // closing: the change no longer matters
            }
        }

        @Override
        public SrtTransport srtTransport(EventLoop loop) {
            return srtTransports.computeIfAbsent(loop, l -> SrtTransport.shared(l, channelType));
        }

        @Override
        public PressTransport pressTransport(EventLoop loop) {
            return pressTransports.computeIfAbsent(loop, l -> PressTransport.shared(l, channelType));
        }

        @Override
        public ScheduledExecutorService dialer() {
            return dialer;
        }
    };
}