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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import org.brewstream.barista.Brew;
import org.brewstream.barista.BrewState;
import org.brewstream.barista.BrewStatus;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;
import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.OutputId;
import org.brewstream.barista.spec.OutputSpec;
import org.brewstream.barista.spec.RtpReceiveEndpoint;
import org.brewstream.barista.spec.RtpSendEndpoint;
import org.brewstream.barista.spec.SourceId;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtCallerEndpoint;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.press.net.PressTransport;
import org.brewstream.roast.socket.SrtTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One brew as it runs.
 *
 * <p><b>Two kinds of thread.</b> The data path runs on the brew's event loop:
 * every source delivers there, and the active source's chunks are handed to
 * every output without a lock. Changes (start, stop, update, activate) run on
 * the engine's management thread, because opening and closing sockets waits,
 * and waiting on the loop a socket uses would block it. The two meet through
 * volatile, immutable snapshots: the array of outputs and the active source. A
 * change builds the new legs, then publishes them; the data path picks them up
 * with its next chunk.
 */
final class RunningBrew implements Brew, BrewContext {

    private static final Logger LOG = Logger.getLogger(RunningBrew.class.getName());
    private static final long TICK_MILLIS = 250;
    private static final long RATE_WINDOW_NANOS = TimeUnit.SECONDS.toNanos(1);

    interface Events {
        void brewStateChanged(BrewId brew, BrewState from, BrewState to);

        void endpointStateChanged(BrewId brew, String endpointId, EndpointState from, EndpointState to);

        void sourceActivated(BrewId brew, SourceId source);
    }

    private final Engine engine;
    private final Events events;
    private final EventLoop brewLoop;
    private volatile BrewSpec spec;
    private volatile BrewState state = BrewState.STOPPED;
    private volatile String error;

    // Changed on the management thread; read by status() from anywhere.
    private final Map<SourceId, SourceLeg> sources = new ConcurrentHashMap<>();
    private final Map<OutputId, OutputLeg> outputs = new ConcurrentHashMap<>();

    private volatile SourceLeg active;
    private volatile SourceLeg[] sourceSnapshot = new SourceLeg[0];
    private volatile OutputLeg[] outputSnapshot = new OutputLeg[0];
    private volatile ScheduledFuture<?> tick;
    private volatile long startedNanos;

    // Owned by the brew loop.
    private long windowBytes;
    private long windowStartNanos;
    private volatile long inputBitsPerSecond;

    /** What a brew needs from the engine that runs it. */
    interface Engine {
        BaristaSettings settings();

        String publishedHost();

        ByteBufAllocator allocator();

        EventLoop nextLoop();

        SrtTransport srtTransport(EventLoop loop);

        PressTransport pressTransport(EventLoop loop);

        ScheduledExecutorService dialer();
    }

    RunningBrew(BrewSpec spec, Engine engine, Events events) {
        this.spec = spec;
        this.engine = engine;
        this.events = events;
        this.brewLoop = engine.nextLoop();
    }


    @Override
    public BrewId id() {
        return spec.id();
    }

    @Override
    public BrewSpec spec() {
        return spec;
    }

    @Override
    public BrewState state() {
        return state;
    }

    @Override
    public SourceId activeSource() {
        SourceLeg current = active;
        return current == null ? spec.preferredSource().id() : current.spec.id();
    }

    @Override
    public BrewStatus status() {
        List<EndpointStatus> sourceStatus = new ArrayList<>();
        for (SourceSpec source : spec.sources()) {
            SourceLeg leg = sources.get(source.id());
            sourceStatus.add(leg != null ? leg.status() : idle(source.id().value()));
        }
        List<EndpointStatus> outputStatus = new ArrayList<>();
        for (OutputSpec output : spec.outputs()) {
            OutputLeg leg = outputs.get(output.id());
            outputStatus.add(leg != null ? leg.status() : idle(output.id().value()));
        }
        return new BrewStatus(spec.id(), spec.name(), state, activeSource(), inputBitsPerSecond, sourceStatus,
                outputStatus, error);
    }

    private static EndpointStatus idle(String id) {
        return new EndpointStatus(id, null, null, EndpointState.STOPPED, List.of(), 0, 0, 0, 0, null, null);
    }


    /** Opens every leg. On any failure, closes what opened and becomes FAILED. */
    void start() {
        if (state != BrewState.STOPPED && state != BrewState.FAILED) {
            return;
        }
        error = null;
        state(BrewState.STARTING);
        try {
            for (SourceSpec source : spec.sources()) {
                sources.put(source.id(), openSource(source));
            }
            for (OutputSpec output : spec.outputs()) {
                outputs.put(output.id(), openOutput(output));
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "brew " + spec.id() + " failed to start", e);
            closeLegs();
            error = describe(e);
            state(BrewState.FAILED);
            return;
        }
        active = sources.get(spec.preferredSource().id());
        publishOutputs();
        startedNanos = System.nanoTime();
        tick = brewLoop.scheduleAtFixedRate(this::onTick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Marks a brew that cannot even be started, such as one whose stored ports are taken. */
    void fail(String reason) {
        error = reason;
        state(BrewState.FAILED);
    }

    void stop() {
        if (state == BrewState.STOPPED) {
            return;
        }
        closeLegs();
        error = null;
        state(BrewState.STOPPED);
    }

    /**
     * Applies a new spec in place: legs added are opened, legs removed are
     * closed, legs whose endpoint changed are reopened, and the rest are left
     * running untouched. If the brew is not running, only the spec changes.
     */
    void update(BrewSpec next) {
        BrewSpec previous = spec;
        spec = next;
        if (state == BrewState.STOPPED || state == BrewState.FAILED) {
            return;
        }
        try {
            for (SourceSpec old : previous.sources()) {
                SourceSpec now = find(next.sources(), old.id());
                if (now == null || !now.endpoint().equals(old.endpoint())) {
                    SourceLeg leg = sources.remove(old.id());
                    if (leg != null) {
                        leg.close();
                    }
                }
            }
            for (SourceSpec source : next.sources()) {
                if (!sources.containsKey(source.id())) {
                    sources.put(source.id(), openSource(source));
                }
            }
            for (OutputSpec old : previous.outputs()) {
                OutputSpec now = findOutput(next.outputs(), old.id());
                if (now == null || !now.endpoint().equals(old.endpoint())) {
                    OutputLeg leg = outputs.remove(old.id());
                    if (leg != null) {
                        removeFromSnapshot(leg);
                        leg.close();
                    }
                }
            }
            for (OutputSpec output : next.outputs()) {
                if (!outputs.containsKey(output.id())) {
                    outputs.put(output.id(), openOutput(output));
                }
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "brew " + next.id() + " failed to apply an update", e);
            closeLegs();
            error = describe(e);
            state(BrewState.FAILED);
            return;
        }
        SourceLeg current = active;
        if (current == null || !sources.containsKey(current.spec.id())
                || sources.get(current.spec.id()) != current) {
            SourceLeg preferred = sources.get(next.preferredSource().id());
            active = preferred;
            events.sourceActivated(next.id(), preferred.spec.id());
        }
        publishOutputs();
    }

    void activate(SourceId source) {
        SourceLeg leg = sources.get(source);
        if (leg == null) {
            throw new IllegalArgumentException("brew " + spec.id() + " has no source " + source);
        }
        if (active != leg) {
            active = leg;
            events.sourceActivated(spec.id(), source);
        }
    }

    private void closeLegs() {
        ScheduledFuture<?> running = tick;
        if (running != null) {
            running.cancel(false);
            tick = null;
        }
        outputSnapshot = new OutputLeg[0];
        sourceSnapshot = new SourceLeg[0];
        active = null;
        outputs.values().forEach(OutputLeg::close);
        outputs.clear();
        sources.values().forEach(SourceLeg::close);
        sources.clear();
    }

    private SourceLeg openSource(SourceSpec source) throws Exception {
        SourceLeg leg = switch (source.endpoint()) {
            case SrtListenerEndpoint srt -> new SrtListenerSource(source, srt, this);
            case SrtCallerEndpoint srt -> new SrtCallerSource(source, srt, this);
            case RtpReceiveEndpoint rtp -> new RtpReceiveSource(source, rtp, this);
        };
        open(leg);
        return leg;
    }

    private OutputLeg openOutput(OutputSpec output) throws Exception {
        OutputLeg leg = switch (output.endpoint()) {
            case SrtListenerEndpoint srt -> new SrtListenerOutput(output, srt, this);
            case SrtCallerEndpoint srt -> new SrtCallerOutput(output, srt, this);
            case RtpSendEndpoint rtp -> new RtpSendOutput(output, rtp, this);
        };
        open(leg);
        return leg;
    }

    private static void open(Leg leg) throws Exception {
        try {
            leg.open();
        } catch (Exception e) {
            leg.fail(e);
            leg.close();
            throw new IllegalStateException(leg.kind + " " + leg.id + ": " + describe(e), e);
        }
    }

    private void publishOutputs() {
        sourceSnapshot = sources.values().toArray(new SourceLeg[0]);
        long capacity = queueCapacity();
        outputs.values().forEach(leg -> leg.capacity(capacity));
        outputSnapshot = spec.outputs().stream().map(o -> outputs.get(o.id())).filter(Objects::nonNull)
                .toArray(OutputLeg[]::new);
    }

    private void removeFromSnapshot(OutputLeg leg) {
        outputSnapshot = java.util.Arrays.stream(outputSnapshot).filter(o -> o != leg).toArray(OutputLeg[]::new);
    }

    private static SourceSpec find(List<SourceSpec> specs, SourceId id) {
        return specs.stream().filter(s -> s.id().equals(id)).findFirst().orElse(null);
    }

    private static OutputSpec findOutput(List<OutputSpec> specs, OutputId id) {
        return specs.stream().filter(s -> s.id().equals(id)).findFirst().orElse(null);
    }

    private static String describe(Throwable e) {
        if (e.getMessage() != null) {
            return e.getMessage();
        }
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : root.toString();
    }

    private void state(BrewState next) {
        BrewState previous = state;
        if (previous != next) {
            state = next;
            events.brewStateChanged(spec.id(), previous, next);
        }
    }


    @Override
    public void onSourceChunk(SourceLeg source, ByteBuf chunk) {
        try {
            if (source != active) {
                return; // only the active source feeds the outputs
            }
            windowBytes += chunk.readableBytes();
            for (OutputLeg output : outputSnapshot) {
                output.offer(chunk.retainedDuplicate());
            }
        } finally {
            chunk.release();
        }
    }

    private void onTick() {
        try {
            long now = System.nanoTime();
            long loss = engine.settings().sourceLossTimeout().toNanos();
            for (SourceLeg source : sourceSnapshot) {
                source.tick(now, loss);
            }
            SourceLeg current = active;
            if (current != null && (state == BrewState.STARTING || state == BrewState.RUNNING
                    || state == BrewState.SOURCE_LOST)) {
                if (current.fresh(now, loss)) {
                    state(BrewState.RUNNING);
                } else if (current.everDelivered() || now - startedNanos >= loss) {
                    state(BrewState.SOURCE_LOST);
                }
            }
            if (windowStartNanos == 0) {
                windowStartNanos = now;
            } else if (now - windowStartNanos >= RATE_WINDOW_NANOS) {
                inputBitsPerSecond = windowBytes * 8 * 1_000_000_000L / (now - windowStartNanos);
                windowBytes = 0;
                windowStartNanos = now;
                long capacity = queueCapacity();
                for (OutputLeg output : outputSnapshot) {
                    output.capacity(capacity);
                }
            }
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "brew " + spec.id() + " tick failed; continuing", e);
        }
    }

    /** The configured time of input at the measured rate, within the floor and ceiling. */
    private long queueCapacity() {
        BaristaSettings settings = engine.settings();
        long rate = inputBitsPerSecond;
        if (rate == 0) {
            return settings.maxQueueBytes(); // nothing measured yet: be generous
        }
        long bytes = rate / 8 * settings.queueTime().toMillis() / 1000;
        return Math.max(settings.minQueueBytes(), Math.min(settings.maxQueueBytes(), bytes));
    }


    @Override
    public BaristaSettings settings() {
        return engine.settings();
    }

    @Override
    public String publishedHost() {
        return engine.publishedHost();
    }

    @Override
    public ByteBufAllocator allocator() {
        return engine.allocator();
    }

    @Override
    public EventLoop brewLoop() {
        return brewLoop;
    }

    /**
     * A loop other than the brew's own whenever the group has one. An output on the
     * brew loop cannot drain while the source is being handled, so a burst of
     * source data (Roast releases many packets in one pass) would land in its
     * queue all at once instead of flowing through.
     */
    @Override
    public EventLoop nextOutputLoop() {
        EventLoop candidate = engine.nextLoop();
        for (int attempt = 0; attempt < 8 && candidate == brewLoop; attempt++) {
            candidate = engine.nextLoop();
        }
        return candidate;
    }

    @Override
    public SrtTransport srtTransport(EventLoop loop) {
        return engine.srtTransport(loop);
    }

    @Override
    public PressTransport pressTransport(EventLoop loop) {
        return engine.pressTransport(loop);
    }

    @Override
    public ScheduledExecutorService dialer() {
        return engine.dialer();
    }

    @Override
    public void endpointStateChanged(String endpointId, EndpointState from, EndpointState to) {
        events.endpointStateChanged(spec.id(), Objects.requireNonNull(endpointId), from, to);
    }
}