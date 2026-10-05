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
import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;
import org.brewstream.barista.spec.OutputSpec;
import org.brewstream.barista.spec.SlowSubscriberPolicy;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An SRT listener that subscribers pull the stream from. Each subscriber gets
 * its own queue, so one slow subscriber never holds up the others: in all but
 * configuration, a subscriber is an output of its own.
 */
final class SrtListenerOutput extends OutputLeg {

    private record Subscriber(SrtConnection connection, Lane lane, long since, AtomicBoolean disconnected) {
    }

    private final SrtListenerEndpoint endpoint;
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicLong droppedByDeparted = new AtomicLong();
    private volatile long capacity;
    private SrtListener listener;

    SrtListenerOutput(OutputSpec spec, SrtListenerEndpoint endpoint, BrewContext context) {
        super(spec, "srt-listener", context);
        this.endpoint = endpoint;
        this.capacity = context.settings().maxQueueBytes();
    }

    @Override
    void open() throws InterruptedException {
        listener = SrtListener.bind(new InetSocketAddress(endpoint.port()),
                SrtConfig.defaults().withLatency(endpoint.latency()), context.srtTransport(loop));
        listener.setAcceptHandler(request -> {
            AcceptDecision decision = SrtSupport.admit(request, endpoint.streamId(), endpoint.security());
            if (decision instanceof AcceptDecision.Reject rejected) {
                event(EndpointEvent.Kind.REJECTED, Reasons.refused(rejected.reason(),
                        SrtSupport.hostPort(request.peerAddress()), request.streamId()));
            }
            return decision;
        });
        listener.onConnection(connection -> {
            Lane lane = new Lane(loop, new OutputQueue(capacity), null, new Lane.Writer() {
                @Override
                public boolean writable() {
                    return connection.channel().isWritable();
                }

                @Override
                public void write(ByteBuf chunk) {
                    SrtSupport.write(connection, chunk);
                }
            });
            connection.pipeline().addLast(SrtSupport.writability(lane));
            Subscriber subscriber = new Subscriber(connection, lane, System.currentTimeMillis(), new AtomicBoolean());
            String who = SrtSupport.describe(connection);
            lane.queue().onDrop(() -> {
                event(EndpointEvent.Kind.DROPPING, "subscriber " + who + " is slower than the stream: dropping oldest");
                disconnectIfTooFarBehind(subscriber, who);
            });
            subscribers.add(subscriber);
            event(EndpointEvent.Kind.CONNECTED, "subscriber " + who);
            connection.onClose(() -> {
                if (subscribers.remove(subscriber)) {
                    if (!subscriber.disconnected().get()) {
                        event(EndpointEvent.Kind.DISCONNECTED, "subscriber " + who + " went away (cause unknown)");
                    }
                    droppedByDeparted.addAndGet(lane.queue().dropped());
                    lane.queue().clear();
                }
                state(subscribers.isEmpty() ? EndpointState.WAITING : EndpointState.ACTIVE);
            });
            state(EndpointState.ACTIVE);
        });
        event(EndpointEvent.Kind.STARTED, "listening on " + context.publishedHost() + ":" + endpoint.port());
        state(EndpointState.WAITING);
    }

    /** On the brew loop, after a drop: applies the slow-subscriber policy. */
    private void disconnectIfTooFarBehind(Subscriber subscriber, String who) {
        SlowSubscriberPolicy policy = endpoint.slowSubscribers();
        if (policy.action() != SlowSubscriberPolicy.Action.DISCONNECT || subscriber.disconnected().get()) {
            return;
        }
        OutputQueue queue = subscriber.lane().queue();
        String why = tooFarBehind(policy, queue.behindDroppedBytes(), queue.capacity(),
                System.nanoTime() - queue.behindSinceNanos());
        if (why != null && subscriber.disconnected().compareAndSet(false, true)) {
            event(EndpointEvent.Kind.DISCONNECTED, "subscriber " + who + " disconnected: " + why
                    + " (slow-subscriber policy)");
            subscriber.connection().close();
        }
    }

    /** Why a subscriber this far behind is too far behind, or {@code null} if it is not. */
    static String tooFarBehind(SlowSubscriberPolicy policy, long droppedBytes, long capacityBytes, long behindNanos) {
        if (droppedBytes >= policy.maxDroppedCapacities() * capacityBytes) {
            return "dropped " + droppedBytes / 1024 + " KiB without catching up";
        }
        if (behindNanos >= policy.maxBehindTime().toNanos()) {
            return "behind for " + behindNanos / 1_000_000 + " ms";
        }
        return null;
    }

    @Override
    void offer(ByteBuf chunk) {
        try {
            if (subscribers.isEmpty()) {
                return;
            }
            monitor.observe(chunk);
            for (Subscriber subscriber : subscribers) {
                subscriber.lane().offer(chunk.retainedDuplicate());
            }
        } finally {
            chunk.release();
        }
    }

    @Override
    void capacity(long bytes) {
        capacity = bytes;
        subscribers.forEach(subscriber -> subscriber.lane().queue().capacity(bytes));
    }

    @Override
    void close() {
        try {
            if (listener != null) {
                listener.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        subscribers.forEach(subscriber -> subscriber.lane().queue().clear());
        subscribers.clear();
        event(EndpointEvent.Kind.STOPPED, "closed");
        state(EndpointState.STOPPED);
    }

    @Override
    EndpointStatus status() {
        long dropped = droppedByDeparted.get();
        for (Subscriber subscriber : subscribers) {
            dropped += subscriber.lane().queue().dropped();
        }
        return new EndpointStatus(id, kind, context.publishedHost() + ":" + endpoint.port(), state(), health(),
                subscribers.stream().map(s -> Connections.srt(s.connection(), s.since())).toList(),
                monitor.chunks(), monitor.bytes(), dropped, 0, monitor.tsStats(), false, List.of(), null, history.snapshot(), error);
    }
}