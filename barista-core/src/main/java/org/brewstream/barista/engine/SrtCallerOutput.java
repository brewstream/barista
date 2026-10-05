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
import org.brewstream.barista.spec.SrtCallerEndpoint;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Pushes to a remote SRT listener, redialling with backoff. While disconnected
 * its queue fills and drops oldest, and on reconnect what is left is discarded,
 * so a reconnected peer gets the live stream, not a backlog.
 */
final class SrtCallerOutput extends OutputLeg {

    private final SrtCallerEndpoint endpoint;
    private final Lane lane;
    private Redialer redialer;
    private volatile long connectedSince;

    SrtCallerOutput(OutputSpec spec, SrtCallerEndpoint endpoint, BrewContext context) {
        super(spec, "srt-caller", context);
        this.endpoint = endpoint;
        this.lane = new Lane(loop, new OutputQueue(context.settings().maxQueueBytes()), monitor, new Lane.Writer() {
            @Override
            public boolean writable() {
                SrtConnection current = redialer == null ? null : redialer.connection();
                return current != null && current.channel().isWritable();
            }

            @Override
            public void write(ByteBuf chunk) {
                SrtConnection current = redialer.connection();
                if (current == null) {
                    chunk.release();
                } else {
                    SrtSupport.write(current, chunk);
                }
            }
        });
    }

    @Override
    void open() {
        redialer = new Redialer(context,
                () -> SrtCaller.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), endpoint.streamId(),
                        SrtSupport.passphrase(endpoint.security()), SrtSupport.keyLength(endpoint.security()),
                        SrtConfig.defaults().withLatency(endpoint.latency()), context.srtTransport(loop)),
                connection -> {
                    connectedSince = System.currentTimeMillis();
                    lane.queue().clear();
                    connection.pipeline().addLast(SrtSupport.writability(lane));
                    event(EndpointEvent.Kind.CONNECTED, "connected to " + target());
                    state(EndpointState.ACTIVE);
                    lane.schedule();
                },
                () -> {
                    event(EndpointEvent.Kind.DISCONNECTED, "connection to " + target() + " ended (cause unknown)");
                    state(EndpointState.RECONNECTING);
                },
                failure -> event(EndpointEvent.Kind.FAILED, Reasons.dialFailed(failure, target())));
        // While disconnected the queue drops by design; only a connected peer too slow is news.
        lane.queue().onDrop(() -> {
            if (redialer.connection() != null) {
                event(EndpointEvent.Kind.DROPPING, target() + " is slower than the stream: dropping oldest");
            }
        });
        event(EndpointEvent.Kind.STARTED, "dialling " + target());
        state(EndpointState.CONNECTING);
        redialer.start();
    }

    @Override
    void offer(ByteBuf chunk) {
        lane.offer(chunk);
    }

    @Override
    void capacity(long bytes) {
        lane.queue().capacity(bytes);
    }

    @Override
    void close() {
        if (redialer != null) {
            redialer.close();
        }
        lane.queue().clear();
        event(EndpointEvent.Kind.STOPPED, "closed");
        state(EndpointState.STOPPED);
    }

    private String target() {
        return endpoint.host() + ":" + endpoint.port();
    }

    @Override
    EndpointStatus status() {
        SrtConnection current = redialer == null ? null : redialer.connection();
        return new EndpointStatus(id, kind, endpoint.host() + ":" + endpoint.port(), state(), health(),
                current == null ? List.of() : List.of(Connections.srt(current, connectedSince)),
                monitor.chunks(), monitor.bytes(), lane.queue().dropped(), 0, monitor.tsStats(), false, List.of(), history.snapshot(),
                error);
    }
}