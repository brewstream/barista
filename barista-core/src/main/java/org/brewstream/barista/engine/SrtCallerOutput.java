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
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;
import org.brewstream.barista.spec.OutputSpec;
import org.brewstream.barista.spec.SrtCallerEndpoint;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;

import java.net.InetSocketAddress;

/**
 * Pushes to a remote SRT listener, redialling with backoff. While disconnected
 * its queue fills and drops oldest, and on reconnect what is left is discarded,
 * so a reconnected peer gets the live stream, not a backlog.
 */
final class SrtCallerOutput extends OutputLeg {

    private final SrtCallerEndpoint endpoint;
    private final Lane lane;
    private Redialer redialer;

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
                    lane.queue().clear();
                    connection.pipeline().addLast(SrtSupport.writability(lane));
                    state(EndpointState.ACTIVE);
                    lane.schedule();
                },
                () -> state(EndpointState.RECONNECTING));
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
        state(EndpointState.STOPPED);
    }

    @Override
    EndpointStatus status() {
        SrtConnection current = redialer == null ? null : redialer.connection();
        return new EndpointStatus(id, kind, endpoint.host() + ":" + endpoint.port(), state(), current == null ? 0 : 1,
                monitor.chunks(), monitor.bytes(), lane.queue().dropped(), 0, monitor.health(),
                current == null ? null : current.stats(), error);
    }
}