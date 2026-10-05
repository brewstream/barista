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

import org.brewstream.barista.ConnectionView;
import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtCallerEndpoint;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;

import java.net.InetSocketAddress;
import java.util.List;

/** Pulls from a remote SRT listener, redialling with backoff when it cannot connect or is dropped. */
final class SrtCallerSource extends SourceLeg {

    private final SrtCallerEndpoint endpoint;
    private Redialer redialer;
    private volatile long connectedSince;
    private volatile int failedDials;

    SrtCallerSource(SourceSpec spec, SrtCallerEndpoint endpoint, BrewContext context) {
        super(spec, "srt-caller", context);
        this.endpoint = endpoint;
    }

    @Override
    void open() {
        redialer = new Redialer(context,
                () -> SrtCaller.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), endpoint.streamId(),
                        SrtSupport.passphrase(endpoint.security()), SrtSupport.keyLength(endpoint.security()),
                        SrtConfig.defaults().withLatency(endpoint.latency()),
                        context.srtTransport(context.brewLoop())),
                connection -> {
                    connectedSince = System.currentTimeMillis();
                    failedDials = 0;
                    connection.onData(this::receive);
                    event(EndpointEvent.Kind.CONNECTED, "connected to " + address());
                    state(EndpointState.ACTIVE);
                },
                () -> {
                    event(EndpointEvent.Kind.DISCONNECTED, "connection to " + address() + " ended (cause unknown)");
                    state(EndpointState.RECONNECTING);
                },
                failure -> {
                    failedDials = failedDials + 1; // dials are sequential: one writer at a time
                    event(EndpointEvent.Kind.FAILED, Reasons.dialFailed(failure, address()));
                });
        event(EndpointEvent.Kind.STARTED, "dialling " + address());
        state(EndpointState.CONNECTING);
        redialer.start();
    }

    @Override
    boolean connected() {
        return redialer != null && redialer.connection() != null;
    }

    @Override
    int failedDials() {
        return failedDials;
    }

    @Override
    String address() {
        return endpoint.host() + ":" + endpoint.port();
    }

    @Override
    List<ConnectionView> connections() {
        SrtConnection current = redialer == null ? null : redialer.connection();
        return current == null ? List.of() : List.of(Connections.srt(current, connectedSince));
    }

    @Override
    void close() {
        if (redialer != null) {
            redialer.close();
        }
        releaseAligner();
        event(EndpointEvent.Kind.STOPPED, "closed");
        state(EndpointState.STOPPED);
    }
}