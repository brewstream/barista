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

import org.brewstream.barista.EndpointState;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtCallerEndpoint;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;

import java.net.InetSocketAddress;

/** Pulls from a remote SRT listener, redialling with backoff when it cannot connect or is dropped. */
final class SrtCallerSource extends SourceLeg {

    private final SrtCallerEndpoint endpoint;
    private Redialer redialer;

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
                    connection.onData(this::receive);
                    state(EndpointState.ACTIVE);
                },
                () -> state(EndpointState.RECONNECTING));
        state(EndpointState.CONNECTING);
        redialer.start();
    }

    @Override
    boolean connected() {
        return redialer != null && redialer.connection() != null;
    }

    @Override
    String address() {
        return endpoint.host() + ":" + endpoint.port();
    }

    @Override
    int connections() {
        return connected() ? 1 : 0;
    }

    @Override
    Object transportStats() {
        SrtConnection current = redialer == null ? null : redialer.connection();
        return current == null ? null : current.stats();
    }

    @Override
    void close() {
        if (redialer != null) {
            redialer.close();
        }
        releaseAligner();
        state(EndpointState.STOPPED);
    }
}