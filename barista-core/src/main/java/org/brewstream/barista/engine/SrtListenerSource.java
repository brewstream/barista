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
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * An SRT listener an encoder publishes to. One publisher at a time: a second
 * caller is refused with CONFLICT while the first is connected, so two encoders
 * can never interleave into one stream.
 */
final class SrtListenerSource extends SourceLeg {

    private final SrtListenerEndpoint endpoint;
    private SrtListener listener;
    private volatile SrtConnection publisher;
    private volatile long publisherSince;

    SrtListenerSource(SourceSpec spec, SrtListenerEndpoint endpoint, BrewContext context) {
        super(spec, "srt-listener", context);
        this.endpoint = endpoint;
    }

    @Override
    void open() throws InterruptedException {
        listener = SrtListener.bind(new InetSocketAddress(endpoint.port()),
                SrtConfig.defaults().withLatency(endpoint.latency()), context.srtTransport(context.brewLoop()));
        listener.setAcceptHandler(request -> publisher != null
                ? AcceptDecision.reject(RejectionReason.CONFLICT)
                : SrtSupport.admit(request, endpoint.streamId(), endpoint.security()));
        // On the brew loop, before the caller's first packet.
        listener.onConnection(connection -> {
            publisherSince = System.currentTimeMillis();
            publisher = connection;
            connection.onData(this::receive);
            connection.onClose(() -> {
                if (publisher == connection) {
                    publisher = null;
                    state(EndpointState.WAITING);
                }
            });
            state(EndpointState.ACTIVE);
        });
        state(EndpointState.WAITING);
    }

    @Override
    boolean connected() {
        return publisher != null;
    }

    @Override
    String address() {
        return context.publishedHost() + ":" + endpoint.port();
    }

    @Override
    List<ConnectionView> connections() {
        SrtConnection current = publisher;
        return current == null ? List.of() : List.of(Connections.srt(current, publisherSince));
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
        releaseAligner();
        state(EndpointState.STOPPED);
    }
}