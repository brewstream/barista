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
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.ConnectionRequest;
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
    private SharedSrtPort shared;
    private SharedSrtPort.Route route;
    private volatile SrtConnection publisher;
    private volatile long publisherSince;

    SrtListenerSource(SourceSpec spec, SrtListenerEndpoint endpoint, BrewContext context) {
        super(spec, "srt-listener", context);
        this.endpoint = endpoint;
    }

    @Override
    void open() throws InterruptedException {
        shared = context.sharedSrtPort(endpoint.port());
        if (shared != null) {
            route = new SharedSrtPort.Route(endpoint.streamId(), this::admit, this::accepted,
                    reason -> event(EndpointEvent.Kind.REJECTED, reason));
            shared.register(route);
        } else {
            // On the brew loop, so the publisher's data arrives where the brew runs.
            listener = SrtListener.bind(new InetSocketAddress(endpoint.port()),
                    SrtConfig.defaults().withLatency(endpoint.latency()), context.srtTransport(context.brewLoop()));
            listener.setAcceptHandler(this::admit);
            listener.onConnection(this::accepted);
        }
        event(EndpointEvent.Kind.STARTED, "listening on " + address()
                + (shared != null ? " (shared, stream ID '" + endpoint.streamId() + "')" : ""));
        state(EndpointState.WAITING);
    }

    private AcceptDecision admit(ConnectionRequest request) {
        AcceptDecision decision = publisher != null
                ? AcceptDecision.reject(RejectionReason.CONFLICT)
                : SrtSupport.admit(request, endpoint.streamId(), endpoint.security());
        if (decision instanceof AcceptDecision.Reject rejected) {
            event(EndpointEvent.Kind.REJECTED, Reasons.refused(rejected.reason(),
                    SrtSupport.hostPort(request.peerAddress()), request.streamId()));
        }
        return decision;
    }

    /** On the brew loop, before the caller's first packet. */
    private void accepted(SrtConnection connection) {
        publisherSince = System.currentTimeMillis();
        publisher = connection;
        String who = SrtSupport.describe(connection);
        event(EndpointEvent.Kind.CONNECTED, "publisher " + who);
        connection.onData(this::receive);
        connection.onClose(() -> {
            if (publisher == connection) {
                publisher = null;
                event(EndpointEvent.Kind.DISCONNECTED, "publisher " + who + " went away (cause unknown)");
                state(EndpointState.WAITING);
            }
        });
        state(EndpointState.ACTIVE);
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
            if (shared != null) {
                SrtConnection current = publisher;
                if (current != null) {
                    current.close();
                }
                shared.unregister(route);
            } else if (listener != null) {
                listener.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        releaseAligner();
        event(EndpointEvent.Kind.STOPPED, "closed");
        state(EndpointState.STOPPED);
    }
}