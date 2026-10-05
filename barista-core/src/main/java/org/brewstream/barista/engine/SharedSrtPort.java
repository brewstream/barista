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

import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.AcceptHandler;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;
import org.brewstream.roast.socket.SrtTransport;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * One SRT listener shared by several endpoints of a brew. Each connection goes to
 * the endpoint whose stream ID it asks for, matched exactly; there is no naming
 * convention. A stream ID no endpoint has is refused, and every endpoint on the
 * port records the refusal.
 *
 * <p>Bound on the brew loop, so every connection on the port runs there. The
 * listener closes when its last endpoint leaves. Registration runs on the
 * management thread; routing on the brew loop.
 */
final class SharedSrtPort {

    /**
     * One endpoint's part of the port.
     *
     * @param streamId     the stream ID that reaches this endpoint
     * @param admit        the endpoint's own admission: encryption, a publisher already present
     * @param onConnection an accepted connection for this endpoint
     * @param onRefused    a refusal of a stream ID nobody on the port has, in plain words
     */
    record Route(String streamId, AcceptHandler admit, Consumer<SrtConnection> onConnection,
            Consumer<String> onRefused) {
    }

    private final int port;
    private final SrtListener listener;
    private final Runnable onEmpty;
    private final Map<String, Route> routes = new ConcurrentHashMap<>();

    private SharedSrtPort(int port, SrtListener listener, Runnable onEmpty) {
        this.port = port;
        this.listener = listener;
        this.onEmpty = onEmpty;
        listener.setAcceptHandler(request -> {
            String streamId = request.streamId() == null ? "" : request.streamId();
            Route route = routes.get(streamId);
            if (route == null) {
                String reason = Reasons.refused(RejectionReason.NOTFOUND, SrtSupport.hostPort(request.peerAddress()),
                        request.streamId());
                routes.values().forEach(each -> each.onRefused().accept(reason));
                return AcceptDecision.reject(RejectionReason.NOTFOUND);
            }
            return route.admit().handle(request);
        });
        listener.onConnection(connection -> {
            Route route = routes.get(connection.metadata().streamId());
            if (route == null) {
                connection.close(); // its endpoint left between the handshake and now
            } else {
                route.onConnection().accept(connection);
            }
        });
    }

    /** Binds the port. {@code onEmpty} runs once the last endpoint has left and the port is closed. */
    static SharedSrtPort bind(int port, Duration latency, SrtTransport transport, Runnable onEmpty)
            throws InterruptedException {
        SrtListener listener = SrtListener.bind(new InetSocketAddress(port),
                SrtConfig.defaults().withLatency(latency), transport);
        return new SharedSrtPort(port, listener, onEmpty);
    }

    int port() {
        return port;
    }

    void register(Route route) {
        if (routes.putIfAbsent(route.streamId(), route) != null) {
            throw new IllegalStateException("stream ID '" + route.streamId() + "' is already on SRT port " + port);
        }
    }

    /** Removes an endpoint; the last one out closes the port. Its own connections are the caller's to close. */
    void unregister(Route route) {
        routes.remove(route.streamId(), route);
        if (routes.isEmpty()) {
            try {
                listener.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            onEmpty.run();
        }
    }
}
