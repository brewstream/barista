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
import org.brewstream.barista.spec.RtpSendEndpoint;
import org.brewstream.press.net.RtpSender;
import org.brewstream.press.net.RtpSenderConfig;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;

/**
 * Sends RTP through a Press sender, with FEC if configured. Chunks are whole TS
 * packets, at most seven, which is exactly what the sender packs per RTP packet.
 */
final class RtpSendOutput extends OutputLeg {

    private final RtpSendEndpoint endpoint;
    private final Lane lane;
    private volatile RtpSender sender;
    private volatile long openedSince;

    RtpSendOutput(OutputSpec spec, RtpSendEndpoint endpoint, BrewContext context) {
        super(spec, "rtp-send", context);
        this.endpoint = endpoint;
        this.lane = new Lane(loop, new OutputQueue(context.settings().maxQueueBytes()), monitor, new Lane.Writer() {
            @Override
            public boolean writable() {
                return sender != null; // UDP: the socket never pushes back
            }

            @Override
            public void write(ByteBuf chunk) {
                sender.write(chunk);
            }

            /** Press packs seven TS packets per RTP packet; a shorter run must not wait for more. */
            @Override
            public void flush() {
                sender.flush();
            }
        });
    }

    @Override
    void open() throws InterruptedException, SocketException {
        RtpSenderConfig config = RtpSenderConfig.to(new InetSocketAddress(endpoint.host(), endpoint.port()));
        if (endpoint.fecColumns() > 0) {
            config = config.withFec(endpoint.fecColumns(), endpoint.fecRows());
        }
        if (endpoint.ttl() >= 0) {
            config = config.withTtl(endpoint.ttl());
        }
        if (endpoint.networkInterface() != null) {
            config = config.withInterface(NetworkInterface.getByName(endpoint.networkInterface()));
        }
        sender = RtpSender.connect(config, context.pressTransport(loop));
        openedSince = System.currentTimeMillis();
        String destination = endpoint.host() + ":" + endpoint.port();
        lane.queue().onDrop(() -> event(EndpointEvent.Kind.DROPPING,
                "sending to " + destination + " cannot keep up: dropping oldest"));
        event(EndpointEvent.Kind.STARTED, "sending to " + destination
                + (endpoint.fecColumns() > 0 ? " with FEC " + endpoint.fecColumns() + "x" + endpoint.fecRows() : ""));
        state(EndpointState.ACTIVE);
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
        RtpSender current = sender;
        if (current != null) {
            try {
                current.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        lane.queue().clear();
        event(EndpointEvent.Kind.STOPPED, "closed");
        state(EndpointState.STOPPED);
    }

    @Override
    EndpointStatus status() {
        RtpSender current = sender;
        String destination = endpoint.host() + ":" + endpoint.port();
        return new EndpointStatus(id, kind, destination, state(),
                current == null ? List.of() : List.of(Connections.rtpSend(current.stats(), destination, openedSince)),
                monitor.chunks(), monitor.bytes(), lane.queue().dropped(), 0, monitor.health(), history.snapshot(),
                error);
    }
}