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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.brewstream.barista.ConnectionView;
import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.spec.RtpReceiveEndpoint;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.press.net.RtpReceiver;
import org.brewstream.press.net.RtpReceiverConfig;
import org.brewstream.press.net.RtpReceiverListener;
import org.brewstream.press.net.ReceiverStats;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * RTP arriving on a base port, unicast or multicast, through a Press receiver:
 * reordered, FEC-repaired if configured, and already whole TS packets.
 */
final class RtpReceiveSource extends SourceLeg {

    private final RtpReceiveEndpoint endpoint;
    private RtpReceiver receiver;
    private volatile long heardSinceMillis;
    private volatile long heardSsrc = -1;

    RtpReceiveSource(SourceSpec spec, RtpReceiveEndpoint endpoint, BrewContext context) {
        super(spec, "rtp-receive", context);
        this.endpoint = endpoint;
    }

    @Override
    void open() throws InterruptedException, UnknownHostException, SocketException {
        RtpReceiverConfig config = endpoint.multicastGroup() == null
                ? RtpReceiverConfig.unicast(endpoint.port())
                : RtpReceiverConfig.multicast(InetAddress.getByName(endpoint.multicastGroup()), endpoint.port());
        if (endpoint.networkInterface() != null) {
            config = config.withInterface(NetworkInterface.getByName(endpoint.networkInterface()));
        }
        if (endpoint.sourceFilter() != null) {
            config = config.withSource(InetAddress.getByName(endpoint.sourceFilter()));
        }
        config = config.withFec(endpoint.fec()).withLatency(endpoint.latency());
        receiver = RtpReceiver.bind(config, context.pressTransport(context.brewLoop()),
                pipeline -> pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>(false) {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                        receive(payload);
                    }
                }));
        receiver.addListener(new RtpReceiverListener() {
            @Override
            public void onSourceChanged(RtpReceiver r, long previous, long ssrc, InetSocketAddress source) {
                String sender = (source == null ? "?" : SrtSupport.hostPort(source)) + " (SSRC " + Long.toHexString(ssrc) + ")";
                if (previous == -1) {
                    event(EndpointEvent.Kind.CONNECTED, "sender " + sender + " heard");
                } else if (previous == ssrc) {
                    event(EndpointEvent.Kind.SENDER_CHANGED, "sender " + sender + " restarted its sequence numbers");
                } else {
                    event(EndpointEvent.Kind.SENDER_CHANGED, "new sender " + sender + ", was SSRC " + Long.toHexString(previous));
                }
            }

            @Override
            public void onGoodbye(RtpReceiver r, long ssrc) {
                event(EndpointEvent.Kind.GOODBYE, "sender SSRC " + Long.toHexString(ssrc) + " said it stopped (RTCP BYE)");
            }
        });
        event(EndpointEvent.Kind.STARTED, "receiving on " + address() + (endpoint.fec() ? " with FEC" : ""));
        state(EndpointState.WAITING);
    }

    /** RTP has no connection: a sender counts as connected once it has been heard. */
    @Override
    boolean connected() {
        return everDelivered();
    }

    @Override
    String address() {
        String host = endpoint.multicastGroup() != null ? endpoint.multicastGroup() : context.publishedHost();
        return host + ":" + endpoint.port();
    }

    /** The sender, once heard. A new SSRC (an encoder restart) starts its connected-since afresh. */
    @Override
    List<ConnectionView> connections() {
        RtpReceiver current = receiver;
        if (current == null || !everDelivered()) {
            return List.of();
        }
        ReceiverStats stats = current.stats();
        if (stats == null || stats.ssrc() == -1) {
            return List.of();
        }
        if (stats.ssrc() != heardSsrc) {
            heardSsrc = stats.ssrc();
            heardSinceMillis = System.currentTimeMillis();
        }
        return List.of(Connections.rtpReceive(stats, heardSinceMillis));
    }

    @Override
    void close() {
        try {
            if (receiver != null) {
                receiver.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        releaseAligner();
        event(EndpointEvent.Kind.STOPPED, "closed");
        state(EndpointState.STOPPED);
    }
}