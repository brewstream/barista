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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.brewstream.barista.spec.SrtSecurity;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.ConnectionRequest;
import org.brewstream.roast.socket.SrtConnection;

/** Roast plumbing shared by the SRT legs. */
final class SrtSupport {

    private SrtSupport() {
    }

    /**
     * Writes a chunk, split into whole TS packets that fit the connection's
     * payload limit. A chunk is at most 1316 bytes, which fits the usual limit
     * (MTU minus 44, 1456 at 1500); a peer that negotiated a smaller MTU gets
     * more, smaller writes rather than an error. Takes ownership.
     */
    static void write(SrtConnection connection, ByteBuf chunk) {
        int max = connection.metadata().maxPayloadSize() / TsAligner.PACKET * TsAligner.PACKET;
        if (chunk.readableBytes() <= max) {
            connection.write(chunk);
            return;
        }
        try {
            while (chunk.isReadable()) {
                connection.write(chunk.readRetainedSlice(Math.min(max, chunk.readableBytes())));
            }
        } finally {
            chunk.release();
        }
    }

    /** Wakes a lane when its connection can take more again. */
    static ChannelHandler writability(Lane lane) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelWritabilityChanged(ChannelHandlerContext ctx) {
                if (ctx.channel().isWritable()) {
                    lane.schedule();
                }
                ctx.fireChannelWritabilityChanged();
            }
        };
    }

    /** Admits a caller asking for the right stream ID, with the endpoint's encryption. */
    static AcceptDecision admit(ConnectionRequest request, String streamId, SrtSecurity security) {
        if (streamId != null && !streamId.equals(request.streamId())) {
            return AcceptDecision.reject(RejectionReason.NOTFOUND);
        }
        return security == null
                ? AcceptDecision.accept()
                : AcceptDecision.accept(security.passphrase().toCharArray(), security.keyLength());
    }

    static String hostPort(java.net.InetSocketAddress address) {
        return address == null ? "?" : address.getAddress().getHostAddress() + ":" + address.getPort();
    }

    /** A connection as an operator would name it: its peer and the stream ID it asked for. */
    static String describe(SrtConnection connection) {
        String streamId = connection.metadata().streamId();
        return hostPort(connection.metadata().peerAddress())
                + (streamId == null || streamId.isEmpty() ? "" : " (stream '" + streamId + "')");
    }

    static char[] passphrase(SrtSecurity security) {
        return security == null ? null : security.passphrase().toCharArray();
    }

    static int keyLength(SrtSecurity security) {
        return security == null ? 0 : security.keyLength();
    }
}