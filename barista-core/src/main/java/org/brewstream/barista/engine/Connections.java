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
import org.brewstream.barista.RtpReceiveStats;
import org.brewstream.barista.RtpSendStats;
import org.brewstream.barista.SrtStats;
import org.brewstream.press.net.ReceiverStats;
import org.brewstream.press.net.SenderStats;
import org.brewstream.roast.socket.ConnectionStats;
import org.brewstream.roast.socket.SrtConnection;

import java.net.InetSocketAddress;

/** Translates transport statistics into Barista's own vocabulary. The only place that knows both. */
final class Connections {

    private Connections() {
    }

    static ConnectionView srt(SrtConnection connection, long connectedSinceMillis) {
        return new ConnectionView(Integer.toHexString(connection.metadata().socketId().value()),
                connection.metadata().streamId(), hostPort(connection.metadata().peerAddress()),
                connectedSinceMillis, srt(connection.stats()));
    }

    static SrtStats srt(ConnectionStats s) {
        return new SrtStats(s.rttMicros(), s.rttVarMicros(), s.packetsSent(), s.packetsReceived(), s.bytesSent(),
                s.bytesReceived(), s.packetsLost(), s.packetsRetransmitted(), s.packetsRecovered(), s.packetsDropped(),
                s.receiveRateBytesPerSecond(), s.estimatedSentBytesPerSecond(), s.sendLossRatePercent(),
                s.sendBufferedPackets(), s.receiveBufferedPackets());
    }

    static ConnectionView rtpReceive(ReceiverStats s, long connectedSinceMillis) {
        return new ConnectionView(Long.toHexString(s.ssrc()), null, hostPort(s.source()), connectedSinceMillis,
                new RtpReceiveStats(s.packetsReceived(), s.bytesDelivered(), s.networkLost(), s.packetsLost(),
                        s.packetsRecovered(), s.packetsRecoveredLate(), s.packetsDuplicate(), s.packetsLate(),
                        s.jitterMicros(), s.fecColumns(), s.fecRows()));
    }

    static ConnectionView rtpSend(SenderStats s, String destination, long connectedSinceMillis) {
        return new ConnectionView(Long.toHexString(s.ssrc()), null, destination, connectedSinceMillis,
                new RtpSendStats(s.packetsSent(), s.bytesSent(), s.fecPacketsSent(), s.receiverReports(),
                        s.fractionLost(), s.cumulativeLost(), s.jitterMicros(), s.rttMicros()));
    }

    private static String hostPort(InetSocketAddress address) {
        return address == null ? null : address.getAddress().getHostAddress() + ":" + address.getPort();
    }
}