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

package org.brewstream.barista;

/**
 * An SRT connection, in either direction.
 *
 * @param rttMicros                 smoothed round-trip time
 * @param rttVarMicros              round-trip time variation
 * @param packetsSent               data packets sent, retransmissions excluded
 * @param packetsReceived           data packets received
 * @param bytesSent                 payload bytes sent
 * @param bytesReceived             payload bytes received
 * @param packetsLost               packets the receiving side detected as missing
 * @param packetsRetransmitted      packets this side sent again
 * @param packetsRecovered          packets this side got back through retransmission
 * @param packetsDropped            packets given up as too late to deliver
 * @param receiveRateBytesPerSecond current receive rate
 * @param sendRateBytesPerSecond    current send rate
 * @param sendLossRatePercent       share of sent packets reported lost
 * @param sendBufferedPackets       packets waiting to be sent or acknowledged: the backlog
 * @param receiveBufferedPackets    packets held for in-order, on-time delivery
 */
public record SrtStats(
        long rttMicros,
        long rttVarMicros,
        long packetsSent,
        long packetsReceived,
        long bytesSent,
        long bytesReceived,
        long packetsLost,
        long packetsRetransmitted,
        long packetsRecovered,
        long packetsDropped,
        long receiveRateBytesPerSecond,
        long sendRateBytesPerSecond,
        double sendLossRatePercent,
        long sendBufferedPackets,
        long receiveBufferedPackets) implements TransportStats {
}