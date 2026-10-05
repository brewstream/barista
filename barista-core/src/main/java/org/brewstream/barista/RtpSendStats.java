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
 * An RTP stream being sent, with what its receiver last reported over RTCP.
 *
 * @param packetsSent            media packets sent
 * @param bytesSent              payload bytes sent
 * @param fecPacketsSent         FEC packets sent
 * @param receiverReports        RTCP receiver reports received
 * @param receiverFractionLost   loss over the receiver's last report interval, 0.0 to 1.0
 * @param receiverCumulativeLost the receiver's cumulative loss
 * @param receiverJitterMicros   the receiver's interarrival jitter
 * @param rttMicros              round-trip time from receiver reports, or -1 until one echoes
 *                               this sender's report
 */
public record RtpSendStats(
        long packetsSent,
        long bytesSent,
        long fecPacketsSent,
        long receiverReports,
        double receiverFractionLost,
        long receiverCumulativeLost,
        long receiverJitterMicros,
        long rttMicros) implements TransportStats {
}