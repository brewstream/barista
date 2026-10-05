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
 * An RTP stream being received.
 *
 * @param packetsReceived      packets received, duplicates and late ones included
 * @param bytesReceived        payload bytes delivered in order
 * @param networkLost          what the network lost (RFC 3550 cumulative loss)
 * @param packetsLost          what was never delivered, after reordering and FEC had their chance
 * @param packetsRecovered     packets rebuilt from FEC
 * @param packetsRecoveredLate packets rebuilt after delivery had given up on them
 * @param packetsDuplicate     packets received twice
 * @param packetsLate          packets that arrived after they were given up on
 * @param jitterMicros         interarrival jitter (RFC 3550)
 * @param fecColumns           FEC matrix columns (L), 0 without FEC
 * @param fecRows              FEC matrix rows (D), 0 without column FEC
 */
public record RtpReceiveStats(
        long packetsReceived,
        long bytesReceived,
        long networkLost,
        long packetsLost,
        long packetsRecovered,
        long packetsRecoveredLate,
        long packetsDuplicate,
        long packetsLate,
        long jitterMicros,
        int fecColumns,
        int fecRows) implements TransportStats {
}