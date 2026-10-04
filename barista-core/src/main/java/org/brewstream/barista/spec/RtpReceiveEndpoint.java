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

package org.brewstream.barista.spec;

import java.time.Duration;
import java.util.Objects;

/**
 * RTP-carried MPEG-TS arriving at Barista: unicast on a local base port, or
 * multicast on a group. Uses the base port P and, as Press does, P+1 for RTCP and
 * P+2 and P+4 for FEC, so a whole block is allocated.
 *
 * @param port             local base port, or 0 to allocate a block
 * @param multicastGroup   the group to join, or {@code null} for unicast
 * @param sourceFilter     source-specific multicast sender, or {@code null}
 * @param networkInterface interface name to join on, or {@code null} for the default
 * @param fec              receive SMPTE 2022-1 FEC on P+2 and P+4
 * @param latency          how long a gap is waited on before giving it up
 */
public record RtpReceiveEndpoint(int port, String multicastGroup, String sourceFilter, String networkInterface,
        boolean fec, Duration latency) implements SourceEndpoint {

    public RtpReceiveEndpoint {
        Ports.check(port);
        Objects.requireNonNull(latency, "latency");
        if (sourceFilter != null && multicastGroup == null) {
            throw new IllegalArgumentException("a source filter only applies to multicast");
        }
    }

    /** Unicast on an allocated block, no FEC, 120 ms latency. */
    public static RtpReceiveEndpoint unicast() {
        return new RtpReceiveEndpoint(0, null, null, null, false, Duration.ofMillis(120));
    }

    public RtpReceiveEndpoint withPort(int port) {
        return new RtpReceiveEndpoint(port, multicastGroup, sourceFilter, networkInterface, fec, latency);
    }

    public RtpReceiveEndpoint withFec(boolean fec, Duration latency) {
        return new RtpReceiveEndpoint(port, multicastGroup, sourceFilter, networkInterface, fec, latency);
    }

    @Override
    public boolean listens() {
        return true;
    }
}