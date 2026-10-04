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

import java.util.Objects;

/**
 * RTP-carried MPEG-TS sent from Barista to a destination, unicast or multicast,
 * optionally with SMPTE 2022-1 FEC.
 *
 * @param fecColumns       L, or 0 for no FEC
 * @param fecRows          D, or 0 for no FEC
 * @param ttl              multicast time-to-live, or -1 for the default
 * @param networkInterface interface to send multicast from, or {@code null}
 */
public record RtpSendEndpoint(String host, int port, int fecColumns, int fecRows, int ttl, String networkInterface)
        implements OutputEndpoint {

    public RtpSendEndpoint {
        Objects.requireNonNull(host, "host");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("destination port must be 1-65535, not " + port);
        }
        if ((fecColumns == 0) != (fecRows == 0)) {
            throw new IllegalArgumentException("FEC needs both columns and rows, or neither");
        }
    }

    public static RtpSendEndpoint to(String host, int port) {
        return new RtpSendEndpoint(host, port, 0, 0, -1, null);
    }

    public RtpSendEndpoint withFec(int columns, int rows) {
        return new RtpSendEndpoint(host, port, columns, rows, ttl, networkInterface);
    }

    @Override
    public boolean listens() {
        return false;
    }
}