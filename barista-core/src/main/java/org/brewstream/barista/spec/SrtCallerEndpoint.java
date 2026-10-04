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
 * SRT, with Barista dialling out. As a source it pulls from a remote listener; as
 * an output it pushes to one. Reconnects with backoff when the connection drops
 * or cannot be made.
 *
 * @param streamId sent in the handshake, or {@code null}
 * @param security encryption, or {@code null} for none
 */
public record SrtCallerEndpoint(String host, int port, String streamId, SrtSecurity security, Duration latency)
        implements SourceEndpoint, OutputEndpoint {

    public SrtCallerEndpoint {
        Objects.requireNonNull(host, "host");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("remote port must be 1-65535, not " + port);
        }
        Objects.requireNonNull(latency, "latency");
    }

    public static SrtCallerEndpoint to(String host, int port) {
        return new SrtCallerEndpoint(host, port, null, null, Duration.ofMillis(120));
    }

    @Override
    public boolean listens() {
        return false;
    }
}