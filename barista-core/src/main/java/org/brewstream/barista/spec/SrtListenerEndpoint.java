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
 * SRT, with the far side dialling in. As a source, an encoder publishes to it,
 * one at a time. As an output, any number of subscribers connect and each gets
 * the stream.
 *
 * @param port     local port, or 0 to allocate one
 * @param streamId the stream ID callers must ask for, or {@code null} to accept any
 * @param security encryption, or {@code null} for none
 * @param latency  SRT receive latency
 */
public record SrtListenerEndpoint(int port, String streamId, SrtSecurity security, Duration latency)
        implements SourceEndpoint, OutputEndpoint {

    public SrtListenerEndpoint {
        Ports.check(port);
        Objects.requireNonNull(latency, "latency");
    }

    /** Any stream ID, no encryption, 120 ms latency, on an allocated port. */
    public static SrtListenerEndpoint any() {
        return new SrtListenerEndpoint(0, null, null, Duration.ofMillis(120));
    }

    public SrtListenerEndpoint withPort(int port) {
        return new SrtListenerEndpoint(port, streamId, security, latency);
    }

    @Override
    public boolean listens() {
        return true;
    }
}