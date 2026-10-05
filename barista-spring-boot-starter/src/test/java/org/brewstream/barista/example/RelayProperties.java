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

package org.brewstream.barista.example;

import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.OutputSpec;
import org.brewstream.barista.spec.RtpSendEndpoint;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * An application's own configuration of the brews it wants, mapped to Barista's
 * specs. The README's example; Barista binds no brews from configuration itself.
 */
@ConfigurationProperties("relay")
public record RelayProperties(List<ConfiguredBrew> brews) {

    public RelayProperties {
        brews = brews == null ? List.of() : List.copyOf(brews);
    }

    /** An encoder publishes over SRT on {@code srtPort}; each output is an RTP destination. */
    public record ConfiguredBrew(String id, String name, int srtPort, List<Destination> outputs) {

        public BrewSpec toSpec() {
            List<OutputSpec> rtp = outputs == null ? List.of() : outputs.stream()
                    .map(out -> OutputSpec.of(out.id(), RtpSendEndpoint.to(out.host(), out.port())))
                    .toList();
            return new BrewSpec(new BrewId(id), name,
                    List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any().withPort(srtPort))), rtp, true);
        }
    }

    public record Destination(String id, String host, int port) {
    }
}
