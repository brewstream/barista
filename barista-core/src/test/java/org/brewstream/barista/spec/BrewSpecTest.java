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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrewSpecTest {

    @Test
    void needsASource() {
        assertThatThrownBy(() -> BrewSpec.of("empty", List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesDuplicateIds() {
        assertThatThrownBy(() -> BrewSpec.of("dup",
                List.of(SourceSpec.of("a", 0, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("a", 1, RtpReceiveEndpoint.unicast())),
                List.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void prefersTheLowestPrioritySource() {
        BrewSpec spec = BrewSpec.of("pair",
                List.of(SourceSpec.of("backup", 5, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("main", 1, SrtListenerEndpoint.any())),
                List.of());

        assertThat(spec.preferredSource().id()).isEqualTo(new SourceId("main"));
    }

    @Test
    void failoverIsOffUnlessAskedForAndSurvivesOtherChanges() {
        BrewSpec spec = BrewSpec.of("one", List.of(SourceSpec.of("a", 0, RtpReceiveEndpoint.unicast())), List.of());
        assertThat(spec.failover().enabled()).isFalse();
        assertThat(new BrewSpec(spec.id(), "n", spec.sources(), List.of(), true, null).failover())
                .isEqualTo(FailoverPolicy.off());

        BrewSpec automatic = spec.withFailover(FailoverPolicy.automatic());
        assertThat(automatic.failover().failback()).as("failback off by default").isFalse();
        assertThat(automatic.withEnabled(false).withOutputs(List.of()).withSources(spec.sources()).failover())
                .isEqualTo(FailoverPolicy.automatic());
    }

    @Test
    void validatesEndpoints() {
        assertThatThrownBy(() -> new SrtSecurity("short", 16)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SrtSecurity("long-enough-secret", 20)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RtpSendEndpoint.to("h", 5000).withFec(5, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RtpReceiveEndpoint(0, null, "10.0.0.1", null, false, java.time.Duration.ZERO))
                .as("a source filter needs multicast").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BrewId("has space")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void neverPrintsThePassphrase() {
        assertThat(new SrtSecurity("super-secret-phrase", 16).toString()).doesNotContain("super-secret");
    }
}