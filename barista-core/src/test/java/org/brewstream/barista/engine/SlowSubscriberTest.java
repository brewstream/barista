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

import org.brewstream.barista.spec.SlowSubscriberPolicy;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.brewstream.barista.engine.SrtListenerOutput.tooFarBehind;

class SlowSubscriberTest {

    private static final long MS = 1_000_000;
    private static final SlowSubscriberPolicy POLICY = SlowSubscriberPolicy.disconnect()
            .withMaxDroppedCapacities(2).withMaxBehindTime(Duration.ofSeconds(10));

    @Test
    void isTooFarBehindOnceItHasDroppedTheLimitInQueues() {
        assertThat(tooFarBehind(POLICY, 2047 * 1024, 1024 * 1024, 0)).isNull();
        assertThat(tooFarBehind(POLICY, 2048 * 1024, 1024 * 1024, 0)).isEqualTo("dropped 2048 KiB without catching up");
    }

    @Test
    void isTooFarBehindOnceItHasBeenBehindTooLong() {
        assertThat(tooFarBehind(POLICY, 1, 1024 * 1024, 9_999 * MS)).isNull();
        assertThat(tooFarBehind(POLICY, 1, 1024 * 1024, 10_000 * MS)).isEqualTo("behind for 10000 ms");
    }

    @Test
    void dropsOldestByDefault() {
        assertThat(SrtListenerEndpoint.any().slowSubscribers()).isEqualTo(SlowSubscriberPolicy.dropOldest());
        assertThat(SrtListenerEndpoint.any().withSlowSubscribers(null).slowSubscribers().action())
                .isEqualTo(SlowSubscriberPolicy.Action.DROP_OLDEST);
        assertThat(SrtListenerEndpoint.any().withSlowSubscribers(POLICY).withPort(9000).slowSubscribers())
                .isEqualTo(POLICY);
    }

    @Test
    void validatesThePolicy() {
        assertThatThrownBy(() -> POLICY.withMaxDroppedCapacities(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> POLICY.withMaxBehindTime(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }
}
