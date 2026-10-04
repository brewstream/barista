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

package org.brewstream.barista.support;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PortRangeTest {

    @Test
    void parsesARangeOrASinglePort() {
        assertThat(PortRange.valueOf("9000-9099")).isEqualTo(new PortRange(9000, 9099));
        assertThat(PortRange.valueOf(" 9000 - 9099 ")).isEqualTo(new PortRange(9000, 9099));
        assertThat(PortRange.valueOf("9000")).isEqualTo(new PortRange(9000, 9000));
    }

    @Test
    void printsTheWayItIsWritten() {
        assertThat(new PortRange(9000, 9099)).hasToString("9000-9099");
        assertThat(new PortRange(9000, 9000)).hasToString("9000");
    }

    @Test
    void refusesWhatIsNotARange() {
        assertThatThrownBy(() -> PortRange.valueOf("9099-9000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PortRange.valueOf("9000-")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PortRange.valueOf("abc")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("9000-9099");
        assertThatThrownBy(() -> PortRange.valueOf("0-10")).isInstanceOf(IllegalArgumentException.class);
    }
}