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

import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointEvent.Kind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventHistoryTest {

    private final EventHistory history = new EventHistory();

    @Test
    void collapsesConsecutiveRepeatsIntoACount() {
        history.add(Kind.STARTED, "dialling a:1");
        history.add(Kind.FAILED, "no answer from a:1");
        history.add(Kind.FAILED, "no answer from a:1");
        EndpointEvent third = history.add(Kind.FAILED, "no answer from a:1");

        assertThat(history.snapshot()).hasSize(2);
        assertThat(third.count()).isEqualTo(3);
        assertThat(third.lastMillis()).isGreaterThanOrEqualTo(third.firstMillis());
    }

    @Test
    void keepsDifferentReasonsApart() {
        history.add(Kind.FAILED, "no answer from a:1");
        history.add(Kind.FAILED, "a:1 refused it: wrong passphrase, or the peer is not encrypted (BADSECRET)");
        history.add(Kind.FAILED, "no answer from a:1");

        assertThat(history.snapshot()).hasSize(3);
    }

    @Test
    void keepsOnlyTheLatestFifty() {
        for (int i = 0; i < 60; i++) {
            history.add(Kind.CONNECTED, "peer " + i);
        }

        assertThat(history.snapshot()).hasSize(EventHistory.CAPACITY);
        assertThat(history.snapshot().getFirst().reason()).isEqualTo("peer 10");
        assertThat(history.snapshot().getLast().reason()).isEqualTo("peer 59");
    }
}