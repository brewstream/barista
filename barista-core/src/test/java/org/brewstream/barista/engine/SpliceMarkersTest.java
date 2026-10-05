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

import org.brewstream.barista.SpliceMarker;
import org.brewstream.grind.scte.SpliceCommandType;
import org.brewstream.grind.scte.SpliceEvent;
import org.brewstream.grind.scte.SpliceInfoSection;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SpliceMarkersTest {

    @Test
    void keepsTheMostRecentTwenty() {
        SpliceMarkers markers = new SpliceMarkers();
        for (int i = 1; i <= 25; i++) {
            markers.add(new SpliceEvent(500, timeSignal(i * 90_000L), -1, i));
        }

        List<SpliceMarker> kept = markers.snapshot();
        assertThat(kept).hasSize(SpliceMarkers.CAPACITY);
        assertThat(kept.getFirst().spliceSeconds()).as("the oldest five dropped").isEqualTo(6.0);
        assertThat(kept.getLast().spliceSeconds()).isEqualTo(25.0);
    }

    @Test
    void theSameSectionOnAnotherPidIsAnotherMarker() {
        SpliceMarkers markers = new SpliceMarkers();
        markers.add(new SpliceEvent(500, timeSignal(90_000), -1, 1));
        markers.add(new SpliceEvent(501, timeSignal(90_000), -1, 2));

        assertThat(markers.snapshot()).extracting(SpliceMarker::pid).containsExactly(500, 501);
    }

    private static SpliceInfoSection timeSignal(long pts) {
        return new SpliceInfoSection(SpliceCommandType.TIME_SIGNAL, 0, 0xFFF, false, null, pts, List.of());
    }
}
