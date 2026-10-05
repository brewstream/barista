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

import org.brewstream.barista.spi.PortKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangePortAllocatorTest {

    private final RangePortAllocator ports = new RangePortAllocator(PortRange.valueOf("9000-9002"),
            PortRange.valueOf("5000-5024"));

    @Test
    void handsOutSrtPortsLowestFirstUntilTheRangeIsUsed() {
        assertThat(ports.allocate(PortKind.SRT)).isEqualTo(9000);
        assertThat(ports.allocate(PortKind.SRT)).isEqualTo(9001);
        assertThat(ports.allocate(PortKind.SRT)).isEqualTo(9002);
        assertThatThrownBy(() -> ports.allocate(PortKind.SRT)).isInstanceOf(IllegalStateException.class);
    }

    /** P to P+4 must fit: 5020 would need 5024, which is the last port, so it fits; 5030 would not. */
    @Test
    void handsOutRtpBlocksTenApartThatFitWhole() {
        assertThat(ports.allocate(PortKind.RTP_BLOCK)).isEqualTo(5000);
        assertThat(ports.allocate(PortKind.RTP_BLOCK)).isEqualTo(5010);
        assertThat(ports.allocate(PortKind.RTP_BLOCK)).isEqualTo(5020);
        assertThatThrownBy(() -> ports.allocate(PortKind.RTP_BLOCK)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void releasedPortsAreHandedOutAgain() {
        int first = ports.allocate(PortKind.SRT);
        ports.allocate(PortKind.SRT);

        ports.release(PortKind.SRT, first);

        assertThat(ports.allocate(PortKind.SRT)).isEqualTo(first);
    }

    @Test
    void reservingATakenPortFails() {
        ports.reserve(PortKind.SRT, 9001);

        assertThatThrownBy(() -> ports.reserve(PortKind.SRT, 9001)).isInstanceOf(IllegalStateException.class);
        assertThat(ports.allocate(PortKind.SRT)).isEqualTo(9000);
        assertThat(ports.allocate(PortKind.SRT)).as("9001 was reserved").isEqualTo(9002);
    }

    @Test
    void rtpReservationsMustBeBlockBases() {
        assertThatThrownBy(() -> ports.reserve(PortKind.RTP_BLOCK, 5004)).isInstanceOf(IllegalStateException.class);
    }

    /** A fixed port outside the ranges is still taken: naming it twice is refused, not left to fail at bind. */
    @Test
    void tracksFixedPortsOutsideTheRanges() {
        ports.reserve(PortKind.SRT, 7000);

        assertThatThrownBy(() -> ports.reserve(PortKind.SRT, 7000)).isInstanceOf(IllegalStateException.class);
        ports.release(PortKind.SRT, 7000);
        ports.reserve(PortKind.SRT, 7000);
    }

    /** SRT and RTP share one UDP port space: a block outside the ranges cannot cover a fixed SRT port. */
    @Test
    void refusesAnRtpBlockOverAFixedSrtPort() {
        ports.reserve(PortKind.SRT, 7002);

        assertThatThrownBy(() -> ports.reserve(PortKind.RTP_BLOCK, 7000)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("7000-7004");
        ports.reserve(PortKind.RTP_BLOCK, 7003);
        assertThatThrownBy(() -> ports.reserve(PortKind.SRT, 7005)).as("7005 is inside 7003-7007")
                .isInstanceOf(IllegalStateException.class);
    }

    /** A fixed block reaching into the SRT range takes those ports from allocation too. */
    @Test
    void allocationSkipsPortsAFixedBlockReachesInto() {
        RangePortAllocator allocator = new RangePortAllocator(PortRange.valueOf("9000-9004"),
                PortRange.valueOf("5000-5024"));
        allocator.reserve(PortKind.RTP_BLOCK, 8998); // 8998-9002

        assertThat(allocator.allocate(PortKind.SRT)).isEqualTo(9003);
    }

    @Test
    void refusesOverlappingRanges() {
        assertThatThrownBy(() -> new RangePortAllocator(PortRange.valueOf("5000-5100"),
                PortRange.valueOf("5050-5200")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}