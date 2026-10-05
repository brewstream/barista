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

import org.brewstream.barista.ConnectionView;
import org.brewstream.barista.EndpointHealth;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;
import org.brewstream.barista.RtpReceiveStats;
import org.brewstream.barista.RtpSendStats;
import org.brewstream.barista.SrtStats;
import org.brewstream.barista.TransportStats;
import org.brewstream.barista.TsFixtures;
import org.brewstream.grind.TsAnalyzer;
import org.brewstream.grind.TsPacket;
import org.brewstream.grind.TsStreamStats;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HealthRuleTest {

    private static final double LOSS_PERCENT = 1.0;

    private final HealthRule source = new HealthRule(true);
    private final HealthRule output = new HealthRule(false);

    @Test
    void aLegThatIsNotActiveIsDown() {
        for (EndpointState state : EndpointState.values()) {
            if (state != EndpointState.ACTIVE) {
                assertThat(source.classify(state)).as("source %s", state).isEqualTo(EndpointHealth.DOWN);
                assertThat(output.classify(state)).as("output %s", state).isEqualTo(EndpointHealth.DOWN);
            }
        }
    }

    @Test
    void anActiveLegIsGoodBeforeAnyWindowAndAfterACleanOne() {
        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.GOOD);
        source.sample(status(srtReceived(1000, 0), 0, clean()), LOSS_PERCENT);
        source.sample(status(srtReceived(2000, 5), 0, clean()), LOSS_PERCENT);
        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.GOOD);

        output.sample(status(srtSent(1000, 0), 0, clean()), LOSS_PERCENT);
        output.sample(status(srtSent(2000, 5), 0, clean()), LOSS_PERCENT);
        assertThat(output.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.GOOD);
    }

    @Test
    void theFirstWindowOnlySetsTheStartingPoint() {
        source.sample(status(srtReceived(1000, 500), 10, damaged()), LOSS_PERCENT);

        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.GOOD);
    }

    @Test
    void aSourceLosingMoreThanTheThresholdIsDegraded() {
        source.sample(status(srtReceived(1000, 0), 0, clean()), LOSS_PERCENT);
        source.sample(status(srtReceived(1980, 20), 0, clean()), LOSS_PERCENT); // 20 of 1000: 2%

        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void lossIsJudgedOnTheWindowNotTheTotal() {
        source.sample(status(srtReceived(1000, 100), 0, clean()), LOSS_PERCENT);
        source.sample(status(srtReceived(101_000, 105), 0, clean()), LOSS_PERCENT);

        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.GOOD);
    }

    @Test
    void anRtpSourceCountsNetworkLoss() {
        source.sample(status(rtpReceived(1000, 0), 0, clean()), LOSS_PERCENT);
        source.sample(status(rtpReceived(1950, 50), 0, clean()), LOSS_PERCENT);

        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void continuityErrorsDegradeASource() {
        source.sample(status(srtReceived(1000, 0), 0, clean()), LOSS_PERCENT);
        source.sample(status(srtReceived(2000, 0), 0, damaged()), LOSS_PERCENT);

        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void aSourceRecoversAfterACleanWindow() {
        source.sample(status(srtReceived(1000, 0), 0, clean()), LOSS_PERCENT);
        source.sample(status(srtReceived(2000, 0), 0, damaged()), LOSS_PERCENT);
        source.sample(status(srtReceived(3000, 0), 0, damaged()), LOSS_PERCENT);

        assertThat(source.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.GOOD);
    }

    @Test
    void anOutputDroppingFromItsQueueIsDegraded() {
        output.sample(status(srtSent(1000, 0), 3, clean()), LOSS_PERCENT);
        output.sample(status(srtSent(2000, 0), 4, clean()), LOSS_PERCENT);

        assertThat(output.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void anSrtOutputCountsWhatItsSubscriberReportedLost() {
        output.sample(status(srtSent(1000, 0), 0, clean()), LOSS_PERCENT);
        output.sample(status(srtSent(2000, 20), 0, clean()), LOSS_PERCENT);

        assertThat(output.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void anyOneSubscriberOverTheThresholdDegradesTheOutput() {
        output.sample(status(List.of(view("a", srtSent(1000, 0)), view("b", srtSent(1000, 0))), 0, clean()),
                LOSS_PERCENT);
        output.sample(status(List.of(view("a", srtSent(2000, 0)), view("b", srtSent(2000, 50))), 0, clean()),
                LOSS_PERCENT);

        assertThat(output.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void anRtpOutputUsesOnlyAReceiverReportFromThisWindow() {
        output.sample(status(rtpSent(1, 0.05), 0, clean()), LOSS_PERCENT);
        output.sample(status(rtpSent(1, 0.05), 0, clean()), LOSS_PERCENT);
        assertThat(output.classify(EndpointState.ACTIVE)).as("stale report").isEqualTo(EndpointHealth.GOOD);

        output.sample(status(rtpSent(2, 0.05), 0, clean()), LOSS_PERCENT);
        assertThat(output.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void continuityErrorsDegradeAnOutput() {
        output.sample(status(rtpSent(0, 0), 0, clean()), LOSS_PERCENT);
        output.sample(status(rtpSent(0, 0), 0, damaged()), LOSS_PERCENT);

        assertThat(output.classify(EndpointState.ACTIVE)).isEqualTo(EndpointHealth.DEGRADED);
    }

    @Test
    void aDegradedLegThatDisconnectsIsDown() {
        output.sample(status(srtSent(1000, 0), 0, clean()), LOSS_PERCENT);
        output.sample(status(srtSent(2000, 0), 1, clean()), LOSS_PERCENT);

        assertThat(output.classify(EndpointState.RECONNECTING)).isEqualTo(EndpointHealth.DOWN);
    }

    private static EndpointStatus status(TransportStats stats, long dropped, TsStreamStats ts) {
        return status(List.of(view("1", stats)), dropped, ts);
    }

    private static EndpointStatus status(List<ConnectionView> connections, long dropped, TsStreamStats ts) {
        return new EndpointStatus("leg", "test", "h:1", EndpointState.ACTIVE, EndpointHealth.GOOD, connections,
                0, 0, dropped, 0, ts, false, List.of(), List.of(), null);
    }

    private static ConnectionView view(String id, TransportStats stats) {
        return new ConnectionView(id, null, "h:2", 0, stats);
    }

    private static SrtStats srtReceived(long received, long lost) {
        return new SrtStats(0, 0, 0, received, 0, 0, lost, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static SrtStats srtSent(long sent, long lost) {
        return new SrtStats(0, 0, sent, 0, 0, 0, lost, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static RtpReceiveStats rtpReceived(long received, long networkLost) {
        return new RtpReceiveStats(received, 0, networkLost, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static RtpSendStats rtpSent(long reports, double fractionLost) {
        return new RtpSendStats(0, 0, 0, reports, fractionLost, 0, 0, -1);
    }

    /** Grind's statistics for 20 packets in order. */
    private static TsStreamStats clean() {
        return analyse(TsFixtures.packets(0, 20));
    }

    /** Grind's statistics for 20 packets with two gaps in the continuity counter. */
    private static TsStreamStats damaged() {
        byte[] a = TsFixtures.packets(0, 10);
        byte[] b = TsFixtures.packets(13, 5);
        byte[] c = TsFixtures.packets(21, 5);
        byte[] ts = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, ts, 0, a.length);
        System.arraycopy(b, 0, ts, a.length, b.length);
        System.arraycopy(c, 0, ts, a.length + b.length, c.length);
        TsStreamStats stats = analyse(ts);
        assertThat(stats.continuityErrors()).isPositive();
        return stats;
    }

    private static TsStreamStats analyse(byte[] ts) {
        TsAnalyzer analyzer = new TsAnalyzer();
        for (int offset = 0; offset < ts.length; offset += 188) {
            analyzer.consume(TsPacket.parse(ts, offset));
        }
        return analyzer.stats();
    }
}
