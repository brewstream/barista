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

import org.brewstream.barista.EndpointHealth;
import org.brewstream.barista.engine.Failover.Switch;
import org.brewstream.barista.engine.Failover.View;
import org.brewstream.barista.spec.FailoverPolicy;
import org.brewstream.barista.spec.SourceId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FailoverTest {

    private static final long MS = 1_000_000;
    private static final long LOSS = 2000 * MS;
    private static final FailoverPolicy ON = FailoverPolicy.automatic().withMinDwell(Duration.ofSeconds(10));
    private static final SourceId MAIN = new SourceId("main");

    @Test
    void doesNothingWhenOff() {
        assertThat(decide(FailoverPolicy.off(), List.of(lost("main", 0), good("backup", 1)), 60_000))
                .isEmpty();
    }

    @Test
    void switchesToTheNextHealthySourceByPriorityNotSpecOrder() {
        Optional<Switch> decision = decide(ON,
                List.of(good("spare", 2), lost("main", 0), good("backup", 1)), 60_000);

        assertThat(decision).contains(new Switch(new SourceId("backup"), "failover from main: no data for 2000 ms"));
    }

    @Test
    void breaksAPriorityTieBySpecOrder() {
        assertThat(decide(ON, List.of(lost("main", 0), good("first", 1, 1), good("second", 1, 2)), 60_000))
                .map(Switch::target).contains(new SourceId("first"));
    }

    @Test
    void prefersAGoodSourceToADegradedOneOfBetterPriority() {
        assertThat(decide(ON, List.of(lost("main", 0), degraded("backup", 1), good("spare", 2)), 60_000))
                .map(Switch::target).contains(new SourceId("spare"));
    }

    @Test
    void takesADegradedSourceWhenNoneIsGood() {
        assertThat(decide(ON, List.of(lost("main", 0), down("backup", 1), degraded("spare", 2)), 60_000))
                .map(Switch::target).contains(new SourceId("spare"));
    }

    @Test
    void staysWhenThereIsNowhereHealthyToGo() {
        assertThat(decide(ON, List.of(lost("main", 0), down("backup", 1)), 60_000)).isEmpty();
    }

    @Test
    void ignoresASourceThatIsGoodButNotDelivering() {
        View connectedNoData = new View(new SourceId("backup"), 1, 1, EndpointHealth.GOOD, false, 0, 0);
        assertThat(decide(ON, List.of(lost("main", 0), connectedNoData), 60_000)).isEmpty();
    }

    @Test
    void neverSwitchesFasterThanTheDwell() {
        assertThat(decide(ON, List.of(lost("main", 0), good("backup", 1)), 9_999)).isEmpty();
        assertThat(decide(ON, List.of(lost("main", 0), good("backup", 1)), 10_000)).isPresent();
    }

    @Test
    void givesANewlyActivatedSourceTheLossTimeoutToDeliver() {
        FailoverPolicy noDwell = ON.withMinDwell(Duration.ZERO);

        assertThat(decide(noDwell, List.of(lost("main", 0), good("backup", 1)), 1_999)).isEmpty();
        assertThat(decide(noDwell, List.of(lost("main", 0), good("backup", 1)), 2_000)).isPresent();
    }

    @Test
    void aDeliveringSourceIsNotLost() {
        assertThat(decide(ON, List.of(good("main", 0), good("backup", 1)), 60_000)).isEmpty();
    }

    @Test
    void failedDialsCountAsLostOnceTheyReachTheLimit() {
        View dialling = new View(MAIN, 0, 0, EndpointHealth.DOWN, false, 0, 2);
        FailoverPolicy noDwell = ON.withMinDwell(Duration.ZERO);

        assertThat(decide(noDwell, List.of(dialling, good("backup", 1)), 100)).isEmpty();
        View third = new View(MAIN, 0, 0, EndpointHealth.DOWN, false, 0, 3);
        assertThat(decide(noDwell, List.of(third, good("backup", 1)), 100))
                .contains(new Switch(new SourceId("backup"), "failover from main: 3 dials in a row failed"));
    }

    @Test
    void doesNotFailBackUnlessConfigured() {
        List<View> recovered = List.of(good("main", 0), good("backup", 1));

        assertThat(decide(ON, recovered, new SourceId("backup"), 60_000)).isEmpty();
    }

    @Test
    void failsBackToTheMostPreferredSourceThatDeliveredForTheDwell() {
        FailoverPolicy failback = ON.withFailback(true);
        List<View> sources = List.of(good("main", 0), good("second", 1), good("backup", 2));

        assertThat(decide(failback, sources, new SourceId("backup"), 60_000))
                .contains(new Switch(MAIN, "failback from backup: main delivering for 60000 ms"));
    }

    @Test
    void failsBackOnlyAfterThePreferredSourceDeliveredForTheDwell() {
        FailoverPolicy failback = ON.withFailback(true);
        View justBack = new View(MAIN, 0, 0, EndpointHealth.GOOD, true, 9_999 * MS, 0);

        assertThat(decide(failback, List.of(justBack, good("backup", 1)), new SourceId("backup"), 60_000))
                .isEmpty();
    }

    @Test
    void neverFailsBackToALessPreferredSource() {
        FailoverPolicy failback = ON.withFailback(true);

        assertThat(decide(failback, List.of(good("main", 0), good("backup", 1)), 60_000)).isEmpty();
    }

    @Test
    void validatesThePolicy() {
        assertThatThrownBy(() -> ON.withMaxFailedDials(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ON.withMinDwell(Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
    }

    private static Optional<Switch> decide(FailoverPolicy policy, List<View> sources, long activeForMillis) {
        return decide(policy, sources, MAIN, activeForMillis);
    }

    private static Optional<Switch> decide(FailoverPolicy policy, List<View> sources, SourceId active,
            long activeForMillis) {
        return Failover.decide(policy, sources, active, activeForMillis * MS, LOSS);
    }

    private static View good(String id, int priority) {
        return good(id, priority, priority);
    }

    private static View good(String id, int priority, int order) {
        return new View(new SourceId(id), priority, order, EndpointHealth.GOOD, true, 60_000 * MS, 0);
    }

    private static View degraded(String id, int priority) {
        return new View(new SourceId(id), priority, priority, EndpointHealth.DEGRADED, true, 60_000 * MS, 0);
    }

    private static View down(String id, int priority) {
        return new View(new SourceId(id), priority, priority, EndpointHealth.DOWN, false, 0, 0);
    }

    private static View lost(String id, int priority) {
        return new View(new SourceId(id), priority, priority, EndpointHealth.DOWN, false, 0, 0);
    }
}
