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
import org.brewstream.barista.spec.FailoverPolicy;
import org.brewstream.barista.spec.SourceId;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Decides whether a brew should switch sources by itself; see {@link FailoverPolicy}
 * for the rule. Pure: it looks at a view of each source and changes nothing.
 */
final class Failover {

    /** One source as the brew tick sees it. {@code order} is its place in the spec, for ties. */
    record View(SourceId id, int priority, int order, EndpointHealth health, boolean fresh,
            long deliveringForNanos, int failedDials) {
    }

    /** Switch to {@code target}; {@code reason} says why, in plain words, from the target's side. */
    record Switch(SourceId target, String reason) {
    }

    private static final Comparator<View> PREFERENCE =
            Comparator.comparingInt(View::priority).thenComparingInt(View::order);

    private Failover() {
    }

    /**
     * @param activeForNanos how long the active source has been active
     * @param lossNanos      the source-loss timeout
     */
    static Optional<Switch> decide(FailoverPolicy policy, List<View> sources, SourceId active,
            long activeForNanos, long lossNanos) {
        if (!policy.enabled() || activeForNanos < policy.minDwell().toNanos()) {
            return Optional.empty();
        }
        View current = sources.stream().filter(view -> view.id().equals(active)).findFirst().orElse(null);
        if (current == null) {
            return Optional.empty();
        }
        String lost = lost(policy, current, activeForNanos, lossNanos);
        if (lost != null) {
            return best(sources, current, EndpointHealth.GOOD)
                    .or(() -> best(sources, current, EndpointHealth.DEGRADED))
                    .map(view -> new Switch(view.id(), "failover from " + active.value() + ": " + lost));
        }
        if (policy.failback()) {
            long dwell = policy.minDwell().toNanos();
            return sources.stream()
                    .filter(view -> PREFERENCE.compare(view, current) < 0)
                    .filter(view -> view.health() == EndpointHealth.GOOD && view.fresh()
                            && view.deliveringForNanos() >= dwell)
                    .min(PREFERENCE)
                    .map(view -> new Switch(view.id(), "failback from " + active.value() + ": "
                            + view.id().value() + " delivering for " + view.deliveringForNanos() / 1_000_000 + " ms"));
        }
        return Optional.empty();
    }

    /** Why the active source counts as lost, or {@code null} if it does not. */
    private static String lost(FailoverPolicy policy, View current, long activeForNanos, long lossNanos) {
        if (current.failedDials() >= policy.maxFailedDials()) {
            int dials = current.failedDials();
            return dials == 1 ? "a dial failed" : dials + " dials in a row failed";
        }
        if (!current.fresh() && activeForNanos >= lossNanos) {
            return "no data for " + lossNanos / 1_000_000 + " ms";
        }
        return null;
    }

    private static Optional<View> best(List<View> sources, View current, EndpointHealth health) {
        return sources.stream()
                .filter(view -> view != current && view.health() == health && view.fresh())
                .min(PREFERENCE);
    }
}
