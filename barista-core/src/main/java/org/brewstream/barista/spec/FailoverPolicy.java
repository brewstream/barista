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

import java.time.Duration;
import java.util.Objects;

/**
 * Whether a brew switches sources by itself, and how cautiously.
 *
 * <p>When the active source is lost (it has delivered nothing for the source-loss
 * timeout) or, for an SRT caller, {@code maxFailedDials} dials in a row have failed,
 * the brew activates the healthiest other source by priority: a {@code GOOD} one if
 * any, else a {@code DEGRADED} one; never one that is {@code DOWN}. With nowhere to go
 * it stays where it is.
 *
 * <p>Every source keeps its own connection to its own fixed target, so a switch only
 * changes which one feeds the outputs; nothing redials.
 *
 * @param enabled        whether the brew switches by itself. Off by default; an operator
 *                       can always switch with {@code Barista.activate}
 * @param minDwell       how long a source stays active before the brew switches away from
 *                       it by itself; for failback, also how long the preferred source must
 *                       have been delivering
 * @param maxFailedDials for an active SRT caller source, failed dials in a row that count
 *                       as losing it
 * @param failback       whether a more preferred source takes over again once it has been
 *                       healthy for {@code minDwell}. Off by default: switching back on air
 *                       is a glitch the operator should choose
 */
public record FailoverPolicy(boolean enabled, Duration minDwell, int maxFailedDials, boolean failback) {

    public FailoverPolicy {
        Objects.requireNonNull(minDwell, "minDwell");
        if (minDwell.isNegative()) {
            throw new IllegalArgumentException("minDwell must not be negative");
        }
        if (maxFailedDials < 1) {
            throw new IllegalArgumentException("maxFailedDials must be at least 1");
        }
    }

    /** No automatic switching; the default. */
    public static FailoverPolicy off() {
        return new FailoverPolicy(false, Duration.ofSeconds(10), 3, false);
    }

    /** Switches on loss, staying at least 10 s on a source, after 3 failed dials, without failback. */
    public static FailoverPolicy automatic() {
        return new FailoverPolicy(true, Duration.ofSeconds(10), 3, false);
    }

    public FailoverPolicy withMinDwell(Duration minDwell) {
        return new FailoverPolicy(enabled, minDwell, maxFailedDials, failback);
    }

    public FailoverPolicy withMaxFailedDials(int maxFailedDials) {
        return new FailoverPolicy(enabled, minDwell, maxFailedDials, failback);
    }

    public FailoverPolicy withFailback(boolean failback) {
        return new FailoverPolicy(enabled, minDwell, maxFailedDials, failback);
    }
}
