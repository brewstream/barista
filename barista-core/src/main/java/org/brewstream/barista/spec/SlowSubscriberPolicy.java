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
 * What an SRT listener output does with a subscriber that falls too far behind.
 *
 * <p>Every subscriber has its own queue. When a subscriber is slower than the
 * stream its queue fills, and the oldest data is dropped: it keeps watching,
 * with gaps. A subscriber is <em>behind</em> from its first drop until its queue
 * next runs empty. One that stays behind can never catch up with live, and under
 * {@link #disconnect()} it is disconnected instead, so it can rejoin at the live
 * edge; the reason goes into the output's event history.
 *
 * <p>Applies to SRT listener outputs only, and is ignored on a source. A caller
 * output would only redial, and RTP has no connection to close.
 *
 * @param action               what to do with a subscriber that stays behind
 * @param maxDroppedCapacities how much it may drop while behind, in multiples of its queue
 *                             capacity, before it is disconnected
 * @param maxBehindTime        how long it may stay behind before it is disconnected
 */
public record SlowSubscriberPolicy(Action action, double maxDroppedCapacities, Duration maxBehindTime) {

    public enum Action {
        /** Keep it, dropping its oldest data. */
        DROP_OLDEST,
        /** Disconnect it, so it can rejoin at the live edge. */
        DISCONNECT
    }

    public SlowSubscriberPolicy {
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(maxBehindTime, "maxBehindTime");
        if (!(maxDroppedCapacities > 0)) {
            throw new IllegalArgumentException("maxDroppedCapacities must be positive");
        }
        if (maxBehindTime.isNegative() || maxBehindTime.isZero()) {
            throw new IllegalArgumentException("maxBehindTime must be positive");
        }
    }

    /** Keep a slow subscriber, dropping its oldest data; the default. */
    public static SlowSubscriberPolicy dropOldest() {
        return new SlowSubscriberPolicy(Action.DROP_OLDEST, 3, Duration.ofSeconds(10));
    }

    /** Disconnect a subscriber once it has dropped three queues' worth, or stayed behind 10 s. */
    public static SlowSubscriberPolicy disconnect() {
        return new SlowSubscriberPolicy(Action.DISCONNECT, 3, Duration.ofSeconds(10));
    }

    public SlowSubscriberPolicy withMaxDroppedCapacities(double maxDroppedCapacities) {
        return new SlowSubscriberPolicy(action, maxDroppedCapacities, maxBehindTime);
    }

    public SlowSubscriberPolicy withMaxBehindTime(Duration maxBehindTime) {
        return new SlowSubscriberPolicy(action, maxDroppedCapacities, maxBehindTime);
    }
}
