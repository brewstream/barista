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

import java.time.Duration;
import java.util.Objects;

/**
 * Engine tuning.
 *
 * @param queueTime         how much of the input each output may hold when its peer is slower
 *                          than the stream, as time at the measured input bitrate
 * @param minQueueBytes     floor for that, so a low-bitrate brew still absorbs a burst
 * @param maxQueueBytes     ceiling for that, and the size used before a bitrate is measured
 * @param sourceLossTimeout how long the active source may deliver nothing before the brew is
 *                          {@code SOURCE_LOST}, and any source before it is {@code IDLE}
 * @param reconnectMin      first wait before a caller dials again
 * @param reconnectMax      longest wait between dials; the wait doubles up to this
 * @param healthWindow      how much recent history decides whether a leg is {@code DEGRADED};
 *                          the word is judged once per window, so it can lag by up to one
 * @param degradedLossPercent transport loss, as a percentage of packets over a window, above
 *                          which a connected leg is {@code DEGRADED}. See {@link org.brewstream.barista.EndpointHealth}
 *                          for the whole rule
 * @param keyframeDemandWindow how long keyframe extraction keeps running on a source after
 *                          {@code Brew.keyframe()} asks for one; each ask extends it
 */
public record BaristaSettings(Duration queueTime, long minQueueBytes, long maxQueueBytes,
        Duration sourceLossTimeout, Duration reconnectMin, Duration reconnectMax,
        Duration healthWindow, double degradedLossPercent, Duration keyframeDemandWindow) {

    public BaristaSettings {
        Objects.requireNonNull(queueTime, "queueTime");
        Objects.requireNonNull(sourceLossTimeout, "sourceLossTimeout");
        Objects.requireNonNull(reconnectMin, "reconnectMin");
        Objects.requireNonNull(reconnectMax, "reconnectMax");
        Objects.requireNonNull(healthWindow, "healthWindow");
        Objects.requireNonNull(keyframeDemandWindow, "keyframeDemandWindow");
        if (healthWindow.isNegative() || healthWindow.isZero()) {
            throw new IllegalArgumentException("healthWindow must be positive");
        }
        if (!(degradedLossPercent >= 0 && degradedLossPercent <= 100)) {
            throw new IllegalArgumentException("degradedLossPercent must be 0 to 100");
        }
        if (minQueueBytes <= 0 || maxQueueBytes < minQueueBytes) {
            throw new IllegalArgumentException("need 0 < minQueueBytes <= maxQueueBytes");
        }
    }

    /**
     * 2 s of queue per output (256 KiB to 16 MiB), 2 s source loss, reconnect 0.5 s doubling to
     * 10 s, health judged over 5 s windows with more than 1% transport loss counting as degraded,
     * keyframes extracted for 30 s after each ask.
     */
    public static BaristaSettings defaults() {
        return new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024, Duration.ofSeconds(2),
                Duration.ofMillis(500), Duration.ofSeconds(10), Duration.ofSeconds(5), 1.0, Duration.ofSeconds(30));
    }
}