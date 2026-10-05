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

package org.brewstream.barista;

/**
 * An SCTE-35 splice marker that entered a brew through a source: what cue arrived,
 * when, and how much warning it gave. Copies of one section, which muxers send for
 * redundancy, are one marker with a count. Barista passes cues through unchanged.
 *
 * @param pid            the splice PID it arrived on
 * @param command        the SCTE-35 command, e.g. {@code splice_insert}, {@code time_signal}
 * @param description    in one line, e.g. "event 1001 out for 2.0s"
 * @param firstMillis    when the first copy arrived (epoch milliseconds)
 * @param lastMillis     when the last copy arrived; equal to {@code firstMillis} for one copy
 * @param arrivalSeconds stream time (program clock) when the first copy arrived, or -1 if
 *                       no clock had been seen yet
 * @param spliceSeconds  stream time at which the splice takes effect, or -1 when the
 *                       section names no time (an immediate splice, a {@code splice_null})
 * @param preRollSeconds warning the first copy gave: splice time minus the program clock (PCR)
 *                       at arrival, or -1 when either is unknown. Negative means it arrived
 *                       after its splice point. Measured against the PCR, this is time until
 *                       the splice is presented; equipment that acts on frames as they arrive
 *                       has less, by the stream's mux delay (video PTS runs ahead of the PCR)
 * @param count          copies received
 */
public record SpliceMarker(int pid, String command, String description, long firstMillis, long lastMillis,
        double arrivalSeconds, double spliceSeconds, double preRollSeconds, int count) {
}
