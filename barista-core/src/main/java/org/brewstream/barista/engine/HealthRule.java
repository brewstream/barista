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
import org.brewstream.grind.TsStreamStats;

import java.util.HashMap;
import java.util.Map;

/**
 * The one place a leg's {@link EndpointHealth} is decided.
 * <ul>
 *   <li>{@code DOWN} unless the leg is {@link EndpointState#ACTIVE}: connected and, for a source,
 *       delivering within the source-loss timeout.</li>
 *   <li>{@code DEGRADED} when, over the last whole health window
 *       ({@link BaristaSettings#healthWindow()}, 5 s by default), any of:
 *       <ul>
 *         <li>a connection's transport loss exceeds {@link BaristaSettings#degradedLossPercent()}
 *             (1% by default). This is loss on the network, before SRT retransmission or RTP FEC
 *             repair it, so it warns of a struggling link even while the stream is intact. A
 *             source counts what it received against what went missing; an SRT output counts
 *             what its subscriber reported lost against what it sent; an RTP output uses its
 *             receiver's latest report, when one arrived in the window;</li>
 *         <li>any continuity error in the TS on this leg (Grind): data the stream really lost.
 *             An output sees the TS before its queue, so upstream damage shows on it too;</li>
 *         <li>any chunk an output dropped because its queue was full.</li>
 *       </ul></li>
 *   <li>{@code GOOD} otherwise.</li>
 * </ul>
 * Counters are cumulative, so each window is judged on how much they grew. The first window a
 * leg is seen only sets the starting point. {@link #sample} runs on the brew loop;
 * {@link #classify} is safe from any thread.
 */
final class HealthRule {

    private final boolean source;
    private Sample previous;
    private volatile boolean degraded;

    HealthRule(boolean source) {
        this.source = source;
    }

    /** Judges the window that ends with {@code status}. */
    void sample(EndpointStatus status, double lossPercent) {
        Sample current = Sample.of(status);
        if (previous != null) {
            degraded = current.dropped > previous.dropped
                    || current.continuityErrors > previous.continuityErrors
                    || lossy(current, lossPercent);
        }
        previous = current;
    }

    EndpointHealth classify(EndpointState state) {
        if (state != EndpointState.ACTIVE) {
            return EndpointHealth.DOWN;
        }
        return degraded ? EndpointHealth.DEGRADED : EndpointHealth.GOOD;
    }

    private boolean lossy(Sample current, double lossPercent) {
        for (Map.Entry<String, TransportStats> entry : current.connections.entrySet()) {
            TransportStats before = previous.connections.get(entry.getKey());
            if (lossPercent(before, entry.getValue()) > lossPercent) {
                return true;
            }
        }
        return false;
    }

    /** Loss over the window, in percent; {@code before} is null for a connection new in it. */
    private double lossPercent(TransportStats before, TransportStats now) {
        return switch (now) {
            case SrtStats srt -> {
                SrtStats was = before instanceof SrtStats s ? s : null;
                long lost = grown(was == null ? 0 : was.packetsLost(), srt.packetsLost());
                long total = source
                        ? grown(was == null ? 0 : was.packetsReceived(), srt.packetsReceived()) + lost
                        : grown(was == null ? 0 : was.packetsSent(), srt.packetsSent());
                yield percent(lost, total);
            }
            case RtpReceiveStats rtp -> {
                RtpReceiveStats was = before instanceof RtpReceiveStats r ? r : null;
                long lost = grown(was == null ? 0 : was.networkLost(), rtp.networkLost());
                long total = grown(was == null ? 0 : was.packetsReceived(), rtp.packetsReceived()) + lost;
                yield percent(lost, total);
            }
            case RtpSendStats rtp -> {
                long reportsBefore = before instanceof RtpSendStats r ? r.receiverReports() : 0;
                yield rtp.receiverReports() > reportsBefore ? rtp.receiverFractionLost() * 100 : 0;
            }
        };
    }

    /** How much a cumulative counter grew; a counter that went backwards (a reset) grew by none. */
    private static long grown(long before, long now) {
        return Math.max(0, now - before);
    }

    private static double percent(long part, long whole) {
        return whole == 0 ? 0 : part * 100.0 / whole;
    }

    private record Sample(Map<String, TransportStats> connections, long dropped, long continuityErrors) {

        static Sample of(EndpointStatus status) {
            Map<String, TransportStats> connections = new HashMap<>();
            for (ConnectionView connection : status.connections()) {
                connections.put(connection.id(), connection.stats());
            }
            TsStreamStats ts = status.tsStats();
            return new Sample(connections, status.droppedChunks(), ts == null ? 0 : ts.continuityErrors());
        }
    }
}
