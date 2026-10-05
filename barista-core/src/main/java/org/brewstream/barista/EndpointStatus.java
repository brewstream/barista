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

import org.brewstream.grind.TsStreamStats;

import java.util.List;

/**
 * One source or output, as of the snapshot.
 *
 * @param id             the source or output id
 * @param kind           e.g. {@code srt-listener}, {@code rtp-send}
 * @param address        for a listening endpoint, where callers dial it (published host and
 *                       port); otherwise the remote address it dials or sends to
 * @param state          see {@link EndpointState}
 * @param health         {@code GOOD}, {@code DEGRADED} or {@code DOWN}; see {@link EndpointHealth}
 * @param connections    every connection on this leg, each with its own statistics: the
 *                       publisher of an SRT listener source, each subscriber of an SRT listener
 *                       output, the peer of a caller, the RTP sender or receiver. Empty when
 *                       nothing is connected
 * @param chunks         TS chunks passed: received for a source, sent for an output
 * @param bytes          bytes in those chunks
 * @param droppedChunks  for an output: chunks discarded because its queue was full (a slow
 *                       or disconnected peer); 0 for a source
 * @param discardedBytes for a source: bytes that were not part of any TS packet
 * @param tsStats        TS health on this leg from Grind, or {@code null} before any data
 * @param carriesScte35  for a source: whether its program map declares an SCTE-35 splice PID,
 *                       known before any marker arrives (a splice PID is silent between
 *                       breaks). Always false for an output
 * @param spliceMarkers  for a source: the last 20 SCTE-35 markers, oldest first, copies of
 *                       one section counted as one. Always empty for an output
 * @param history        what happened to this leg and why, oldest first, at most the last 50
 *                       events (consecutive repeats count as one)
 * @param error          why the endpoint failed, or {@code null}
 */
public record EndpointStatus(
        String id,
        String kind,
        String address,
        EndpointState state,
        EndpointHealth health,
        List<ConnectionView> connections,
        long chunks,
        long bytes,
        long droppedChunks,
        long discardedBytes,
        TsStreamStats tsStats,
        boolean carriesScte35,
        List<SpliceMarker> spliceMarkers,
        List<EndpointEvent> history,
        String error) {

    public EndpointStatus {
        connections = List.copyOf(connections);
        spliceMarkers = List.copyOf(spliceMarkers);
        history = List.copyOf(history);
    }
}