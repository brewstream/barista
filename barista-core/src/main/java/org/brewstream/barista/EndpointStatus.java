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

/**
 * One source or output, as of the snapshot.
 *
 * @param id             the source or output id
 * @param kind           e.g. {@code srt-listener}, {@code rtp-send}
 * @param address        for a listening endpoint, where callers dial it (published host and
 *                       port); otherwise the remote address it dials or sends to
 * @param state          see {@link EndpointState}
 * @param connections    connected peers: publishers or subscribers for SRT listeners, 0 or 1
 *                       for callers
 * @param chunks         TS chunks passed: received for a source, sent for an output
 * @param bytes          bytes in those chunks
 * @param droppedChunks  for an output: chunks discarded because its queue was full (a slow
 *                       or disconnected peer); 0 for a source
 * @param discardedBytes for a source: bytes that were not part of any TS packet
 * @param health         TS health on this leg from Grind, or {@code null} before any data
 * @param transport      the transport's own statistics (Roast {@code ConnectionStats}, Press
 *                       {@code ReceiverStats} or {@code SenderStats}), or {@code null}
 * @param error          why the endpoint failed, or {@code null}
 */
public record EndpointStatus(
        String id,
        String kind,
        String address,
        EndpointState state,
        int connections,
        long chunks,
        long bytes,
        long droppedChunks,
        long discardedBytes,
        TsStreamStats health,
        Object transport,
        String error) {
}