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
 * One connection on a source or output: an SRT publisher, caller or subscriber,
 * or the RTP sender or receiver at the other end.
 *
 * @param id                   unique within its leg: the SRT socket id, or the RTP SSRC, in hex
 * @param streamId             the SRT stream ID asked for, or {@code null}
 * @param peer                 the other end as {@code host:port}, or {@code null} if not yet known
 * @param connectedSinceMillis when Barista saw this connection start (epoch milliseconds). For an
 *                             RTP source, when the current sender was first heard
 * @param stats                the transport's statistics
 */
public record ConnectionView(String id, String streamId, String peer, long connectedSinceMillis,
        TransportStats stats) {
}