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
 * Something that happened to a source or output, and why, for answering "why did
 * this destination drop at 14:32?". Consecutive repeats (the same kind and
 * reason, such as a caller failing to dial every few seconds) are one event with
 * a count, so a flapping peer cannot push everything else out of the history.
 *
 * @param kind        what happened
 * @param reason      in plain words, e.g. "wrong passphrase, or the peer is not encrypted"
 * @param firstMillis when it first happened (epoch milliseconds)
 * @param lastMillis  when it last happened; equal to {@code firstMillis} unless repeated
 * @param count       how many times in a row
 */
public record EndpointEvent(Kind kind, String reason, long firstMillis, long lastMillis, int count) {

    public enum Kind {
        /** The endpoint opened: listening, dialling, receiving or sending. */
        STARTED,
        /** A peer connected, or an RTP sender was first heard. */
        CONNECTED,
        /** A peer went away. */
        DISCONNECTED,
        /** A dial or a bind failed. */
        FAILED,
        /** Barista refused a caller. */
        REJECTED,
        /** A source stopped delivering for the source-loss timeout. */
        IDLE,
        /** A source started delivering again. */
        RESUMED,
        /** This source became the one feeding the outputs. */
        ACTIVATED,
        /** This source stopped feeding the outputs. */
        DEACTIVATED,
        /** An RTP source moved to a new sender (a new SSRC): an encoder restart, usually. */
        SENDER_CHANGED,
        /** An RTP sender said it stopped (RTCP BYE). */
        GOODBYE,
        /** An output's queue is full and it is dropping the oldest data. */
        DROPPING,
        /** The endpoint closed. */
        STOPPED
    }
}