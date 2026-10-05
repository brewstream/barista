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

import org.brewstream.roast.packet.cif.RejectionReason;

import java.net.BindException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** Transport failures in plain words, for operators rather than for protocol engineers. */
final class Reasons {

    private static final String REJECTED = "connection rejected: ";

    private Reasons() {
    }

    /** Why a dial to {@code target} failed. */
    static String dialFailed(Throwable failure, String target) {
        Throwable cause = unwrap(failure);
        String message = cause.getMessage() == null ? "" : cause.getMessage();
        if (cause instanceof TimeoutException) {
            return "no answer from " + target;
        }
        if (message.startsWith(REJECTED)) {
            return rejectedBy(message.substring(REJECTED.length()).trim(), target);
        }
        if (message.contains("passphrase mismatch")) {
            return "passphrase mismatch with " + target;
        }
        if (message.contains("handshake v5")) {
            return target + " runs an SRT too old for this (needs handshake v5)";
        }
        return "could not connect to " + target + ": " + (message.isEmpty() ? cause.toString() : message);
    }

    /** Why opening an endpoint failed: usually a port someone else holds. */
    static String openFailed(Throwable failure) {
        Throwable cause = unwrap(failure);
        while (cause.getCause() != null && !(cause instanceof BindException)) {
            cause = cause.getCause();
        }
        if (cause instanceof BindException) {
            return "port already in use";
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.toString();
    }

    /** A remote peer's rejection code, in words. */
    static String rejectedBy(String code, String target) {
        RejectionReason reason;
        try {
            reason = RejectionReason.valueOf(code);
        } catch (IllegalArgumentException unknown) {
            return target + " rejected the connection (" + code + ")";
        }
        return target + " " + phrase(reason) + " (" + reason.name() + ")";
    }

    /** Barista's own reasons for refusing a caller, in the same words a remote peer's would get. */
    static String refused(RejectionReason reason, String peer, String streamId) {
        String stream = streamId == null || streamId.isEmpty() ? "no stream ID" : "stream ID '" + streamId + "'";
        return switch (reason) {
            case NOTFOUND -> "refused " + peer + ": unknown " + stream;
            case CONFLICT -> "refused " + peer + ": another publisher is already connected";
            default -> "refused " + peer + " (" + reason.name() + ")";
        };
    }

    private static String phrase(RejectionReason reason) {
        return switch (reason) {
            case BADSECRET -> "refused it: wrong passphrase, or the peer is not encrypted";
            case UNSECURE -> "refused it: encryption required but not offered";
            case NOTFOUND -> "does not know the stream ID asked for";
            case FORBIDDEN, UNAUTHORIZED -> "refused it: not allowed";
            case CONFLICT -> "refused it: the stream is already in use";
            case OVERLOAD, BACKLOG, RESOURCE -> "is overloaded and refused it";
            case VERSION -> "speaks an incompatible SRT version";
            case PEER, CLOSE -> "closed the connection while it was being set up";
            default -> "rejected the connection";
        };
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable cause = failure;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}