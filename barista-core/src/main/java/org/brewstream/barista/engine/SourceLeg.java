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

import io.netty.buffer.ByteBuf;
import org.brewstream.barista.ConnectionView;
import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;
import org.brewstream.barista.spec.SourceSpec;

import java.util.List;

/**
 * A source: delivers payloads on the brew loop, which are aligned into TS chunks,
 * monitored, and handed to the brew. Every source is monitored whether it is the
 * active one or not; only the brew decides which one's chunks go on.
 */
abstract class SourceLeg extends Leg {

    final SourceSpec spec;
    private final TsAligner aligner;
    private final LegMonitor monitor = new LegMonitor(true);
    private volatile long lastDataNanos;

    SourceLeg(SourceSpec spec, String kind, BrewContext context) {
        super(spec.id().value(), kind, context, true);
        this.spec = spec;
        this.aligner = new TsAligner(context.allocator());
    }

    /** Takes ownership of {@code payload}. Must be called on the brew loop. */
    protected final void receive(ByteBuf payload) {
        aligner.feed(payload, chunk -> {
            monitor.observe(chunk);
            lastDataNanos = System.nanoTime();
            context.onSourceChunk(this, chunk);
        });
    }

    /** Whether a peer is connected (or, for RTP, has been heard). */
    abstract boolean connected();

    /** The address in the status. */
    abstract String address();

    /** Every connection on this source, with its statistics. */
    abstract List<ConnectionView> connections();

    /** On the brew loop: ACTIVE while data is fresh, IDLE once it is not. */
    void tick(long nowNanos, long lossNanos) {
        if (!connected()) {
            return;
        }
        EndpointState previous = state();
        EndpointState next = fresh(nowNanos, lossNanos) ? EndpointState.ACTIVE : EndpointState.IDLE;
        if (previous == EndpointState.ACTIVE && next == EndpointState.IDLE) {
            event(EndpointEvent.Kind.IDLE, "no data for " + lossNanos / 1_000_000 + " ms");
        } else if (previous == EndpointState.IDLE && next == EndpointState.ACTIVE) {
            event(EndpointEvent.Kind.RESUMED, "data flowing again");
        }
        state(next);
    }

    boolean fresh(long nowNanos, long lossNanos) {
        long last = lastDataNanos;
        return last != 0 && nowNanos - last < lossNanos;
    }

    /** Dials in a row that failed, for a source that dials; 0 for any other. */
    int failedDials() {
        return 0;
    }

    boolean everDelivered() {
        return lastDataNanos != 0;
    }

    /** Releases the aligner's buffer. Call on the brew loop, after the transport is closed. */
    protected final void releaseAligner() {
        context.brewLoop().execute(aligner::release);
    }

    @Override
    EndpointStatus status() {
        return new EndpointStatus(id, kind, address(), state(), health(), connections(), monitor.chunks(), monitor.bytes(),
                0, aligner.discardedBytes(), monitor.tsStats(), monitor.carriesScte35(), monitor.spliceMarkers(), history.snapshot(),
                error);
    }
}