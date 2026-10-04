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
import io.netty.channel.EventLoop;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One output's queue and the loop that empties it into its transport. Offering
 * schedules a drain on the output's event loop; the drain sends while the
 * transport can take more, and stops when it cannot. Whoever owns the transport
 * calls {@link #schedule()} again when it can (Roast's channel writability).
 */
final class Lane {

    /** Where a lane's chunks go. Called on the lane's event loop. */
    interface Writer {
        /** Whether a chunk can be written now without piling up in the transport. */
        boolean writable();

        /** Takes ownership of {@code chunk}. */
        void write(ByteBuf chunk);

        /**
         * The queue has run dry: send anything the transport is holding back for
         * a fuller packet. Without this, a short tail waits for data that may not
         * come, or goes out mixed with the next source's after a switch.
         */
        default void flush() {
        }
    }

    private final EventLoop loop;
    private final OutputQueue queue;
    private final LegMonitor monitor;
    private final Writer writer;
    private final AtomicBoolean scheduled = new AtomicBoolean();

    /** @param monitor observes chunks as they are written, or {@code null} to observe elsewhere */
    Lane(EventLoop loop, OutputQueue queue, LegMonitor monitor, Writer writer) {
        this.loop = loop;
        this.queue = queue;
        this.monitor = monitor;
        this.writer = writer;
    }

    /** Takes ownership of {@code chunk}. Safe from any thread. */
    void offer(ByteBuf chunk) {
        queue.offer(chunk);
        schedule();
    }

    void schedule() {
        if (scheduled.compareAndSet(false, true)) {
            loop.execute(this::drain);
        }
    }

    OutputQueue queue() {
        return queue;
    }

    private void drain() {
        scheduled.set(false);
        while (writer.writable()) {
            ByteBuf chunk = queue.poll();
            if (chunk == null) {
                writer.flush();
                return;
            }
            if (monitor != null) {
                monitor.observe(chunk);
            }
            writer.write(chunk);
        }
    }
}