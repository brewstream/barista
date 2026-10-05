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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What one output has been given and not yet sent, bounded in bytes. Filled
 * from the brew's event loop and drained from the output's, so a slow output
 * only ever fills its own queue: the source and the other outputs never wait
 * on it.
 *
 * <p>When full, the oldest chunk is dropped. A live output wants the newest
 * data; by the time old data could be sent it is useless. Every drop is
 * counted.
 */
final class OutputQueue {

    private final ConcurrentLinkedQueue<ByteBuf> chunks = new ConcurrentLinkedQueue<>();
    private final AtomicLong queuedBytes = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private volatile long capacityBytes;
    private volatile Runnable onDrop = () -> { };
    // How far behind: bytes dropped since the queue last ran empty, and since when.
    // Written where chunks are offered; poll() only flags that the queue ran empty.
    private long behindDroppedBytes;
    private long behindSinceNanos;
    private volatile boolean ranEmpty = true;

    OutputQueue(long capacityBytes) {
        this.capacityBytes = capacityBytes;
    }

    /** Takes ownership of {@code chunk}. */
    void offer(ByteBuf chunk) {
        // Count first: once queued, the output's loop may send and release it at once.
        queuedBytes.addAndGet(chunk.readableBytes());
        chunks.add(chunk);
        while (queuedBytes.get() > capacityBytes) {
            ByteBuf oldest = chunks.poll();
            if (oldest == null) {
                return;
            }
            if (ranEmpty) {
                ranEmpty = false;
                behindDroppedBytes = 0;
                behindSinceNanos = System.nanoTime();
            }
            behindDroppedBytes += oldest.readableBytes();
            queuedBytes.addAndGet(-oldest.readableBytes());
            dropped.incrementAndGet();
            oldest.release();
            onDrop.run();
        }
    }

    /** The oldest chunk, now owned by the caller, or {@code null}. */
    ByteBuf poll() {
        ByteBuf chunk = chunks.poll();
        if (chunk != null) {
            queuedBytes.addAndGet(-chunk.readableBytes());
        } else {
            ranEmpty = true;
        }
        return chunk;
    }

    /** Releases everything queued, without counting it as dropped. */
    void clear() {
        ByteBuf chunk;
        while ((chunk = poll()) != null) {
            chunk.release();
        }
    }

    /** Called after each chunk dropped for space. */
    void onDrop(Runnable action) {
        onDrop = action;
    }

    void capacity(long bytes) {
        capacityBytes = bytes;
    }

    long capacity() {
        return capacityBytes;
    }

    long queuedBytes() {
        return queuedBytes.get();
    }

    /** Bytes dropped since the queue last ran empty. Read where chunks are offered, e.g. in onDrop. */
    long behindDroppedBytes() {
        return behindDroppedBytes;
    }

    /** When the current run of drops began ({@link System#nanoTime()}). Read where chunks are offered. */
    long behindSinceNanos() {
        return behindSinceNanos;
    }

    long dropped() {
        return dropped.get();
    }
}