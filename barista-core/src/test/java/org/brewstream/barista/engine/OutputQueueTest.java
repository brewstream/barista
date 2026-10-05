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
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutputQueueTest {

    @Test
    void dropsTheOldestWhenFullAndCountsIt() {
        OutputQueue queue = new OutputQueue(3 * 100);
        ByteBuf[] chunks = new ByteBuf[5];
        for (int i = 0; i < 5; i++) {
            chunks[i] = Unpooled.buffer(100).writeZero(99).writeByte(i);
            queue.offer(chunks[i]);
        }

        assertThat(queue.dropped()).isEqualTo(2);
        assertThat(chunks[0].refCnt()).as("dropped chunks are released").isZero();
        assertThat(chunks[1].refCnt()).isZero();
        assertThat(queue.poll().getByte(99)).as("the newest survive").isEqualTo((byte) 2);
        assertThat(queue.queuedBytes()).isEqualTo(200);
    }

    /**
     * The output's loop can take a chunk, send it (consuming its bytes) and
     * release it the moment it is in the queue. The producer must have counted
     * its size before then. This chunk stalls the producer's first look at its
     * size until the consumer has taken and consumed it, which is the
     * interleaving that used to leave the count at -188 on an empty queue.
     * (Adapted from the Codex review.)
     */
    @Test
    void countsAChunkBeforeTheConsumerCanTakeIt() throws Exception {
        OutputQueue queue = new OutputQueue(188);
        java.util.concurrent.CountDownLatch published = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch consumed = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Thread> producerThread = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicBoolean stalled = new java.util.concurrent.atomic.AtomicBoolean();
        @SuppressWarnings("deprecation")
        ByteBuf chunk = new io.netty.buffer.DuplicatedByteBuf(Unpooled.buffer(188).writeZero(188)) {
            @Override
            public int readableBytes() {
                if (Thread.currentThread() == producerThread.get() && stalled.compareAndSet(false, true)) {
                    published.countDown();
                    try {
                        consumed.await(500, java.util.concurrent.TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.readableBytes();
            }
        };
        Thread producer = new Thread(() -> {
            producerThread.set(Thread.currentThread());
            queue.offer(chunk);
        });
        producer.start();

        published.await(2, java.util.concurrent.TimeUnit.SECONDS);
        ByteBuf taken = queue.poll();
        if (taken != null) {
            taken.skipBytes(taken.readableBytes()); // as Press's sender consumes it
        }
        consumed.countDown();
        producer.join(2000);
        ByteBuf rest = queue.poll();
        if (rest != null) {
            rest.skipBytes(rest.readableBytes());
        }

        assertThat(queue.queuedBytes()).isZero();
    }

    @Test
    void clearReleasesWithoutCountingDrops() {
        OutputQueue queue = new OutputQueue(1000);
        ByteBuf chunk = Unpooled.buffer(10).writeZero(10);
        queue.offer(chunk);

        queue.clear();

        assertThat(chunk.refCnt()).isZero();
        assertThat(queue.dropped()).isZero();
        assertThat(queue.poll()).isNull();
    }

    @Test
    void aSmallerCapacityAppliesToTheNextOffer() {
        OutputQueue queue = new OutputQueue(1000);
        for (int i = 0; i < 5; i++) {
            queue.offer(Unpooled.buffer(100).writeZero(100));
        }

        queue.capacity(200);
        queue.offer(Unpooled.buffer(100).writeZero(100));

        assertThat(queue.queuedBytes()).isEqualTo(200);
        assertThat(queue.dropped()).isEqualTo(4);
        queue.clear();
    }

    /** How far behind: drops since the queue last ran empty, starting again once it has. (#10) */
    @Test
    void tracksHowFarBehindUntilTheQueueRunsEmpty() {
        OutputQueue queue = new OutputQueue(200);
        assertThat(queue.behindDroppedBytes()).isZero();
        for (int i = 0; i < 5; i++) {
            queue.offer(Unpooled.buffer(100).writeZero(100));
        }
        long since = queue.behindSinceNanos();
        assertThat(queue.behindDroppedBytes()).isEqualTo(300);
        assertThat(since).isPositive();

        queue.offer(Unpooled.buffer(100).writeZero(100));
        assertThat(queue.behindDroppedBytes()).as("still behind: it never ran empty").isEqualTo(400);
        assertThat(queue.behindSinceNanos()).isEqualTo(since);

        queue.poll().release();
        queue.poll().release();
        assertThat(queue.poll()).as("ran empty").isNull();
        for (int i = 0; i < 3; i++) {
            queue.offer(Unpooled.buffer(100).writeZero(100));
        }
        assertThat(queue.behindDroppedBytes()).as("a new run of drops").isEqualTo(100);
        assertThat(queue.behindSinceNanos()).isGreaterThan(since);
        queue.clear();
    }
}