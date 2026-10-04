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
import io.netty.buffer.ByteBufAllocator;

import java.util.function.Consumer;

/**
 * Turns whatever a source delivers into chunks of whole 188-byte TS packets, at
 * most seven (1316 bytes) each, so everything after a source works on aligned
 * data: taps parse packets directly, and outputs never split a packet across
 * two writes.
 *
 * <p>Press delivers RFC 2250 payloads, already whole packets, and SRT senders
 * conventionally write 1316 bytes, so the common case passes through as slices
 * of the original buffer with no copy. Anything else is gathered and cut. On
 * losing alignment, it looks for {@code 0x47} with another {@code 0x47} one
 * packet later before trusting it (the same rule as Grind's
 * {@code MpegTsDecoder}: a lone {@code 0x47} inside compressed video is an
 * ordinary byte), and counts what it throws away.
 *
 * <p>Not thread-safe: one per source, on the brew's event loop.
 */
final class TsAligner {

    static final int PACKET = 188;
    static final int MAX_PACKETS = 7;
    /** More than this with no alignment found is noise; drop it rather than grow. */
    private static final int MAX_CARRY = 64 * 1024;

    private final ByteBufAllocator allocator;
    private ByteBuf carry;
    private long discardedBytes;

    TsAligner(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    /** Takes ownership of {@code in}; each chunk handed to {@code out} belongs to {@code out}. */
    void feed(ByteBuf in, Consumer<ByteBuf> out) {
        try {
            if ((carry == null || !carry.isReadable()) && startsAligned(in)) {
                while (in.readableBytes() >= PACKET) {
                    int packets = Math.min(in.readableBytes() / PACKET, MAX_PACKETS);
                    out.accept(in.readRetainedSlice(packets * PACKET));
                }
                if (in.isReadable()) {
                    carry().writeBytes(in); // a packet split across deliveries
                }
                return;
            }
            carry().writeBytes(in);
            drainCarry(out);
        } finally {
            in.release();
        }
    }

    /** Bytes thrown away because they were not part of any TS packet. */
    long discardedBytes() {
        return discardedBytes;
    }

    void release() {
        if (carry != null) {
            carry.release();
            carry = null;
        }
    }

    private boolean startsAligned(ByteBuf in) {
        int start = in.readerIndex();
        int whole = in.readableBytes() / PACKET;
        for (int i = 0; i < whole; i++) {
            if (in.getByte(start + i * PACKET) != 0x47) {
                return false;
            }
        }
        return whole > 0;
    }

    private void drainCarry(Consumer<ByteBuf> out) {
        while (carry.readableBytes() >= PACKET) {
            int start = carry.readerIndex();
            if (carry.getByte(start) != 0x47) {
                int sync = findSync(start + 1, carry.writerIndex());
                if (sync < 0) {
                    discardedBytes += carry.readableBytes();
                    carry.clear();
                    return;
                }
                discardedBytes += sync - start;
                carry.readerIndex(sync);
                continue;
            }
            int packets = 1;
            while (packets < MAX_PACKETS && carry.readableBytes() >= (packets + 1) * PACKET
                    && carry.getByte(start + packets * PACKET) == 0x47) {
                packets++;
            }
            ByteBuf chunk = allocator.buffer(packets * PACKET);
            carry.readBytes(chunk, packets * PACKET);
            out.accept(chunk);
        }
        carry.discardReadBytes();
        if (carry.readableBytes() > MAX_CARRY) {
            discardedBytes += carry.readableBytes();
            carry.clear();
        }
    }

    /** A 0x47 confirmed by another one packet later, or one too near the end to confirm yet. */
    private int findSync(int from, int end) {
        for (int i = from; i < end; i++) {
            if (carry.getByte(i) == 0x47 && (i + PACKET >= end || carry.getByte(i + PACKET) == 0x47)) {
                return i;
            }
        }
        return -1;
    }

    private ByteBuf carry() {
        if (carry == null) {
            carry = allocator.buffer(PACKET * MAX_PACKETS * 2);
        }
        return carry;
    }
}