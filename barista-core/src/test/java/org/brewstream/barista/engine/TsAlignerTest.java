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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.brewstream.barista.TsFixtures;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TsAlignerTest {

    private final TsAligner aligner = new TsAligner(PooledByteBufAllocator.DEFAULT);
    private final List<Integer> sizes = new ArrayList<>();
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @AfterEach
    void release() {
        aligner.release();
    }

    @Test
    void cutsAlignedInputIntoChunksOfAtMostSevenPackets() {
        byte[] ts = TsFixtures.packets(0, 16);

        feed(ts);

        assertThat(sizes).containsExactly(7 * 188, 7 * 188, 2 * 188);
        assertThat(out.toByteArray()).isEqualTo(ts);
        assertThat(aligner.discardedBytes()).isZero();
    }

    @Test
    void joinsPacketsSplitAcrossDeliveries() {
        byte[] ts = TsFixtures.packets(0, 10);

        for (int at = 0; at < ts.length; at += 100) {
            feed(java.util.Arrays.copyOfRange(ts, at, Math.min(ts.length, at + 100)));
        }

        assertThat(out.toByteArray()).isEqualTo(ts);
        assertThat(sizes).allMatch(size -> size % 188 == 0 && size <= 7 * 188);
    }

    @Test
    void regainsAlignmentAfterGarbageAndCountsWhatItDropped() {
        byte[] ts = TsFixtures.packets(0, 6);
        byte[] garbage = {1, 2, 3, 0x47, 5};
        byte[] input = new byte[garbage.length + ts.length];
        System.arraycopy(garbage, 0, input, 0, garbage.length);
        System.arraycopy(ts, 0, input, garbage.length, ts.length);

        feed(input);

        assertThat(out.toByteArray()).isEqualTo(ts);
        assertThat(aligner.discardedBytes()).isEqualTo(garbage.length);
    }

    /** A lone 0x47 is an ordinary byte; it is only trusted with another one a packet later. */
    @Test
    void doesNotLockOntoALoneSyncByte() {
        byte[] ts = TsFixtures.packets(0, 3);
        byte[] input = new byte[200 + ts.length];
        input[10] = 0x47; // not followed by 0x47 188 bytes later
        System.arraycopy(ts, 0, input, 200, ts.length);

        feed(input);

        assertThat(out.toByteArray()).isEqualTo(ts);
        assertThat(aligner.discardedBytes()).isEqualTo(200);
    }

    private void feed(byte[] bytes) {
        ByteBuf in = Unpooled.copiedBuffer(bytes);
        aligner.feed(in, chunk -> {
            sizes.add(chunk.readableBytes());
            out.writeBytes(ByteBufUtil.getBytes(chunk));
            chunk.release();
        });
        assertThat(in.refCnt()).isZero();
    }
}