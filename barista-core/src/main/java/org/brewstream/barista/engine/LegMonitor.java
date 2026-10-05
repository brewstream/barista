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
import org.brewstream.grind.ProgramMap;
import org.brewstream.grind.TsAnalyzer;
import org.brewstream.grind.TsPacket;
import org.brewstream.grind.TsStreamListener;
import org.brewstream.grind.TsStreamStats;

import java.util.concurrent.TimeUnit;

/**
 * Counts and analyses what passes one leg: a Grind {@link TsAnalyzer} over every
 * TS packet. Written by one thread (the leg's), read by any: counters are
 * volatile, and the analyzer's statistics are published as a snapshot at most
 * once a second, so readers never touch the analyzer itself. A source also has a
 * {@link SourceWatch} for SCTE-35 and keyframes.
 */
final class LegMonitor {

    private static final long PUBLISH_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final TsAnalyzer analyzer = new TsAnalyzer();
    private final SourceWatch watch;
    private volatile long chunks;
    private volatile long bytes;
    private volatile TsStreamStats health;
    private long lastPublishNanos;

    /** For an output. */
    LegMonitor() {
        this(null);
    }

    /** For a source when {@code watch} is not null. */
    LegMonitor(SourceWatch watch) {
        this.watch = watch;
        if (watch != null) {
            analyzer.addListener(new TsStreamListener() {
                @Override
                public void onProgramsChanged(ProgramMap programs) {
                    watch.programsChanged(programs);
                }
            });
        }
    }

    /** Observes a chunk of whole TS packets. Does not take ownership. */
    void observe(ByteBuf chunk) {
        int length = chunk.readableBytes();
        byte[] data = new byte[length];
        chunk.getBytes(chunk.readerIndex(), data);
        long now = System.nanoTime();
        if (watch != null) {
            watch.beforeChunk(now);
        }
        for (int offset = 0; offset + TsAligner.PACKET <= length; offset += TsAligner.PACKET) {
            TsPacket packet = TsPacket.parse(data, offset);
            analyzer.consume(packet);
            if (watch != null) {
                watch.consume(packet);
            }
        }
        if (watch != null) {
            watch.afterChunk();
        }
        chunks = chunks + 1;
        bytes = bytes + length;
        if (health == null || now - lastPublishNanos >= PUBLISH_NANOS) {
            health = analyzer.stats();
            lastPublishNanos = now;
        }
    }

    long chunks() {
        return chunks;
    }

    long bytes() {
        return bytes;
    }

    /** The last published statistics, or {@code null} before any data. */
    TsStreamStats tsStats() {
        return health;
    }
}
