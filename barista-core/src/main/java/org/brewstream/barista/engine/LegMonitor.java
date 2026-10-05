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
import org.brewstream.barista.SpliceMarker;
import org.brewstream.grind.ProgramMap;
import org.brewstream.grind.TsAnalyzer;
import org.brewstream.grind.TsPacket;
import org.brewstream.grind.TsStreamStats;
import org.brewstream.grind.scte.SpliceMonitor;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Counts and analyses what passes one leg: a Grind {@link TsAnalyzer} over every
 * TS packet. Written by one thread (the leg's), read by any: counters are
 * volatile, and the analyzer's statistics are published as a snapshot at most
 * once a second, so readers never touch the analyzer itself.
 *
 * <p>For a source it also reads SCTE-35 with Grind's {@link SpliceMonitor}. The
 * monitor learns the splice PIDs from the program map, which is handed over only
 * when it changes, and sees only splice and clock packets; a stream whose PMT
 * declares no splice PID never reaches it.
 */
final class LegMonitor {

    private static final long PUBLISH_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final TsAnalyzer analyzer = new TsAnalyzer();
    private volatile long chunks;
    private volatile long bytes;
    private volatile TsStreamStats health;
    private long lastPublishNanos;
    private final SpliceMonitor splice;
    private final SpliceMarkers markers;
    private ProgramMap programs = ProgramMap.EMPTY;
    private int[] splicePids = new int[0];
    private volatile boolean carriesScte35;

    /** @param spliceMarkers whether to read SCTE-35: for a source, not an output */
    LegMonitor(boolean spliceMarkers) {
        if (spliceMarkers) {
            markers = new SpliceMarkers();
            splice = new SpliceMonitor();
            splice.addListener(markers::add);
        } else {
            markers = null;
            splice = null;
        }
    }

    /** Observes a chunk of whole TS packets. Does not take ownership. */
    void observe(ByteBuf chunk) {
        int length = chunk.readableBytes();
        byte[] data = new byte[length];
        chunk.getBytes(chunk.readerIndex(), data);
        for (int offset = 0; offset + TsAligner.PACKET <= length; offset += TsAligner.PACKET) {
            TsPacket packet = TsPacket.parse(data, offset);
            analyzer.consume(packet);
            if (splice != null) {
                watchSplices(packet);
            }
        }
        chunks = chunks + 1;
        bytes = bytes + length;
        long now = System.nanoTime();
        if (health == null || now - lastPublishNanos >= PUBLISH_NANOS) {
            health = analyzer.stats();
            lastPublishNanos = now;
        }
    }

    private void watchSplices(TsPacket packet) {
        ProgramMap current = analyzer.programs();
        if (current != programs) {
            programs = current;
            splice.programs(current);
            splicePids = splice.splicePids().stream().mapToInt(Integer::intValue).toArray();
            carriesScte35 = splicePids.length > 0;
        }
        if (splicePids.length > 0 && (packet.pcr() >= 0 || isSplicePid(packet.pid()))) {
            splice.consume(packet);
        }
    }

    private boolean isSplicePid(int pid) {
        for (int splicePid : splicePids) {
            if (splicePid == pid) {
                return true;
            }
        }
        return false;
    }

    long chunks() {
        return chunks;
    }

    long bytes() {
        return bytes;
    }

    /** Whether the program map declares a splice PID, whether or not a marker has arrived. */
    boolean carriesScte35() {
        return carriesScte35;
    }

    /** The recent splice markers, oldest first; empty for an output. */
    List<SpliceMarker> spliceMarkers() {
        return markers == null ? List.of() : markers.snapshot();
    }

    /** The last published statistics, or {@code null} before any data. */
    TsStreamStats tsStats() {
        return health;
    }
}