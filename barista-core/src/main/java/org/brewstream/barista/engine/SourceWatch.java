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

import org.brewstream.barista.BrewKeyframe;
import org.brewstream.barista.SpliceMarker;
import org.brewstream.grind.ElementaryStream;
import org.brewstream.grind.Keyframe;
import org.brewstream.grind.KeyframeExtractor;
import org.brewstream.grind.ProgramMap;
import org.brewstream.grind.StreamType;
import org.brewstream.grind.TsPacket;
import org.brewstream.grind.scte.SpliceMonitor;

import java.util.List;

/**
 * What a source reads beyond TS health: SCTE-35 markers and, on demand, keyframes.
 * Both follow the program map, which Grind announces only when it changes.
 *
 * <p><b>SCTE-35.</b> Grind's {@link SpliceMonitor} sees only splice and clock
 * packets; a stream whose PMT declares no splice PID never reaches it.
 *
 * <p><b>Keyframes, demand-gated.</b> Grind's {@link KeyframeExtractor} runs only
 * while someone has asked for a keyframe within the demand window; each ask extends
 * it. Outside the window the extractor is disabled, which costs nothing per packet.
 *
 * <p>Fed by the leg's thread. {@link #demand()} and the getters are safe from any thread.
 */
final class SourceWatch {

    private final SpliceMonitor splice = new SpliceMonitor();
    private final SpliceMarkers markers = new SpliceMarkers();
    private final KeyframeExtractor keyframes = new KeyframeExtractor();
    private final long demandWindowNanos;
    private int[] splicePids = new int[0];
    private long keyframesSeen;
    private volatile boolean carriesScte35;
    private volatile String videoCodec;
    private volatile long demandUntilNanos;
    private volatile boolean demanded;
    private volatile BrewKeyframe keyframe;

    SourceWatch(long demandWindowNanos) {
        this.demandWindowNanos = demandWindowNanos;
        splice.addListener(markers::add);
    }

    void programsChanged(ProgramMap programs) {
        splice.programs(programs);
        splicePids = splice.splicePids().stream().mapToInt(Integer::intValue).toArray();
        carriesScte35 = splicePids.length > 0;
        keyframes.programsChanged(programs);
        videoCodec = programs.allStreams().stream()
                .filter(stream -> stream.streamType().kind() == StreamType.Kind.VIDEO)
                .map(ElementaryStream::streamType)
                .map(StreamType::description)
                .findFirst()
                .orElse(null);
    }

    /** Before a chunk: turns extraction on or off for the demand window. */
    void beforeChunk(long nowNanos) {
        boolean wanted = demanded && demandUntilNanos - nowNanos > 0;
        if (wanted != keyframes.isEnabled()) {
            if (wanted) {
                keyframes.enable();
            } else {
                keyframes.disable();
            }
        }
    }

    void consume(TsPacket packet) {
        keyframes.consume(packet);
        if (splicePids.length > 0 && (packet.pcr() >= 0 || isSplicePid(packet.pid()))) {
            splice.consume(packet);
        }
    }

    /** After a chunk: publishes a keyframe taken during it. */
    void afterChunk() {
        long taken = keyframes.keyframesTaken();
        if (taken != keyframesSeen) {
            keyframesSeen = taken;
            Keyframe latest = keyframes.latest().orElseThrow();
            keyframe = new BrewKeyframe(latest.codec(), System.currentTimeMillis(), latest.data());
        }
    }

    /** Asks for keyframes for the next demand window. */
    void demand() {
        demandUntilNanos = System.nanoTime() + demandWindowNanos;
        demanded = true;
    }

    /** The latest keyframe, or {@code null} before one is captured. */
    BrewKeyframe keyframe() {
        return keyframe;
    }

    boolean extracting() {
        return keyframes.isEnabled();
    }

    boolean carriesScte35() {
        return carriesScte35;
    }

    /** The first video track's codec, from the program map, or {@code null} when it has none. */
    String videoCodec() {
        return videoCodec;
    }

    List<SpliceMarker> spliceMarkers() {
        return markers.snapshot();
    }

    private boolean isSplicePid(int pid) {
        for (int splicePid : splicePids) {
            if (splicePid == pid) {
                return true;
            }
        }
        return false;
    }
}
