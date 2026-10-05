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
import org.brewstream.barista.BrewKeyframe;
import org.brewstream.barista.SpliceMarker;
import org.brewstream.grind.TsAnalyzer;
import org.brewstream.grind.TsPacket;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * SCTE-35 on a source. {@code splice.ts} comes from Grind's fixtures (made with TSDuck):
 * five sections on PID 500, each sent twice.
 *
 * <p>Pre-roll is Grind's: splice time minus the program clock (PCR) when the cue arrived,
 * which for the first copy of event 1001 is 403,200 / 90 kHz minus the last PCR before
 * packet 264, 2.456 s, computed from the raw packets outside Grind. TSDuck's
 * {@code splicemonitor} reports 1,714 ms for the same cue because it counts from the
 * latest video PTS instead, which runs ahead of the PCR by the mux delay.
 */
class LegMonitorTest {

    private static final int CHUNK = 7 * 188;
    private static final long WINDOW = 30_000_000_000L;

    @Test
    void knowsTheVideoCodecBeforeAnyoneAsksForAKeyframe() throws IOException {
        SourceWatch watch = new SourceWatch(WINDOW);
        feed(new LegMonitor(watch), fixture("/keyframe-h264.ts"), CHUNK);

        assertThat(watch.videoCodec()).isEqualTo("H.264 / AVC");
        assertThat(watch.extracting()).isFalse();
        assertThat(watch.keyframe()).as("nobody asked").isNull();
    }

    @Test
    void capturesAKeyframeOnceAsked() throws IOException {
        for (String fixture : List.of("/keyframe-h264.ts", "/keyframe-hevc.ts")) {
            SourceWatch watch = new SourceWatch(WINDOW);
            watch.demand();
            feed(new LegMonitor(watch), fixture(fixture), CHUNK);

            BrewKeyframe keyframe = watch.keyframe();
            assertThat(keyframe).as(fixture).isNotNull();
            assertThat(keyframe.codec()).as(fixture).matches("avc1\\..*|hev1\\..*|hvc1\\..*");
            assertThat(keyframe.data()).as("Annex B").startsWith(0, 0, 0, 1);
            assertThat(keyframe.capturedAtMillis()).isPositive();
        }
    }

    @Test
    void stopsExtractingWhenTheDemandWindowLapses() throws Exception {
        byte[] ts = fixture("/keyframe-h264.ts");
        SourceWatch watch = new SourceWatch(50_000_000L);
        LegMonitor monitor = new LegMonitor(watch);
        watch.demand();
        feed(monitor, ts, CHUNK);
        BrewKeyframe first = watch.keyframe();
        assertThat(first).isNotNull();

        Thread.sleep(100);
        feed(monitor, ts, CHUNK);

        assertThat(watch.extracting()).isFalse();
        assertThat(watch.keyframe()).as("nothing new captured").isSameAs(first);
    }

    @Test
    void showsEachCueOnceWithItsCopiesCountedAndItsPreRoll() throws IOException {
        SourceWatch watch = new SourceWatch(WINDOW);
        LegMonitor monitor = new LegMonitor(watch);
        feed(monitor, fixture("/splice.ts"), CHUNK);

        List<SpliceMarker> markers = watch.spliceMarkers();
        assertThat(markers).as("five sections, each sent twice").hasSize(5)
                .allSatisfy(marker -> {
                    assertThat(marker.pid()).isEqualTo(500);
                    assertThat(marker.count()).isEqualTo(2);
                    assertThat(marker.lastMillis()).isGreaterThanOrEqualTo(marker.firstMillis());
                });
        SpliceMarker out = markers.getFirst();
        assertThat(out.command()).isEqualTo("splice_insert");
        assertThat(out.description()).isEqualTo("event 1001 out for 2.0s");
        assertThat(out.spliceSeconds()).isCloseTo(403_200 / 90_000.0, within(0.000_1));
        assertThat(out.preRollSeconds()).as("against the PCR").isCloseTo(2.456_186, within(0.000_01));
        assertThat(out.spliceSeconds() - out.arrivalSeconds()).isCloseTo(out.preRollSeconds(), within(0.000_1));
        assertThat(markers).extracting(SpliceMarker::command)
                .containsExactly("splice_insert", "time_signal", "splice_insert", "time_signal", "time_signal");
    }

    @Test
    void carriesScte35IsKnownBeforeTheFirstMarker() throws IOException {
        SourceWatch watch = new SourceWatch(WINDOW);
        LegMonitor monitor = new LegMonitor(watch);
        byte[] ts = fixture("/splice.ts");
        boolean declaredWithNoMarkerYet = false;
        for (int at = 0; at < ts.length && watch.spliceMarkers().isEmpty(); at += 188) {
            feed(monitor, ts, at, 188);
            declaredWithNoMarkerYet |= watch.carriesScte35() && watch.spliceMarkers().isEmpty();
        }

        assertThat(declaredWithNoMarkerYet).isTrue();
        assertThat(watch.spliceMarkers()).isNotEmpty();
    }

    @Test
    void aStreamWithoutScte35CarriesNone() throws IOException {
        SourceWatch watch = new SourceWatch(WINDOW);
        LegMonitor monitor = new LegMonitor(watch);
        feed(monitor, fixture("/sample.ts"), CHUNK);

        assertThat(watch.carriesScte35()).isFalse();
        assertThat(watch.spliceMarkers()).isEmpty();
    }

    /**
     * A source's extra reading costs nothing per packet on a stream that declares no splice
     * PID while nobody asks for keyframes ({@code sample.ts} carries H.264). Both monitors first see the whole stream, tables included; then they are fed only
     * its media packets, where the SCTE-35 path must allocate nothing the analyzer does
     * not. (A new PAT or PMT costs a little, once per table.)
     */
    @Test
    void addsNoAllocationPerPacketWithoutScte35() throws IOException {
        byte[] ts = fixture("/sample.ts");
        byte[] media = withoutTables(ts);
        LegMonitor with = new LegMonitor(new SourceWatch(WINDOW));
        LegMonitor without = new LegMonitor();
        feed(with, ts, CHUNK);
        feed(without, ts, CHUNK);
        for (int i = 0; i < 5; i++) { // warm both paths up
            allocatedFeeding(with, media);
            allocatedFeeding(without, media);
        }

        long extra = allocatedFeeding(with, media) - allocatedFeeding(without, media);

        assertThat(extra).as("bytes beyond the plain analyzer over %d media packets", media.length / 188)
                .isLessThan(1024);
    }

    /** The stream without its PAT and PMT packets. */
    private static byte[] withoutTables(byte[] ts) {
        TsAnalyzer analyzer = new TsAnalyzer();
        for (int at = 0; at < ts.length; at += 188) {
            analyzer.consume(TsPacket.parse(ts, at));
        }
        java.util.Set<Integer> tables = new java.util.HashSet<>(analyzer.programs().pmtPids().values());
        tables.add(TsPacket.PAT_PID);
        java.io.ByteArrayOutputStream media = new java.io.ByteArrayOutputStream();
        for (int at = 0; at < ts.length; at += 188) {
            if (!tables.contains(TsPacket.parse(ts, at).pid())) {
                media.write(ts, at, 188);
            }
        }
        return media.toByteArray();
    }

    private static long allocatedFeeding(LegMonitor monitor, byte[] ts) {
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long before = threads.getCurrentThreadAllocatedBytes();
        feed(monitor, ts, CHUNK);
        return threads.getCurrentThreadAllocatedBytes() - before;
    }

    private static void feed(LegMonitor monitor, byte[] ts, int chunk) {
        for (int at = 0; at < ts.length; at += chunk) {
            feed(monitor, ts, at, Math.min(chunk, ts.length - at));
        }
    }

    private static void feed(LegMonitor monitor, byte[] ts, int at, int length) {
        ByteBuf buffer = Unpooled.wrappedBuffer(ts, at, length);
        monitor.observe(buffer);
        buffer.release();
    }

    private static byte[] fixture(String name) throws IOException {
        try (InputStream in = LegMonitorTest.class.getResourceAsStream(name)) {
            assertThat(in).as(name).isNotNull();
            return in.readAllBytes();
        }
    }
}
