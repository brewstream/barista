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

import org.brewstream.barista.SpliceMarker;
import org.brewstream.grind.scte.SpliceEvent;
import org.brewstream.grind.scte.SpliceInfoSection;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * One source's last {@link #CAPACITY} splice markers, oldest first. A section equal
 * to one already held (same PID, same contents) counts into it rather than adding
 * a marker: redundant copies interleave with other cues, so this is not limited to
 * the newest. Written by the leg's thread, read by any.
 */
final class SpliceMarkers {

    static final int CAPACITY = 20;

    private final Deque<Entry> entries = new ArrayDeque<>();

    synchronized void add(SpliceEvent event) {
        long now = System.currentTimeMillis();
        for (Entry entry : entries) {
            if (entry.pid == event.pid() && entry.section.equals(event.section())) {
                entry.lastMillis = now;
                entry.count++;
                return;
            }
        }
        if (entries.size() == CAPACITY) {
            entries.pollFirst();
        }
        entries.addLast(new Entry(event, now));
    }

    synchronized List<SpliceMarker> snapshot() {
        return entries.stream().map(Entry::marker).toList();
    }

    private static final class Entry {

        final int pid;
        final SpliceInfoSection section;
        final String description;
        final long firstMillis;
        final double arrivalSeconds;
        final double preRollSeconds;
        long lastMillis;
        int count = 1;

        Entry(SpliceEvent event, long now) {
            this.pid = event.pid();
            this.section = Objects.requireNonNull(event.section());
            this.description = event.describe();
            this.firstMillis = now;
            this.lastMillis = now;
            this.arrivalSeconds = event.arrivalSeconds();
            this.preRollSeconds = event.preRollSeconds();
        }

        SpliceMarker marker() {
            return new SpliceMarker(pid, section.commandType().label(), description, firstMillis, lastMillis,
                    arrivalSeconds, section.spliceTimeSeconds(), preRollSeconds, count);
        }
    }
}
