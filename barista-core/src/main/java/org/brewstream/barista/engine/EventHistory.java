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

import org.brewstream.barista.EndpointEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * One leg's last {@link #CAPACITY} events, oldest first. An event of the same
 * kind and reason as the last one is counted into it instead of added, so a
 * peer failing every few seconds is one entry, not the whole history.
 * Written from several threads (event loops, the dialer, management).
 */
final class EventHistory {

    static final int CAPACITY = 50;

    private final Deque<EndpointEvent> events = new ArrayDeque<>();

    /** Records an event and returns it as stored: new, or the previous one counted up. */
    synchronized EndpointEvent add(EndpointEvent.Kind kind, String reason) {
        long now = System.currentTimeMillis();
        EndpointEvent last = events.peekLast();
        EndpointEvent stored;
        if (last != null && last.kind() == kind && last.reason().equals(reason)) {
            events.pollLast();
            stored = new EndpointEvent(kind, reason, last.firstMillis(), now, last.count() + 1);
        } else {
            stored = new EndpointEvent(kind, reason, now, now, 1);
            if (events.size() == CAPACITY) {
                events.pollFirst();
            }
        }
        events.addLast(stored);
        return stored;
    }

    synchronized List<EndpointEvent> snapshot() {
        return List.copyOf(events);
    }
}