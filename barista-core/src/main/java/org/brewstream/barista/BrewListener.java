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

package org.brewstream.barista;

import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.SourceId;

/**
 * Events from the engine. Every method has an empty default. Called on a
 * dedicated notification thread, never on a network thread, so a listener may
 * take its time or call back into {@link Barista}. An exception is logged and
 * does not affect the brew.
 */
public interface BrewListener {

    default void onBrewStateChanged(BrewId brew, BrewState from, BrewState to) {
    }

    /** A source or output changed state. {@code endpointId} is the source or output id. */
    default void onEndpointStateChanged(BrewId brew, String endpointId, EndpointState from, EndpointState to) {
    }

    default void onSourceActivated(BrewId brew, SourceId source) {
    }

    /**
     * Something happened to a source or output, with the reason in plain words.
     * The same events make up {@link EndpointStatus#history()}; a repeat of the
     * previous event is reported again here, with its count.
     */
    default void onEndpointEvent(BrewId brew, String endpointId, EndpointEvent event) {
    }
}