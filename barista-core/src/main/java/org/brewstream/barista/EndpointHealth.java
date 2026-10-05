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

/**
 * One word for how a source or output is doing, so every UI and alert built on
 * Barista agrees on what "degraded" means. Barista decides it in one place, with
 * thresholds from its settings.
 */
public enum EndpointHealth {
    /** Connected and, for a source, delivering, with nothing in the last window to say otherwise. */
    GOOD,
    /**
     * Connected but losing data over the last window: transport loss over the threshold,
     * continuity errors in the TS on this leg, or, for an output, chunks dropped from its queue.
     */
    DEGRADED,
    /** Not connected, or a source that is connected but not delivering (lost, or never started). */
    DOWN
}
