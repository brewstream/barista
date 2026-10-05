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

/** Where a brew is in its life. */
public enum BrewState {
    /** Binding ports and starting connections. */
    STARTING,
    /** The active source is delivering. */
    RUNNING,
    /**
     * The active source has delivered nothing for the source-loss timeout.
     * Outputs stay up and send nothing until it returns or another source takes
     * over: by {@link Barista#activate}, or by the brew's failover policy.
     */
    SOURCE_LOST,
    /** Not running; spec and ports kept. */
    STOPPED,
    /** Could not start: a port in use, a bad endpoint. The reason is in the status. */
    FAILED
}