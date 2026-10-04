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

/** Where one source or output is. A brew can be running with one output reconnecting. */
public enum EndpointState {
    STARTING,
    /** A listening endpoint with nobody connected yet, or an RTP receiver that has heard nothing. */
    WAITING,
    /** A caller dialling. */
    CONNECTING,
    /** Connected, and for a source, delivering. */
    ACTIVE,
    /** A source that is connected but has delivered nothing for the source-loss timeout. */
    IDLE,
    /** A caller whose connection dropped, waiting to dial again. */
    RECONNECTING,
    STOPPED,
    FAILED
}