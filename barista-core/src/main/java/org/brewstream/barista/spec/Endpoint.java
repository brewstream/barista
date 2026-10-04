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

package org.brewstream.barista.spec;

/**
 * Where one leg of a brew meets the network. A sealed type per protocol and
 * role, so a combination that cannot exist cannot be written down: RTP has no
 * caller source (an RTP source is always something sending to us) and no
 * listener output (an RTP output is always Barista sending somewhere).
 *
 * <p>A port of 0 on a listening endpoint means "allocate one": the engine picks
 * it when the brew is created, and from then on it is part of the stored spec.
 */
public sealed interface Endpoint permits SourceEndpoint, OutputEndpoint {

    /** Whether the far side dials Barista, so this endpoint's address is a contract. */
    boolean listens();
}