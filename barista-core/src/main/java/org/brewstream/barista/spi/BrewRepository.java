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

package org.brewstream.barista.spi;

import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;

import java.util.List;

/**
 * Stores brew specs: what should run, never the running state. The engine saves
 * a spec on every change and loads this node's specs at startup to recreate its
 * brews, so an implementation that persists is what makes brews survive a
 * restart. The default keeps them in memory only.
 *
 * <p>Specs are keyed by node id as well as brew id: a store shared by several
 * Barista nodes must give each node back only its own brews. Ports are part of
 * a spec and must come back unchanged, because they are a contract with whoever
 * dials them. Specs can hold SRT passphrases; protecting them at rest is the
 * implementation's job.
 *
 * <p>Called from Barista's management thread, one call at a time.
 */
public interface BrewRepository {

    /** Every brew stored for this node. */
    List<BrewSpec> load(String nodeId);

    /** Stores a brew, replacing any spec with the same id. */
    void save(String nodeId, BrewSpec spec);

    /** Forgets a brew. Does nothing if it is not stored. */
    void delete(String nodeId, BrewId id);
}