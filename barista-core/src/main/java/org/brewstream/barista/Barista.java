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
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.SourceId;

import java.util.Collection;
import java.util.Optional;

/**
 * The relay engine of one Barista node: creates, changes and removes brews.
 *
 * <p>Every method returns once the change is made: ports are bound or
 * released, connections started or closed. Changes are carried out one at a
 * time on a management thread, so a slow socket operation on one brew delays
 * other changes but never the data path of any brew.
 *
 * <p><b>Call from any thread except Barista's own event loops</b>, which a change
 * may need while the caller waits; such a call fails at once with
 * {@link IllegalStateException}. Listener callbacks run on a separate thread,
 * so calling back from a {@link BrewListener} is fine.
 */
public interface Barista extends AutoCloseable {

    /** This node: id, published host, and how many brews it runs. */
    NodeInfo node();

    /**
     * Creates and, if the spec is enabled, starts a brew. Ports of 0 on
     * listening endpoints are allocated first, so the returned brew already
     * carries its published addresses, and its stored spec holds the ports.
     *
     * @throws IllegalArgumentException if a brew with this id exists, or a fixed port is taken
     */
    Brew create(BrewSpec spec);

    Optional<Brew> brew(BrewId id);

    Collection<Brew> brews();

    /**
     * Changes a running brew in place. Outputs and sources that were added or
     * removed are started or stopped; one whose endpoint changed is restarted;
     * everything else keeps running untouched.
     */
    Brew update(BrewSpec spec);

    /** Stops a brew, keeping its spec and its ports. */
    void stop(BrewId id);

    /** Starts a stopped brew on its stored ports. */
    void start(BrewId id);

    /** Switches which source feeds the outputs. Every source stays connected. */
    void activate(BrewId id, SourceId source);

    /** Stops a brew, releases its ports and forgets its spec. */
    void delete(BrewId id);

    void addListener(BrewListener listener);

    /** Stops every brew, leaving their specs stored as they are. */
    @Override
    void close();
}