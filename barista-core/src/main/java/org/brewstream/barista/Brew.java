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

/**
 * A brew as it runs: one or more sources, exactly one of them active, feeding
 * any number of outputs. Every source stays connected and monitored whether it
 * is active or not, so switching to a backup is immediate.
 */
public interface Brew {

    BrewId id();

    /** The spec it runs, with every allocated port filled in. */
    BrewSpec spec();

    BrewState state();

    /** The source currently feeding the outputs. */
    SourceId activeSource();

    /** A snapshot of every endpoint's state, traffic and stream health. */
    BrewStatus status();
}