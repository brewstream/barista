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

import java.util.UUID;

/** Identifies a brew on its node, for as long as it exists, across restarts. */
public record BrewId(String value) {

    public BrewId {
        Ids.check("brew id", value);
    }

    public static BrewId random() {
        return new BrewId(UUID.randomUUID().toString());
    }

    @Override
    public String toString() {
        return value;
    }
}