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

import java.util.List;

/**
 * A brew as of one moment. Comparing a source's health with each output's shows
 * where a problem starts: on the way in, or only on one way out.
 *
 * @param inputBitsPerSecond the active source's rate over the last second
 * @param error              why the brew failed, or {@code null}
 */
public record BrewStatus(
        BrewId id,
        String name,
        BrewState state,
        SourceId activeSource,
        long inputBitsPerSecond,
        List<EndpointStatus> sources,
        List<EndpointStatus> outputs,
        String error) {

    public BrewStatus {
        sources = List.copyOf(sources);
        outputs = List.copyOf(outputs);
    }
}