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

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * What a brew should be: its sources (one or more, one active at a time) and
 * its outputs (any number). Immutable; this is what a {@code BrewRepository}
 * stores, and the running brew is rebuilt from it after a restart.
 *
 * @param enabled  whether the brew should be running. A stopped brew keeps its spec
 *                 and its ports.
 * @param failover whether and how the brew switches sources by itself; {@code null} means
 *                 {@link FailoverPolicy#off()}
 */
public record BrewSpec(BrewId id, String name, List<SourceSpec> sources, List<OutputSpec> outputs,
        boolean enabled, FailoverPolicy failover) {

    public BrewSpec {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        sources = List.copyOf(sources);
        outputs = List.copyOf(outputs);
        failover = failover != null ? failover : FailoverPolicy.off();
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("a brew needs at least one source");
        }
        Set<String> ids = new HashSet<>();
        for (SourceSpec source : sources) {
            if (!ids.add("source:" + source.id().value())) {
                throw new IllegalArgumentException("duplicate source id " + source.id());
            }
        }
        for (OutputSpec output : outputs) {
            if (!ids.add("output:" + output.id().value())) {
                throw new IllegalArgumentException("duplicate output id " + output.id());
            }
        }
    }

    /** A brew that switches sources only when an operator says so. */
    public BrewSpec(BrewId id, String name, List<SourceSpec> sources, List<OutputSpec> outputs, boolean enabled) {
        this(id, name, sources, outputs, enabled, FailoverPolicy.off());
    }

    /** A new, enabled brew with a random id. */
    public static BrewSpec of(String name, List<SourceSpec> sources, List<OutputSpec> outputs) {
        return new BrewSpec(BrewId.random(), name, sources, outputs, true);
    }

    /** The source preferred when the brew starts: the lowest priority, first on a tie. */
    public SourceSpec preferredSource() {
        return sources.stream().min(Comparator.comparingInt(SourceSpec::priority)).orElseThrow();
    }

    public BrewSpec withSources(List<SourceSpec> sources) {
        return new BrewSpec(id, name, sources, outputs, enabled, failover);
    }

    public BrewSpec withOutputs(List<OutputSpec> outputs) {
        return new BrewSpec(id, name, sources, outputs, enabled, failover);
    }

    public BrewSpec withEnabled(boolean enabled) {
        return new BrewSpec(id, name, sources, outputs, enabled, failover);
    }

    public BrewSpec withFailover(FailoverPolicy failover) {
        return new BrewSpec(id, name, sources, outputs, enabled, failover);
    }
}