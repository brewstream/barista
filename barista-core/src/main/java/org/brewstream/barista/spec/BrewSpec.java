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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        checkSharedPorts(listeners(sources, outputs));
    }

    /** A brew that switches sources only when an operator says so. */
    public BrewSpec(BrewId id, String name, List<SourceSpec> sources, List<OutputSpec> outputs, boolean enabled) {
        this(id, name, sources, outputs, enabled, FailoverPolicy.off());
    }

    /** A new, enabled brew with a random id. */
    public static BrewSpec of(String name, List<SourceSpec> sources, List<OutputSpec> outputs) {
        return new BrewSpec(BrewId.random(), name, sources, outputs, true);
    }

    /**
     * Fixed SRT ports that more than one of this brew's SRT listener endpoints name.
     * They share one listener, which routes each connection to its endpoint by the
     * exact stream ID it asks for.
     */
    public Set<Integer> sharedSrtPorts() {
        Set<Integer> shared = new HashSet<>();
        Set<Integer> seen = new HashSet<>();
        for (Map.Entry<String, SrtListenerEndpoint> listener : listeners(sources, outputs).entrySet()) {
            int port = listener.getValue().port();
            if (port != 0 && !seen.add(port)) {
                shared.add(port);
            }
        }
        return shared;
    }

    /** Every SRT listener endpoint, by "source x" or "output y", in spec order. */
    private static Map<String, SrtListenerEndpoint> listeners(List<SourceSpec> sources, List<OutputSpec> outputs) {
        Map<String, SrtListenerEndpoint> listeners = new LinkedHashMap<>();
        for (SourceSpec source : sources) {
            if (source.endpoint() instanceof SrtListenerEndpoint srt) {
                listeners.put("source " + source.id(), srt);
            }
        }
        for (OutputSpec output : outputs) {
            if (output.endpoint() instanceof SrtListenerEndpoint srt) {
                listeners.put("output " + output.id(), srt);
            }
        }
        return listeners;
    }

    /**
     * Endpoints sharing a port must each name their own stream ID, since that is all
     * the listener can route on, and the same latency, since the listener has one.
     */
    private static void checkSharedPorts(Map<String, SrtListenerEndpoint> listeners) {
        Map<Integer, Map<String, SrtListenerEndpoint>> byPort = new HashMap<>();
        listeners.forEach((name, endpoint) -> {
            if (endpoint.port() != 0) {
                byPort.computeIfAbsent(endpoint.port(), port -> new LinkedHashMap<>()).put(name, endpoint);
            }
        });
        for (Map.Entry<Integer, Map<String, SrtListenerEndpoint>> port : byPort.entrySet()) {
            Map<String, SrtListenerEndpoint> sharing = port.getValue();
            if (sharing.size() < 2) {
                continue;
            }
            String names = String.join(", ", sharing.keySet());
            Set<String> streamIds = new HashSet<>();
            for (SrtListenerEndpoint endpoint : sharing.values()) {
                String streamId = endpoint.streamId();
                if (streamId == null || streamId.isEmpty()) {
                    throw new IllegalArgumentException("SRT port " + port.getKey() + " is shared by " + names
                            + ": each needs its own stream ID");
                }
                if (!streamIds.add(streamId)) {
                    throw new IllegalArgumentException("SRT port " + port.getKey() + " is shared by " + names
                            + ": stream ID '" + streamId + "' is used twice");
                }
            }
            if (sharing.values().stream().map(SrtListenerEndpoint::latency).distinct().count() > 1) {
                throw new IllegalArgumentException("SRT port " + port.getKey() + " is shared by " + names
                        + ": they need the same latency");
            }
        }
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