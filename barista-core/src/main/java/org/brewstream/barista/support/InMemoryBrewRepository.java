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

package org.brewstream.barista.support;

import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spi.BrewRepository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps specs in memory: brews do not survive the process. The default, so that
 * no storage technology is chosen for anyone; an application that wants brews
 * to survive a restart provides its own {@link BrewRepository}.
 */
public final class InMemoryBrewRepository implements BrewRepository {

    private final Map<String, Map<BrewId, BrewSpec>> byNode = new ConcurrentHashMap<>();

    @Override
    public List<BrewSpec> load(String nodeId) {
        Map<BrewId, BrewSpec> specs = byNode.get(nodeId);
        if (specs == null) {
            return List.of();
        }
        synchronized (specs) {
            return new ArrayList<>(specs.values());
        }
    }

    @Override
    public void save(String nodeId, BrewSpec spec) {
        Map<BrewId, BrewSpec> specs = byNode.computeIfAbsent(nodeId, id -> new LinkedHashMap<>());
        synchronized (specs) {
            specs.put(spec.id(), spec);
        }
    }

    @Override
    public void delete(String nodeId, BrewId id) {
        Map<BrewId, BrewSpec> specs = byNode.get(nodeId);
        if (specs != null) {
            synchronized (specs) {
                specs.remove(id);
            }
        }
    }
}