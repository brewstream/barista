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

/**
 * An inclusive range of ports, written {@code 9000-9099}, or a single port
 * written {@code 9000}.
 */
public record PortRange(int first, int last) {

    public PortRange {
        if (first < 1 || last > 65535 || first > last) {
            throw new IllegalArgumentException("not a port range: " + first + "-" + last);
        }
    }

    /**
     * Parses {@code first-last} or a single port. Named {@code valueOf} so
     * configuration binders, Spring Boot's among them, convert strings with it.
     */
    public static PortRange valueOf(String text) {
        String[] bounds = text.strip().split("\\s*-\\s*", -1); // keep empty parts: "9000-" is not 9000
        try {
            if (bounds.length == 1) {
                int port = Integer.parseInt(bounds[0]);
                return new PortRange(port, port);
            }
            if (bounds.length == 2) {
                return new PortRange(Integer.parseInt(bounds[0]), Integer.parseInt(bounds[1]));
            }
        } catch (NumberFormatException e) {
            // fall through to the message below
        }
        throw new IllegalArgumentException("a port range is written 9000-9099 or 9000, not '" + text + "'");
    }

    public boolean contains(int port) {
        return port >= first && port <= last;
    }

    public boolean overlaps(PortRange other) {
        return first <= other.last && other.first <= last;
    }

    @Override
    public String toString() {
        return first == last ? Integer.toString(first) : first + "-" + last;
    }
}