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

/**
 * Hands out local ports for listening endpoints. Only bookkeeping: binding is
 * the engine's, and a port that turns out to be in use fails its brew.
 *
 * <p>Called from Barista's management thread, one call at a time.
 */
public interface PortAllocator {

    /**
     * A port of the given kind that is not already handed out or reserved.
     *
     * @throws IllegalStateException when the range is exhausted
     */
    int allocate(PortKind kind);

    /**
     * Marks a specific port as taken: one written into a spec by hand, or one
     * stored with a spec being restored.
     *
     * @throws IllegalStateException when it is already taken
     */
    void reserve(PortKind kind, int port);

    /** Returns a port to the pool. */
    void release(PortKind kind, int port);
}