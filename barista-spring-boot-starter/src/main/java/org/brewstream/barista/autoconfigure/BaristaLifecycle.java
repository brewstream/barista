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

package org.brewstream.barista.autoconfigure;

import org.brewstream.barista.engine.DefaultBarista;
import org.springframework.context.SmartLifecycle;

/**
 * Restores the node's stored brews when the context starts, and stops them when
 * it closes, before the event loop group they run on is shut down.
 */
public class BaristaLifecycle implements SmartLifecycle {

    private final DefaultBarista barista;
    private volatile boolean running;

    public BaristaLifecycle(DefaultBarista barista) {
        this.barista = barista;
    }

    @Override
    public void start() {
        barista.start();
        running = true;
    }

    @Override
    public void stop() {
        barista.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}