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

package org.brewstream.barista.example;

import org.brewstream.barista.Barista;
import org.brewstream.barista.spec.BrewId;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Creates the configured brews once the application is ready. By then Barista has
 * restored the brews its repository holds, so a configured brew whose id is already
 * there is left as it is: changes made at runtime survive a restart. A brew that
 * cannot be created is logged and the others go ahead.
 */
public class ConfiguredBrews {

    private static final Logger LOG = Logger.getLogger(ConfiguredBrews.class.getName());

    private final Barista barista;
    private final RelayProperties relay;

    public ConfiguredBrews(Barista barista, RelayProperties relay) {
        this.barista = barista;
        this.relay = relay;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createConfiguredBrews() {
        for (RelayProperties.ConfiguredBrew configured : relay.brews()) {
            try {
                if (barista.brew(new BrewId(configured.id())).isPresent()) {
                    LOG.info("brew " + configured.id() + " restored from the repository; configuration not applied");
                    continue;
                }
                barista.create(configured.toSpec());
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "brew " + configured.id() + " not created: " + e.getMessage(), e);
            }
        }
    }
}
