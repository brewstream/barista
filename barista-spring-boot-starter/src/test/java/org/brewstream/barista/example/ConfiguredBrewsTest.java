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
import org.brewstream.barista.Brew;
import org.brewstream.barista.autoconfigure.BaristaAutoConfiguration;
import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.barista.spi.BrewRepository;
import org.brewstream.barista.support.InMemoryBrewRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.DatagramSocket;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Keeps the README's example honest: it is this code, run against the real starter. */
class ConfiguredBrewsTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(BaristaAutoConfiguration.class))
            .withUserConfiguration(Application.class)
            .withPropertyValues("barista.node.id=node-a", "barista.event-loop-threads=2");

    @Test
    void createsTheConfiguredBrewsWhenTheApplicationIsReady() throws IOException {
        int port = freePort();
        runner.withPropertyValues(
                "relay.brews[0].id=studio-feed",
                "relay.brews[0].name=Studio feed",
                "relay.brews[0].srt-port=" + port,
                "relay.brews[0].outputs[0].id=playout",
                "relay.brews[0].outputs[0].host=127.0.0.1",
                "relay.brews[0].outputs[0].port=5000").run(context -> {
                    Barista barista = context.getBean(Barista.class);
                    assertThat(barista.brews()).isEmpty();

                    ready(context);

                    Brew brew = barista.brew(new BrewId("studio-feed")).orElseThrow();
                    assertThat(brew.spec().name()).isEqualTo("Studio feed");
                    assertThat(((SrtListenerEndpoint) brew.spec().sources().getFirst().endpoint()).port())
                            .isEqualTo(port);
                    assertThat(brew.spec().outputs()).singleElement()
                            .satisfies(output -> assertThat(output.id().value()).isEqualTo("playout"));
                });
    }

    /** A brew restored from the repository keeps what it was changed to at runtime. */
    @Test
    void leavesARestoredBrewOfTheSameIdAlone() throws IOException {
        int stored = freePort();
        Repository.REPOSITORY.save("node-a", new BrewSpec(new BrewId("studio-feed"), "renamed at runtime",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any().withPort(stored))), List.of(), true));
        try {
            runner.withUserConfiguration(Repository.class).withPropertyValues(
                    "relay.brews[0].id=studio-feed",
                    "relay.brews[0].name=Studio feed",
                    "relay.brews[0].srt-port=" + freePort()).run(context -> {
                        ready(context);

                        Brew brew = context.getBean(Barista.class).brew(new BrewId("studio-feed")).orElseThrow();
                        assertThat(brew.spec().name()).isEqualTo("renamed at runtime");
                    });
        } finally {
            Repository.REPOSITORY.delete("node-a", new BrewId("studio-feed"));
        }
    }

    /** One brew that cannot be created is logged; the application starts and the rest are created. */
    @Test
    void oneBadBrewDoesNotStopTheOthers() throws IOException {
        int port = freePort();
        runner.withPropertyValues(
                "relay.brews[0].id=first",
                "relay.brews[0].name=First",
                "relay.brews[0].srt-port=" + port,
                "relay.brews[1].id=clash",
                "relay.brews[1].name=Same port",
                "relay.brews[1].srt-port=" + port,
                "relay.brews[2].id=third",
                "relay.brews[2].name=Third",
                "relay.brews[2].srt-port=" + freePort()).run(context -> {
                    ready(context);

                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(Barista.class).brews()).extracting(brew -> brew.id().value())
                            .containsExactlyInAnyOrder("first", "third");
                });
    }

    private static void ready(AssertableApplicationContext context) {
        context.publishEvent(new ApplicationReadyEvent(new SpringApplication(), new String[0],
                context.getSourceApplicationContext(), Duration.ZERO));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(RelayProperties.class)
    static class Application {
        @Bean
        ConfiguredBrews configuredBrews(Barista barista, RelayProperties relay) {
            return new ConfiguredBrews(barista, relay);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Repository {
        static final InMemoryBrewRepository REPOSITORY = new InMemoryBrewRepository();

        @Bean
        BrewRepository repository() {
            return REPOSITORY;
        }
    }

    private static int freePort() throws IOException {
        try (DatagramSocket probe = new DatagramSocket(0)) {
            return probe.getLocalPort();
        }
    }
}
