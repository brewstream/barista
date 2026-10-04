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

import org.brewstream.barista.Barista;
import org.brewstream.barista.BrewState;
import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.barista.spi.BrewRepository;
import org.brewstream.barista.spi.NodeIdentity;
import org.brewstream.barista.spi.PortAllocator;
import org.brewstream.barista.support.InMemoryBrewRepository;
import org.brewstream.barista.support.RangePortAllocator;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BaristaAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(BaristaAutoConfiguration.class))
            .withPropertyValues("barista.node.id=node-a", "barista.node.published-host=203.0.113.7",
                    "barista.event-loop-threads=2");

    @Test
    void suppliesTheEngineAndItsDefaults() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(Barista.class);
            assertThat(context).hasSingleBean(BrewRepository.class);
            assertThat(context.getBean(BrewRepository.class)).isInstanceOf(InMemoryBrewRepository.class);
            assertThat(context).hasSingleBean(PortAllocator.class);
            assertThat(context.getBean(NodeIdentity.class).nodeId()).isEqualTo("node-a");
            assertThat(context.getBean(Barista.class).node().publishedHost()).isEqualTo("203.0.113.7");
        });
    }

    /** The point of composable beans: an application's own repository replaces the default. */
    @Test
    void anApplicationsOwnBeansReplaceTheDefaults() {
        runner.withUserConfiguration(OwnRepository.class).run(context -> {
            assertThat(context).hasSingleBean(BrewRepository.class);
            assertThat(context.getBean(BrewRepository.class)).isSameAs(OwnRepository.REPOSITORY);
        });
    }

    /** Brews stored for this node come back when the context starts, on their stored ports. */
    @Test
    void restoresStoredBrewsWhenTheContextStarts() throws IOException {
        int port = freePort();
        BrewSpec stored = new BrewSpec(new BrewId("stored"), "stored",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any().withPort(port))), List.of(), true);
        OwnRepository.REPOSITORY.save("node-a", stored);

        runner.withUserConfiguration(OwnRepository.class).run(context -> {
            Barista barista = context.getBean(Barista.class);
            assertThat(barista.brew(new BrewId("stored"))).hasValueSatisfying(brew -> {
                assertThat(brew.state()).isIn(BrewState.STARTING, BrewState.SOURCE_LOST);
                assertThat(((SrtListenerEndpoint) brew.spec().sources().getFirst().endpoint()).port())
                        .isEqualTo(port);
            });
        });
        OwnRepository.REPOSITORY.delete("node-a", new BrewId("stored"));
    }

    /** An application's own, unrelated EventLoopGroup must not confuse Barista's. (From the Codex review.) */
    @Test
    void coexistsWithAnApplicationsOwnEventLoopGroup() throws Exception {
        io.netty.channel.EventLoopGroup unrelated = new io.netty.channel.MultiThreadIoEventLoopGroup(1,
                io.netty.channel.nio.NioIoHandler.newFactory());
        try {
            runner.withBean("applicationEventLoopGroup", io.netty.channel.EventLoopGroup.class, () -> unrelated)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        assertThat(context).hasSingleBean(Barista.class);
                    });
        } finally {
            unrelated.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS).sync();
        }
    }

    @Test
    void bindsPortRanges() {
        runner.withPropertyValues("barista.ports.srt=7000-7001", "barista.ports.rtp=6000-6009").run(context -> {
            PortAllocator ports = context.getBean(PortAllocator.class);
            assertThat(ports).isInstanceOf(RangePortAllocator.class);
            assertThat(ports.allocate(org.brewstream.barista.spi.PortKind.SRT)).isEqualTo(7000);
            assertThat(ports.allocate(org.brewstream.barista.spi.PortKind.RTP_BLOCK)).isEqualTo(6000);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnRepository {
        static final InMemoryBrewRepository REPOSITORY = new InMemoryBrewRepository();

        @Bean
        BrewRepository myRepository() {
            return REPOSITORY;
        }
    }

    private static int freePort() throws IOException {
        try (DatagramSocket probe = new DatagramSocket(0)) {
            return probe.getLocalPort();
        }
    }
}