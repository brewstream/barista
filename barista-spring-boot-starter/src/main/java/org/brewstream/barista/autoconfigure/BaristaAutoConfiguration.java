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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.brewstream.barista.Barista;
import org.brewstream.barista.BrewListener;
import org.brewstream.barista.engine.BaristaSettings;
import org.brewstream.barista.engine.DefaultBarista;
import org.brewstream.barista.spi.BrewRepository;
import org.brewstream.barista.spi.NodeIdentity;
import org.brewstream.barista.spi.PortAllocator;
import org.brewstream.barista.support.InMemoryBrewRepository;
import org.brewstream.barista.support.RangePortAllocator;
import org.brewstream.barista.support.StaticNodeIdentity;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.logging.Logger;

/**
 * Barista as beans. Every bean here backs off when the application defines its
 * own of the same type, which is how persistence, port management and node
 * identity are replaced: define a {@link BrewRepository} bean (JPA, a file,
 * anything) and it is used instead of the in-memory default.
 *
 * <p>No web layer: the application decides how Barista is exposed.
 */
@AutoConfiguration
@EnableConfigurationProperties(BaristaProperties.class)
public class BaristaAutoConfiguration {

    private static final Logger LOG = Logger.getLogger(BaristaAutoConfiguration.class.getName());

    /**
     * One pool of threads for every SRT and RTP socket on the node. Barista
     * always injects it by this name, so an application's own, unrelated
     * {@code EventLoopGroup} beans do not get in the way, and defining a bean
     * named {@code baristaEventLoopGroup} replaces it.
     */
    @Bean(destroyMethod = "shutdownGracefully")
    @ConditionalOnMissingBean(name = "baristaEventLoopGroup")
    public EventLoopGroup baristaEventLoopGroup(BaristaProperties properties) {
        return new MultiThreadIoEventLoopGroup(properties.eventLoopThreads(), NioIoHandler.newFactory());
    }

    @Bean
    @ConditionalOnMissingBean
    public BrewRepository brewRepository() {
        LOG.warning("Barista is keeping brews in memory: they will not survive a restart. "
                + "Define a BrewRepository bean to persist them.");
        return new InMemoryBrewRepository();
    }

    @Bean
    @ConditionalOnMissingBean
    public PortAllocator brewPortAllocator(BaristaProperties properties) {
        return new RangePortAllocator(properties.ports().srt(), properties.ports().rtp());
    }

    @Bean
    @ConditionalOnMissingBean
    public NodeIdentity nodeIdentity(BaristaProperties properties) {
        return new StaticNodeIdentity(properties.node().id(), properties.node().publishedHost());
    }

    @Bean
    @ConditionalOnMissingBean
    public BaristaSettings baristaSettings(BaristaProperties properties) {
        BaristaSettings defaults = BaristaSettings.defaults();
        return new BaristaSettings(properties.queueTime(), defaults.minQueueBytes(), defaults.maxQueueBytes(),
                properties.sourceLossTimeout(), defaults.reconnectMin(), defaults.reconnectMax(),
                properties.health().window(), properties.health().degradedLossPercent(),
                properties.keyframeDemandWindow());
    }

    @Bean
    @ConditionalOnMissingBean(Barista.class)
    public DefaultBarista barista(BaristaSettings settings,
            @Qualifier("baristaEventLoopGroup") EventLoopGroup baristaEventLoopGroup,
            BrewRepository repository, PortAllocator ports, NodeIdentity identity,
            ObjectProvider<BrewListener> listeners) {
        DefaultBarista barista = new DefaultBarista(settings, baristaEventLoopGroup, NioDatagramChannel.class,
                repository, ports, identity);
        listeners.orderedStream().forEach(barista::addListener);
        return barista;
    }

    @Bean
    @ConditionalOnBean(DefaultBarista.class)
    public BaristaLifecycle baristaLifecycle(DefaultBarista barista) {
        return new BaristaLifecycle(barista);
    }
}