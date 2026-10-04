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

package org.brewstream.barista.engine;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.brewstream.barista.Brew;
import org.brewstream.barista.BrewState;
import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.barista.spi.BrewRepository;
import org.brewstream.barista.spi.PortKind;
import org.brewstream.barista.support.InMemoryBrewRepository;
import org.brewstream.barista.support.PortRange;
import org.brewstream.barista.support.RangePortAllocator;
import org.brewstream.barista.support.StaticNodeIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Port bookkeeping stays right when the repository refuses a change, and a
 * brew only ever releases ports it actually holds. (From the Codex review.)
 */
class PortOwnershipTest {

    private EventLoopGroup group;
    private RangePortAllocator ports;
    private DefaultBarista barista;
    private int first;

    /** Saves normally until told to fail. */
    private static final class FlakyRepository implements BrewRepository {
        final InMemoryBrewRepository store = new InMemoryBrewRepository();
        final AtomicBoolean failing = new AtomicBoolean();

        @Override
        public List<BrewSpec> load(String nodeId) {
            return store.load(nodeId);
        }

        @Override
        public void save(String nodeId, BrewSpec spec) {
            if (failing.get()) {
                throw new IllegalStateException("storage unavailable");
            }
            store.save(nodeId, spec);
        }

        @Override
        public void delete(String nodeId, BrewId id) {
            if (failing.get()) {
                throw new IllegalStateException("storage unavailable");
            }
            store.delete(nodeId, id);
        }
    }

    private final FlakyRepository repository = new FlakyRepository();

    @BeforeEach
    void setUp() throws IOException {
        group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        first = freeRun(3);
        ports = new RangePortAllocator(new PortRange(first, first + 2), new PortRange(60_000, 60_019));
        barista = new DefaultBarista(BaristaSettings.defaults(), group, NioDatagramChannel.class, repository, ports,
                new StaticNodeIdentity("node", "127.0.0.1"));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        repository.failing.set(false);
        barista.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }

    @Test
    void aCreateTheRepositoryRefusesGivesItsPortBack() {
        repository.failing.set(true);
        assertThatThrownBy(() -> barista.create(brew("refused", 0, false))).hasMessage("storage unavailable");
        repository.failing.set(false);

        Brew next = barista.create(brew("next", 0, false));

        assertThat(port(next)).as("the refused brew's port is free again").isEqualTo(first);
    }

    @Test
    void anUpdateTheRepositoryRefusesKeepsTheOldPortAndGivesTheNewOneBack() {
        Brew original = barista.create(brew("original", first, false));
        repository.failing.set(true);

        assertThatThrownBy(() -> barista.update(original.spec().withSources(List.of(
                SourceSpec.of("source", 0, SrtListenerEndpoint.any().withPort(first + 1))))))
                .hasMessage("storage unavailable");

        assertThat(port(original)).as("the brew is unchanged").isEqualTo(first);
        assertThat(ports.allocate(PortKind.SRT)).as("old port still held, new one returned").isEqualTo(first + 1);
    }

    @Test
    void anUpdateThatMovesPortsReleasesTheOldOneOnlyOnceSaved() {
        Brew original = barista.create(brew("moving", first, false));

        barista.update(original.spec().withSources(List.of(
                SourceSpec.of("source", 0, SrtListenerEndpoint.any().withPort(first + 1)))));

        assertThat(ports.allocate(PortKind.SRT)).isEqualTo(first);
    }

    @Test
    void aDeleteTheRepositoryRefusesKeepsTheBrewAndItsPort() {
        Brew brew = barista.create(brew("kept", first, false));
        repository.failing.set(true);

        assertThatThrownBy(() -> barista.delete(brew.id())).hasMessage("storage unavailable");

        assertThat(barista.brew(brew.id())).isPresent();
        assertThat(ports.allocate(PortKind.SRT)).as("still held").isEqualTo(first + 1);
    }

    /**
     * Two stored brews naming one port: the first restores, the second fails
     * holding nothing. Deleting the failed one must not free the port the first
     * one holds; starting it again must try for the port afresh.
     */
    @Test
    void aFailedRestoreHoldsNothingAndReleasesNothing() {
        repository.store.save("node", brew("owner", first, false));
        repository.store.save("node", brew("conflict", first, false));
        barista.start();
        Brew conflict = barista.brew(new BrewId("conflict")).orElseThrow();
        assertThat(conflict.state()).isEqualTo(BrewState.FAILED);

        assertThatThrownBy(() -> barista.start(conflict.id())).isInstanceOf(IllegalArgumentException.class);
        barista.delete(conflict.id());

        assertThat(ports.allocate(PortKind.SRT)).as("the owner still holds its port").isEqualTo(first + 1);
    }

    @Test
    void aFailedRestoreCanStartOnceItsPortIsFree() {
        repository.store.save("node", brew("owner", first, false));
        repository.store.save("node", brew("conflict", first, false));
        barista.start();

        barista.delete(new BrewId("owner"));
        barista.start(new BrewId("conflict"));

        assertThat(barista.brew(new BrewId("conflict")).orElseThrow().state()).isNotEqualTo(BrewState.FAILED);
    }

    /** The stored spec says what runs: enabling through update starts, disabling stops. */
    @Test
    void updateHonoursEnabled() {
        Brew brew = barista.create(brew("toggled", 0, false));
        assertThat(brew.state()).isEqualTo(BrewState.STOPPED);

        barista.update(brew.spec().withEnabled(true));
        assertThat(brew.state()).isNotIn(BrewState.STOPPED, BrewState.FAILED);

        barista.update(brew.spec().withEnabled(false));
        assertThat(brew.state()).isEqualTo(BrewState.STOPPED);
        assertThat(repository.load("node").getFirst().enabled()).isFalse();
    }

    private static BrewSpec brew(String name, int port, boolean enabled) {
        return new BrewSpec(new BrewId(name), name,
                List.of(SourceSpec.of("source", 0, SrtListenerEndpoint.any().withPort(port))), List.of(), enabled);
    }

    private static int port(Brew brew) {
        return ((SrtListenerEndpoint) brew.spec().sources().getFirst().endpoint()).port();
    }

    private static int freeRun(int length) throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            int base;
            try (DatagramSocket probe = new DatagramSocket(0)) {
                base = probe.getLocalPort();
            }
            boolean free = base + length < 60_000;
            for (int offset = 0; free && offset < length; offset++) {
                try (DatagramSocket socket = new DatagramSocket(base + offset)) {
                    // free
                } catch (SocketException e) {
                    free = false;
                }
            }
            if (free) {
                return base;
            }
        }
        throw new IOException("no free run");
    }
}