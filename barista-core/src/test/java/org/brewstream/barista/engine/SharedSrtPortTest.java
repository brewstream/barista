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
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SharedSrtPortTest {

    private EventLoopGroup group;
    private SrtTransport transport;

    @BeforeEach
    void setUp() {
        group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        transport = SrtTransport.shared(group.next(), NioDatagramChannel.class);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }

    /** The port stays bound while any endpoint is on it and is released by the last one out. */
    @Test
    void closesThePortWhenTheLastEndpointLeaves() throws Exception {
        int port = freePort();
        AtomicInteger emptied = new AtomicInteger();
        SharedSrtPort shared = SharedSrtPort.bind(port, Duration.ofMillis(120), transport, emptied::incrementAndGet);
        SharedSrtPort.Route in = route("in");
        SharedSrtPort.Route out = route("out");
        shared.register(in);
        shared.register(out);

        shared.unregister(in);
        assertThat(emptied).hasValue(0);
        assertThat(canBind(port)).as("still bound for 'out'").isFalse();

        shared.unregister(out);
        assertThat(emptied).hasValue(1);
        assertThat(awaitBindable(port)).as("released").isTrue();
    }

    @Test
    void refusesTheSameStreamIdTwice() throws Exception {
        SharedSrtPort shared = SharedSrtPort.bind(freePort(), Duration.ofMillis(120), transport, () -> { });
        SharedSrtPort.Route first = route("in");
        shared.register(first);

        assertThatThrownBy(() -> shared.register(route("in"))).isInstanceOf(IllegalStateException.class);
        shared.unregister(first);
    }

    private static SharedSrtPort.Route route(String streamId) {
        return new SharedSrtPort.Route(streamId, request -> AcceptDecision.accept(), connection -> { }, reason -> { });
    }

    private static boolean canBind(int port) {
        try (DatagramSocket socket = new DatagramSocket(port)) {
            return true;
        } catch (SocketException e) {
            return false;
        }
    }

    /** NIO releases a closed UDP port at the selector's next turn, so give it a moment. */
    private static boolean awaitBindable(int port) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (canBind(port)) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    private static int freePort() throws SocketException {
        try (DatagramSocket probe = new DatagramSocket(0)) {
            return probe.getLocalPort();
        }
    }
}
