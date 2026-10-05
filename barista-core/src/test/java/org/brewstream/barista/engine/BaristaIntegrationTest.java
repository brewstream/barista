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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.brewstream.barista.Brew;
import org.brewstream.barista.ConnectionView;
import org.brewstream.barista.RtpReceiveStats;
import org.brewstream.barista.RtpSendStats;
import org.brewstream.barista.SrtStats;
import org.brewstream.barista.BrewListener;
import org.brewstream.barista.BrewState;
import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointHealth;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;
import org.brewstream.barista.TsFixtures;
import org.brewstream.barista.spec.BrewId;
import org.brewstream.barista.spec.BrewSpec;
import org.brewstream.barista.spec.FailoverPolicy;
import org.brewstream.barista.spec.OutputSpec;
import org.brewstream.barista.spec.RtpReceiveEndpoint;
import org.brewstream.barista.spec.RtpSendEndpoint;
import org.brewstream.barista.spec.SourceId;
import org.brewstream.barista.spec.SourceSpec;
import org.brewstream.barista.spec.SrtCallerEndpoint;
import org.brewstream.barista.spec.SrtListenerEndpoint;
import org.brewstream.barista.support.InMemoryBrewRepository;
import org.brewstream.barista.support.PortRange;
import org.brewstream.barista.support.RangePortAllocator;
import org.brewstream.barista.support.StaticNodeIdentity;
import org.brewstream.press.net.RtpReceiver;
import org.brewstream.press.net.RtpReceiverConfig;
import org.brewstream.press.net.RtpSender;
import org.brewstream.press.net.RtpSenderConfig;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Brews over real sockets on loopback, with Roast and Press as the peers on both
 * sides, as they would be on a network.
 */
class BaristaIntegrationTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final String HOST = LOOPBACK.getHostAddress();

    private final List<AutoCloseable> resources = new ArrayList<>();
    private EventLoopGroup group;
    private InMemoryBrewRepository repository;
    private int srtFirst;
    private int rtpFirst;
    private DefaultBarista barista;

    @BeforeEach
    void setUp() throws IOException {
        group = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
        repository = new InMemoryBrewRepository();
        srtFirst = freeRun(10);
        rtpFirst = freeRun(40);
        barista = engine(BaristaSettings.defaults());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable resource : resources.reversed()) {
            try {
                resource.close();
            } catch (Exception e) {
                // keep closing the rest
            }
        }
        barista.close();
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
    }

    @Test
    void relaysAnSrtPublisherToRtpAndSrtOutputs() throws Exception {
        Sink rtp = rtpSink();
        Sink srt = srtSink();
        Brew brew = barista.create(BrewSpec.of("srt-in",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())),
                List.of(OutputSpec.of("to-rtp", RtpSendEndpoint.to(HOST, rtp.port)),
                        OutputSpec.of("to-srt", SrtCallerEndpoint.to(HOST, srt.port)))));
        int port = ((SrtListenerEndpoint) brew.spec().sources().getFirst().endpoint()).port();
        assertThat(port).as("allocated from the SRT range").isBetween(srtFirst, srtFirst + 9);
        awaitOutput(brew, "to-srt", EndpointState.ACTIVE);

        SrtConnection publisher = srtPublish(port);
        byte[] ts = TsFixtures.packets(0, 7 * 60);
        send(publisher, ts);

        rtp.await(ts.length);
        srt.await(ts.length);
        assertThat(rtp.bytes()).isEqualTo(ts);
        assertThat(srt.bytes()).isEqualTo(ts);
        await(() -> brew.state() == BrewState.RUNNING, "brew running");
        EndpointStatus source = brew.status().sources().getFirst();
        assertThat(source.state()).isEqualTo(EndpointState.ACTIVE);
        assertThat(source.address()).isEqualTo(HOST + ":" + port);
        assertThat(source.bytes()).isEqualTo(ts.length);
        await(() -> brew.status().sources().getFirst().tsStats() != null, "source health");

        // Every leg kind reports its connection in its own transport's terms. (#4)
        assertThat(source.connections()).singleElement().satisfies(view -> {
            assertThat(view.streamId()).isEqualTo("publish");
            assertThat(view.stats()).isInstanceOf(SrtStats.class);
        });
        List<EndpointStatus> outputs = brew.status().outputs();
        assertThat(outputs.get(0).connections()).singleElement().satisfies(view -> {
            assertThat(view.peer()).isEqualTo(HOST + ":" + rtp.port);
            assertThat(view.stats()).isInstanceOfSatisfying(RtpSendStats.class,
                    stats -> assertThat(stats.bytesSent()).isEqualTo(ts.length));
        });
        assertThat(outputs.get(1).connections()).singleElement()
                .satisfies(view -> assertThat(view.stats()).isInstanceOf(SrtStats.class));
    }

    @Test
    void relaysAnRtpSourceToEverySrtSubscriber() throws Exception {
        Brew brew = barista.create(BrewSpec.of("rtp-in",
                List.of(SourceSpec.of("feed", 0, RtpReceiveEndpoint.unicast())),
                List.of(OutputSpec.of("pull", SrtListenerEndpoint.any()))));
        int rtpPort = ((RtpReceiveEndpoint) brew.spec().sources().getFirst().endpoint()).port();
        int srtPort = ((SrtListenerEndpoint) brew.spec().outputs().getFirst().endpoint()).port();
        assertThat(rtpPort).as("an RTP block base").isEqualTo(rtpFirst);
        Sink first = srtSubscriber(srtPort);
        Sink second = srtSubscriber(srtPort);
        await(() -> brew.status().outputs().getFirst().connections().size() == 2, "two subscribers");

        RtpSender sender = track(RtpSender.connect(RtpSenderConfig.to(new InetSocketAddress(LOOPBACK, rtpPort))));
        byte[] ts = TsFixtures.packets(0, 7 * 60);
        send(sender, ts);

        first.await(ts.length);
        second.await(ts.length);
        assertThat(first.bytes()).isEqualTo(ts);
        assertThat(second.bytes()).isEqualTo(ts);

        // Each subscriber is its own connection, with its own statistics. (#4)
        List<ConnectionView> subscribers = brew.status().outputs().getFirst().connections();
        assertThat(subscribers).hasSize(2);
        assertThat(subscribers).extracting(ConnectionView::id).doesNotHaveDuplicates();
        assertThat(subscribers).allSatisfy(view -> {
            assertThat(view.streamId()).isEqualTo("pull");
            assertThat(view.peer()).startsWith(HOST + ":");
            assertThat(view.connectedSinceMillis()).isPositive();
            assertThat(view.stats()).isInstanceOfSatisfying(SrtStats.class,
                    stats -> assertThat(stats.bytesSent()).isEqualTo(ts.length));
        });
        ConnectionView feed = brew.status().sources().getFirst().connections().getFirst();
        assertThat(feed.stats()).isInstanceOfSatisfying(RtpReceiveStats.class,
                stats -> assertThat(stats.bytesReceived()).isEqualTo(ts.length));
    }

    @Test
    void outputsJoinAndLeaveWhileTheBrewRuns() throws Exception {
        Sink a = rtpSink();
        Sink b = rtpSink();
        Brew brew = barista.create(BrewSpec.of("live-changes",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())),
                List.of(OutputSpec.of("a", RtpSendEndpoint.to(HOST, a.port)))));
        SrtConnection publisher = srtPublish(srtPort(brew));
        byte[] first = TsFixtures.packets(0, 70);
        byte[] second = TsFixtures.packets(70, 70);
        byte[] third = TsFixtures.packets(140, 70);

        send(publisher, first);
        a.await(first.length);
        barista.update(brew.spec().withOutputs(List.of(brew.spec().outputs().getFirst(),
                OutputSpec.of("b", RtpSendEndpoint.to(HOST, b.port)))));
        send(publisher, second);
        b.await(second.length);
        a.await(first.length + second.length);
        barista.update(brew.spec().withOutputs(List.of(brew.spec().outputs().get(1))));
        send(publisher, third);
        b.await(second.length + third.length);
        Thread.sleep(200);

        assertThat(a.bytes()).isEqualTo(concat(first, second));
        assertThat(b.bytes()).isEqualTo(concat(second, third));
        assertThat(brew.spec().outputs()).extracting(o -> o.id().value()).containsExactly("b");
    }

    @Test
    void switchesBetweenSourcesThatAreAllKeptConnected() throws Exception {
        Sink out = rtpSink();
        List<SourceId> activations = new CopyOnWriteArrayList<>();
        barista.addListener(new BrewListener() {
            @Override
            public void onSourceActivated(BrewId brew, SourceId source, String reason) {
                activations.add(source);
            }
        });
        Brew brew = barista.create(BrewSpec.of("pair",
                List.of(SourceSpec.of("main", 0, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())),
                List.of(OutputSpec.of("out", RtpSendEndpoint.to(HOST, out.port)))));
        RtpSender main = track(RtpSender.connect(RtpSenderConfig.to(rtpAddress(brew, 0))));
        RtpSender backup = track(RtpSender.connect(RtpSenderConfig.to(rtpAddress(brew, 1))));
        byte[] fromMain = TsFixtures.packets(0, 70);
        byte[] fromBackup = TsFixtures.packets(1000, 70);

        send(main, fromMain);
        send(backup, fromBackup);
        out.await(fromMain.length);
        Thread.sleep(200);
        assertThat(out.bytes()).as("only the active source reaches the outputs").isEqualTo(fromMain);
        assertThat(brew.status().sources().get(1).bytes()).as("the backup is monitored all the same")
                .isEqualTo(fromBackup.length);

        barista.activate(brew.id(), new SourceId("backup"));
        byte[] laterBackup = TsFixtures.packets(2000, 70);
        send(main, TsFixtures.packets(3000, 70));
        send(backup, laterBackup);
        out.await(fromMain.length + laterBackup.length);
        Thread.sleep(200);

        assertThat(out.bytes()).isEqualTo(concat(fromMain, laterBackup));
        assertThat(brew.activeSource()).isEqualTo(new SourceId("backup"));
        await(() -> activations.contains(new SourceId("backup")), "activation event");
    }

    /**
     * A source whose stream breaks its continuity counter is DEGRADED for the window that saw
     * it, then GOOD again once a window passes clean. (#6)
     */
    @Test
    void continuityErrorsDegradeASourceForAWindow() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofSeconds(2), Duration.ofMillis(200), Duration.ofSeconds(1), Duration.ofMillis(1500), 1.0));
        Brew brew = barista.create(BrewSpec.of("damaged",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())), List.of()));
        SrtConnection publisher = srtPublish(srtPort(brew));
        List<EndpointHealth> seen = new ArrayList<>();
        int packet = 0;
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (System.nanoTime() < end && !(seen.contains(EndpointHealth.DEGRADED)
                && seen.getLast() == EndpointHealth.GOOD)) {
            if (packet == 7 * 20) {
                packet += 3; // three packets missing: a continuity error
            }
            send(publisher, TsFixtures.packets(packet, 7));
            packet += 7;
            Thread.sleep(20);
            EndpointHealth health = brew.status().sources().getFirst().health();
            if (seen.isEmpty() || seen.getLast() != health) {
                seen.add(health);
            }
        }

        assertThat(seen).as("health over time").containsSubsequence(
                EndpointHealth.GOOD, EndpointHealth.DEGRADED, EndpointHealth.GOOD);
    }

    /**
     * An output whose peer never answers fills only its own queue, drops from it,
     * and holds up nothing else. Queues are 128 KiB here: enough for a burst of
     * source data (Roast releases tens of packets in one pass, which a queue
     * smaller than that drops from even when drained on its own thread), and
     * small enough that the dead output overflows on a 260 KiB stream.
     */
    @Test
    void aDeadOutputHoldsUpNothingElse() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 128 * 1024, 128 * 1024, Duration.ofSeconds(2),
                Duration.ofMillis(200), Duration.ofSeconds(1), Duration.ofSeconds(5), 1.0));
        Sink live = rtpSink();
        int nobody = freeRun(1);
        Brew brew = barista.create(BrewSpec.of("one-dead",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())),
                List.of(OutputSpec.of("dead", SrtCallerEndpoint.to(HOST, nobody)),
                        OutputSpec.of("live", RtpSendEndpoint.to(HOST, live.port)))));
        SrtConnection publisher = srtPublish(srtPort(brew));
        byte[] ts = TsFixtures.packets(0, 7 * 200);

        send(publisher, ts);

        live.await(ts.length);
        assertThat(live.bytes()).isEqualTo(ts);
        EndpointStatus dead = brew.status().outputs().getFirst();
        assertThat(dead.state()).isIn(EndpointState.CONNECTING, EndpointState.RECONNECTING);
        assertThat(dead.droppedChunks()).as("it dropped from its own queue").isPositive();
        assertThat(dead.health()).isEqualTo(EndpointHealth.DOWN);
        assertThat(brew.status().outputs().get(1).droppedChunks()).isZero();
    }

    @Test
    void reportsALostSourceAndItsReturn() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofMillis(400), Duration.ofMillis(200), Duration.ofSeconds(1), Duration.ofSeconds(5), 1.0));
        List<BrewState> states = new CopyOnWriteArrayList<>();
        barista.addListener(new BrewListener() {
            @Override
            public void onBrewStateChanged(BrewId brew, BrewState from, BrewState to) {
                states.add(to);
            }
        });
        Brew brew = barista.create(BrewSpec.of("lossy",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())), List.of()));
        SrtConnection publisher = srtPublish(srtPort(brew));

        send(publisher, TsFixtures.packets(0, 14));
        await(() -> brew.state() == BrewState.RUNNING, "running");
        await(() -> brew.state() == BrewState.SOURCE_LOST, "lost after 400 ms of silence");
        assertThat(brew.status().sources().getFirst().state()).isEqualTo(EndpointState.IDLE);
        send(publisher, TsFixtures.packets(14, 14));
        await(() -> brew.state() == BrewState.RUNNING, "running again");

        await(() -> states.containsAll(List.of(BrewState.RUNNING, BrewState.SOURCE_LOST)), "events");
    }

    /** Restart: the same repository and port ranges bring the brew back on the ports it had. */
    @Test
    void comesBackAfterARestartOnTheSamePorts() throws Exception {
        Sink rtp = rtpSink();
        Brew before = barista.create(BrewSpec.of("durable",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())),
                List.of(OutputSpec.of("out", RtpSendEndpoint.to(HOST, rtp.port)))));
        int port = srtPort(before);
        barista.close();

        barista = engine(BaristaSettings.defaults());
        barista.start();

        Brew after = barista.brew(before.id()).orElseThrow();
        assertThat(srtPort(after)).isEqualTo(port);
        SrtConnection publisher = srtPublish(port);
        byte[] ts = TsFixtures.packets(0, 70);
        send(publisher, ts);
        rtp.await(ts.length);
        assertThat(rtp.bytes()).isEqualTo(ts);
    }

    @Test
    void aBrewThatCannotBindFailsWithTheReasonAndOthersCarryOn() throws Exception {
        int taken = freeRun(1);
        try (DatagramSocket squatter = new DatagramSocket(taken)) {
            Brew failed = barista.create(BrewSpec.of("collides",
                    List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any().withPort(taken))), List.of()));
            Brew fine = barista.create(BrewSpec.of("fine",
                    List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())), List.of()));

            assertThat(failed.state()).isEqualTo(BrewState.FAILED);
            assertThat(failed.status().error()).contains("srt-listener encoder");
            assertThat(fine.state()).isEqualTo(BrewState.STARTING);
        }
    }

    /**
     * A change from one of Barista's own event loops would wait for work that
     * may need that loop. It is refused at once, and the engine carries on.
     * (From the Codex review, which showed stop() deadlocking there.)
     */
    @Test
    void refusesChangesFromItsOwnEventLoops() throws Exception {
        Brew brew = barista.create(BrewSpec.of("loop-call",
                List.of(SourceSpec.of("in", 0, RtpReceiveEndpoint.unicast())), List.of()));

        io.netty.util.concurrent.Future<?> call = group.next().submit(() -> barista.stop(brew.id()));

        assertThat(call.await(5, TimeUnit.SECONDS)).as("refused at once, not stuck").isTrue();
        assertThat(call.cause()).isInstanceOf(IllegalStateException.class).hasMessageContaining("event loop");
        assertThat(brew.state()).isNotEqualTo(BrewState.STOPPED);
        barista.stop(brew.id());
        assertThat(brew.state()).isEqualTo(BrewState.STOPPED);
    }

    /**
     * Press's sender packs seven TS packets per RTP packet. Three must still go
     * out while the brew stays open, not wait for four more. (From the Codex review.)
     */
    @Test
    void aShortRunOfTsPacketsLeavesAnRtpOutputPromptly() throws Exception {
        try (DatagramSocket sink = new DatagramSocket(0, LOOPBACK)) {
            sink.setSoTimeout(2000);
            Brew brew = barista.create(BrewSpec.of("short",
                    List.of(SourceSpec.of("in", 0, RtpReceiveEndpoint.unicast())),
                    List.of(OutputSpec.of("out", RtpSendEndpoint.to(HOST, sink.getLocalPort())))));
            RtpSender sender = track(RtpSender.connect(RtpSenderConfig.to(rtpAddress(brew, 0)).withRtcp(false)));

            sender.write(Unpooled.wrappedBuffer(TsFixtures.packets(0, 3)));
            sender.flush();

            java.net.DatagramPacket datagram = new java.net.DatagramPacket(new byte[2000], 2000);
            sink.receive(datagram);
            assertThat(datagram.getLength()).isEqualTo(12 + 3 * 188);
        }
    }

    // --- event history (#5) ------------------------------------------------------

    @Test
    void recordsAPublisherArrivingAndLeavingAndRefusesASecond() throws Exception {
        List<EndpointEvent> heard = new CopyOnWriteArrayList<>();
        barista.addListener(new BrewListener() {
            @Override
            public void onEndpointEvent(BrewId brew, String endpointId, EndpointEvent event) {
                heard.add(event);
            }
        });
        Brew brew = barista.create(BrewSpec.of("history",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())), List.of()));
        SrtConnection first = srtPublish(srtPort(brew));

        assertThat(SrtCaller.connect(new InetSocketAddress(LOOPBACK, srtPort(brew)), "second")
                .handle((connection, failure) -> failure).get(10, TimeUnit.SECONDS)).isNotNull();
        first.close();
        await(() -> kinds(brew).contains(EndpointEvent.Kind.DISCONNECTED), "disconnect recorded");

        List<EndpointEvent> history = brew.status().sources().getFirst().history();
        assertThat(history).extracting(EndpointEvent::kind).containsSubsequence(
                EndpointEvent.Kind.STARTED, EndpointEvent.Kind.ACTIVATED, EndpointEvent.Kind.CONNECTED,
                EndpointEvent.Kind.REJECTED, EndpointEvent.Kind.DISCONNECTED);
        assertThat(reason(history, EndpointEvent.Kind.CONNECTED)).startsWith("publisher " + HOST + ":")
                .endsWith("(stream 'publish')");
        assertThat(reason(history, EndpointEvent.Kind.REJECTED)).contains("another publisher is already connected");
        assertThat(reason(history, EndpointEvent.Kind.DISCONNECTED)).endsWith("(cause unknown)");
        await(() -> heard.stream().anyMatch(e -> e.kind() == EndpointEvent.Kind.REJECTED), "listener told");
    }

    @Test
    void recordsACallerRefusedForAnUnknownStreamId() throws Exception {
        Brew brew = barista.create(BrewSpec.of("named",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())),
                List.of(OutputSpec.of("studio", new SrtListenerEndpoint(0, "studio", null, Duration.ofMillis(120))))));
        int port = ((SrtListenerEndpoint) brew.spec().outputs().getFirst().endpoint()).port();

        SrtCaller.connect(new InetSocketAddress(LOOPBACK, port), "wrong").handle((c, f) -> f).get(10, TimeUnit.SECONDS);

        await(() -> brew.status().outputs().getFirst().history().stream()
                .anyMatch(e -> e.kind() == EndpointEvent.Kind.REJECTED), "refusal recorded");
        assertThat(reason(brew.status().outputs().getFirst().history(), EndpointEvent.Kind.REJECTED))
                .contains("unknown stream ID 'wrong'");
    }

    @Test
    void explainsWhyACallerCannotConnect() throws Exception {
        SrtListener remote = SrtListener.bind(new InetSocketAddress(LOOPBACK, 0));
        resources.add(remote::close);
        remote.setAcceptHandler(request -> AcceptDecision.accept("the-right-passphrase".toCharArray(), 16));
        int nobody = freeRun(1);
        Brew brew = barista.create(BrewSpec.of("failing",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())),
                List.of(OutputSpec.of("wrong-secret", new SrtCallerEndpoint(HOST, remote.localAddress().getPort(),
                                null, new org.brewstream.barista.spec.SrtSecurity("a-wrong-passphrase", 16),
                                Duration.ofMillis(120))),
                        OutputSpec.of("nobody", SrtCallerEndpoint.to(HOST, nobody)))));

        await(() -> kindsOf(brew.status().outputs().get(0)).contains(EndpointEvent.Kind.FAILED), "passphrase failure");
        await(() -> kindsOf(brew.status().outputs().get(1)).contains(EndpointEvent.Kind.FAILED), "timeout failure");

        assertThat(reason(brew.status().outputs().get(0).history(), EndpointEvent.Kind.FAILED))
                .containsAnyOf("wrong passphrase", "passphrase mismatch");
        assertThat(reason(brew.status().outputs().get(1).history(), EndpointEvent.Kind.FAILED))
                .isEqualTo("no answer from " + HOST + ":" + nobody);
    }

    @Test
    void recordsActivationIdlenessAndAnRtpSender() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofMillis(400), Duration.ofMillis(200), Duration.ofSeconds(1), Duration.ofSeconds(5), 1.0));
        Brew brew = barista.create(BrewSpec.of("pair",
                List.of(SourceSpec.of("main", 0, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())), List.of()));
        RtpSender main = track(RtpSender.connect(RtpSenderConfig.to(rtpAddress(brew, 0))));

        send(main, TsFixtures.packets(0, 14));
        await(() -> kindsOf(brew.status().sources().get(0)).contains(EndpointEvent.Kind.IDLE), "idle recorded");
        send(main, TsFixtures.packets(14, 14));
        await(() -> kindsOf(brew.status().sources().get(0)).contains(EndpointEvent.Kind.RESUMED), "resume recorded");
        barista.activate(brew.id(), new SourceId("backup"));

        List<EndpointEvent> mainHistory = brew.status().sources().get(0).history();
        assertThat(reason(mainHistory, EndpointEvent.Kind.CONNECTED)).startsWith("sender " + HOST + ":").endsWith("heard");
        assertThat(reason(mainHistory, EndpointEvent.Kind.IDLE)).isEqualTo("no data for 400 ms");
        assertThat(reason(mainHistory, EndpointEvent.Kind.DEACTIVATED)).isEqualTo("replaced by backup: by operator");
        assertThat(reason(brew.status().sources().get(1).history(), EndpointEvent.Kind.ACTIVATED)).isEqualTo("by operator");
    }

    /**
     * Losing the active source switches to the next healthy one by priority, not merely
     * "the other" one, and says why. It does not switch back on its own. (#7)
     */
    @Test
    void failsOverToTheNextHealthySourceByPriority() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofMillis(400), Duration.ofMillis(200), Duration.ofSeconds(1), Duration.ofSeconds(5), 1.0));
        List<String> activations = new CopyOnWriteArrayList<>();
        barista.addListener(new BrewListener() {
            @Override
            public void onSourceActivated(BrewId brew, SourceId source, String reason) {
                activations.add(source.value() + " " + reason);
            }
        });
        Brew brew = barista.create(BrewSpec.of("trio",
                List.of(SourceSpec.of("spare", 2, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("main", 0, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())), List.of())
                .withFailover(FailoverPolicy.automatic().withMinDwell(Duration.ofMillis(300))));
        Pump spare = pump(brew, 0);
        Pump main = pump(brew, 1);
        Pump backup = pump(brew, 2);
        await(() -> brew.status().sources().stream().allMatch(s -> s.health() == EndpointHealth.GOOD),
                "all three delivering");
        assertThat(brew.activeSource()).isEqualTo(new SourceId("main"));

        main.close();

        await(() -> brew.activeSource().equals(new SourceId("backup")), "failed over to backup");
        String why = "failover from main: no data for 400 ms";
        assertThat(activations).containsExactly("backup " + why);
        assertThat(reason(brew.status().sources().get(1).history(), EndpointEvent.Kind.DEACTIVATED))
                .isEqualTo("replaced by backup: " + why);
        assertThat(reason(brew.status().sources().get(2).history(), EndpointEvent.Kind.ACTIVATED)).isEqualTo(why);

        Pump again = pump(brew, 1);
        await(() -> brew.status().sources().get(1).health() == EndpointHealth.GOOD, "main back");
        Thread.sleep(1000);
        assertThat(brew.activeSource()).as("no failback unless configured").isEqualTo(new SourceId("backup"));
        assertThat(spare.sent() + backup.sent() + again.sent()).isPositive();
    }

    /** With failback on, the preferred source takes over again once it has delivered for the dwell. (#7) */
    @Test
    void failsBackWhenConfigured() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofMillis(400), Duration.ofMillis(200), Duration.ofSeconds(1), Duration.ofSeconds(5), 1.0));
        Brew brew = barista.create(BrewSpec.of("pair",
                List.of(SourceSpec.of("main", 0, RtpReceiveEndpoint.unicast()),
                        SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())), List.of())
                .withFailover(FailoverPolicy.automatic().withMinDwell(Duration.ofMillis(300)).withFailback(true)));
        Pump main = pump(brew, 0);
        pump(brew, 1);
        await(() -> brew.status().sources().stream().allMatch(s -> s.health() == EndpointHealth.GOOD), "both");

        main.close();
        await(() -> brew.activeSource().equals(new SourceId("backup")), "failed over");
        pump(brew, 0);

        await(() -> brew.activeSource().equals(new SourceId("main")), "failed back");
        assertThat(reason(brew.status().sources().get(0).history(), EndpointEvent.Kind.ACTIVATED))
                .startsWith("failback from backup: main delivering for ");
    }

    /**
     * An SRT caller source that cannot dial counts as lost after the configured failed dials,
     * long before its 60 s loss timeout. (#7)
     */
    @Test
    void failsOverFromACallerThatCannotDial() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofSeconds(60), Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofSeconds(5), 1.0));
        int nobody = freeRun(1);
        Brew brew = barista.create(BrewSpec.of("dialler",
                List.of(SourceSpec.of("main", 0, SrtCallerEndpoint.to(HOST, nobody)),
                        SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())), List.of())
                .withFailover(FailoverPolicy.automatic().withMinDwell(Duration.ZERO).withMaxFailedDials(1)));
        pump(brew, 1);

        await(() -> brew.activeSource().equals(new SourceId("backup")), "failed over after a failed dial (5 s connect timeout)");
        assertThat(reason(brew.status().sources().get(1).history(), EndpointEvent.Kind.ACTIVATED))
                .isEqualTo("failover from main: a dial failed");
    }

    /**
     * A caller that failed and then connected starts counting failed dials afresh: with
     * failback on, it takes over again and stays, rather than being judged lost by its old
     * failures. (#7)
     */
    @Test
    void aCallerThatReconnectsForgetsItsFailedDials() throws Exception {
        barista.close();
        barista = engine(new BaristaSettings(Duration.ofSeconds(2), 256 * 1024, 16 * 1024 * 1024,
                Duration.ofSeconds(60), Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofSeconds(5), 1.0));
        int later = freeRun(1);
        List<String> activations = new CopyOnWriteArrayList<>();
        barista.addListener(new BrewListener() {
            @Override
            public void onSourceActivated(BrewId brew, SourceId source, String reason) {
                activations.add(source.value());
            }
        });
        Brew brew = barista.create(BrewSpec.of("dialler",
                List.of(SourceSpec.of("main", 0, SrtCallerEndpoint.to(HOST, later)),
                        SourceSpec.of("backup", 1, RtpReceiveEndpoint.unicast())), List.of())
                .withFailover(FailoverPolicy.automatic().withMinDwell(Duration.ZERO).withMaxFailedDials(1)
                        .withFailback(true)));
        pump(brew, 1);
        await(() -> brew.activeSource().equals(new SourceId("backup")), "failed over after a failed dial");

        Brew upstream = barista.create(BrewSpec.of("upstream",
                List.of(SourceSpec.of("in", 0, RtpReceiveEndpoint.unicast())),
                List.of(OutputSpec.of("out", SrtListenerEndpoint.any().withPort(later)))));
        pump(upstream, 0);

        await(() -> brew.activeSource().equals(new SourceId("main")), "failed back once main delivers");
        Thread.sleep(1000);
        assertThat(brew.activeSource()).as("main stays").isEqualTo(new SourceId("main"));
        assertThat(activations).containsExactly("backup", "main");
    }

    /** Keeps an RTP source fed, in order, until closed. */
    private final class Pump implements AutoCloseable {

        private final RtpSender sender;
        private final Thread thread;
        private volatile boolean running = true;
        private volatile int packet;

        Pump(InetSocketAddress target) throws Exception {
            sender = RtpSender.connect(RtpSenderConfig.to(target));
            thread = Thread.ofPlatform().daemon().start(() -> {
                while (running) {
                    sender.write(Unpooled.wrappedBuffer(TsFixtures.packets(packet, 7)));
                    packet += 7;
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
        }

        int sent() {
            return packet;
        }

        @Override
        public void close() throws Exception {
            running = false;
            thread.join();
            sender.close();
        }
    }

    private Pump pump(Brew brew, int source) throws Exception {
        return track(new Pump(rtpAddress(brew, source)));
    }

    private static List<EndpointEvent.Kind> kinds(Brew brew) {
        return kindsOf(brew.status().sources().getFirst());
    }

    private static List<EndpointEvent.Kind> kindsOf(EndpointStatus endpoint) {
        return endpoint.history().stream().map(EndpointEvent::kind).toList();
    }

    private static String reason(List<EndpointEvent> history, EndpointEvent.Kind kind) {
        return history.stream().filter(e -> e.kind() == kind).reduce((a, b) -> b).orElseThrow().reason();
    }

    @Test
    void deleteReleasesPortsForTheNextBrew() {
        Brew first = barista.create(BrewSpec.of("first",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())), List.of()));
        int port = srtPort(first);

        barista.delete(first.id());
        Brew second = barista.create(BrewSpec.of("second",
                List.of(SourceSpec.of("encoder", 0, SrtListenerEndpoint.any())), List.of()));

        assertThat(srtPort(second)).isEqualTo(port);
        assertThat(repository.load("test-node")).extracting(BrewSpec::name).containsExactly("second");
    }


    private DefaultBarista engine(BaristaSettings settings) {
        return new DefaultBarista(settings, group, NioDatagramChannel.class, repository,
                new RangePortAllocator(new PortRange(srtFirst, srtFirst + 9), new PortRange(rtpFirst, rtpFirst + 39)),
                new StaticNodeIdentity("test-node", HOST));
    }

    private static int srtPort(Brew brew) {
        return ((SrtListenerEndpoint) brew.spec().sources().getFirst().endpoint()).port();
    }

    private static InetSocketAddress rtpAddress(Brew brew, int source) {
        return new InetSocketAddress(LOOPBACK,
                ((RtpReceiveEndpoint) brew.spec().sources().get(source).endpoint()).port());
    }

    private SrtConnection srtPublish(int port) throws Exception {
        SrtConnection connection = SrtCaller.connect(new InetSocketAddress(LOOPBACK, port), "publish")
                .get(5, TimeUnit.SECONDS);
        resources.add(connection::close);
        return connection;
    }

    /** Writes in 1316-byte pieces, briefly pacing so loopback buffers never overflow. */
    private static void send(SrtConnection connection, byte[] ts) throws InterruptedException {
        for (int at = 0; at < ts.length; at += 1316) {
            connection.write(Unpooled.wrappedBuffer(ts, at, Math.min(1316, ts.length - at)));
            if ((at / 1316) % 8 == 7) {
                Thread.sleep(2);
            }
        }
    }

    private static void send(RtpSender sender, byte[] ts) throws InterruptedException {
        for (int at = 0; at < ts.length; at += 1316) {
            sender.write(Unpooled.wrappedBuffer(ts, at, Math.min(1316, ts.length - at)));
            if ((at / 1316) % 8 == 7) {
                Thread.sleep(2);
            }
        }
        sender.flush();
    }

    private <T extends AutoCloseable> T track(T resource) {
        resources.add(resource);
        return resource;
    }

    /** Collects bytes arriving at a test peer. */
    private static final class Sink {
        final int port;
        private final ByteArrayOutputStream received = new ByteArrayOutputStream();

        Sink(int port) {
            this.port = port;
        }

        void accept(ByteBuf payload) {
            synchronized (received) {
                received.writeBytes(ByteBufUtil.getBytes(payload));
            }
            payload.release();
        }

        byte[] bytes() {
            synchronized (received) {
                return received.toByteArray();
            }
        }

        void await(int length) throws InterruptedException {
            BaristaIntegrationTest.await(() -> bytes().length >= length,
                    () -> "received " + bytes().length + " of " + length + " bytes");
        }
    }

    private Sink rtpSink() throws Exception {
        Sink[] holder = new Sink[1];
        RtpReceiver receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)),
                pipeline -> pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>(false) {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                        holder[0].accept(payload);
                    }
                }));
        holder[0] = new Sink(receiver.localAddress().getPort());
        resources.add(receiver);
        return holder[0];
    }

    private Sink srtSink() throws Exception {
        SrtListener listener = SrtListener.bind(new InetSocketAddress(LOOPBACK, 0));
        Sink sink = new Sink(listener.localAddress().getPort());
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        listener.onConnection(connection -> connection.onData(sink::accept));
        resources.add(listener::close);
        return sink;
    }

    private Sink srtSubscriber(int port) throws Exception {
        Sink sink = new Sink(port);
        SrtConnection connection = SrtCaller.connect(new InetSocketAddress(LOOPBACK, port), "pull")
                .get(5, TimeUnit.SECONDS);
        connection.onData(sink::accept);
        resources.add(connection::close);
        return sink;
    }

    private static void awaitOutput(Brew brew, String id, EndpointState state) throws InterruptedException {
        await(() -> brew.status().outputs().stream().anyMatch(o -> o.id().equals(id) && o.state() == state),
                "output " + id + " " + state);
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        await(condition, () -> what);
    }

    private static void await(BooleanSupplier condition, java.util.function.Supplier<String> what)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what.get());
            }
            Thread.sleep(10);
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] both = new byte[a.length + b.length];
        System.arraycopy(a, 0, both, 0, a.length);
        System.arraycopy(b, 0, both, a.length, b.length);
        return both;
    }

    /** A run of free ports, so allocated ports are not taken by something else on the machine. */
    private static int freeRun(int length) throws IOException {
        for (int attempt = 0; attempt < 100; attempt++) {
            int base;
            try (DatagramSocket probe = new DatagramSocket(0)) {
                base = probe.getLocalPort() / 10 * 10;
            }
            boolean free = base > 1024 && base + length < 65535;
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
        throw new IOException("no run of " + length + " free ports");
    }
}