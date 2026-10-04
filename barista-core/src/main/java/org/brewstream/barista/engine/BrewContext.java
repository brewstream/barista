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
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import org.brewstream.barista.EndpointState;
import org.brewstream.press.net.PressTransport;
import org.brewstream.roast.socket.SrtTransport;

import java.util.concurrent.ScheduledExecutorService;

/** What a leg needs from the brew it belongs to. */
interface BrewContext {

    BaristaSettings settings();

    String publishedHost();

    ByteBufAllocator allocator();

    /** The brew's own loop: every source runs on it. */
    EventLoop brewLoop();

    /** A loop for a new output, spread across the node's event loops. */
    EventLoop nextOutputLoop();

    /** Roast on one event loop. */
    SrtTransport srtTransport(EventLoop loop);

    /** Press on one event loop. */
    PressTransport pressTransport(EventLoop loop);

    /** Where callers schedule their dials, so no event loop ever waits on one. */
    ScheduledExecutorService dialer();

    /** A chunk of whole TS packets from a source, on the brew loop. Takes ownership. */
    void onSourceChunk(SourceLeg source, ByteBuf chunk);

    void endpointStateChanged(String endpointId, EndpointState from, EndpointState to);
}