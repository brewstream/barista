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

import org.brewstream.roast.socket.SrtConnection;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Keeps an SRT caller connected: dials, and when the dial fails or the
 * connection later drops, waits and dials again, doubling the wait up to a
 * ceiling and starting over after a success. Dials are scheduled on the
 * engine's dialer thread, never on an event loop.
 */
final class Redialer {

    private final BrewContext context;
    private final Supplier<CompletableFuture<SrtConnection>> dial;
    private final Consumer<SrtConnection> onConnected;
    private final Runnable onDisconnected;
    private final Consumer<Throwable> onFailed;
    private final long minNanos;
    private final long maxNanos;
    private long waitNanos;
    private volatile boolean closed;
    private volatile SrtConnection connection;
    private ScheduledFuture<?> pending;

    Redialer(BrewContext context, Supplier<CompletableFuture<SrtConnection>> dial,
            Consumer<SrtConnection> onConnected, Runnable onDisconnected, Consumer<Throwable> onFailed) {
        this.context = context;
        this.dial = dial;
        this.onConnected = onConnected;
        this.onDisconnected = onDisconnected;
        this.onFailed = onFailed;
        this.minNanos = context.settings().reconnectMin().toNanos();
        this.maxNanos = context.settings().reconnectMax().toNanos();
        this.waitNanos = minNanos;
    }

    void start() {
        context.dialer().execute(this::attempt);
    }

    SrtConnection connection() {
        return connection;
    }

    void close() {
        closed = true;
        synchronized (this) {
            if (pending != null) {
                pending.cancel(false);
            }
        }
        SrtConnection open = connection;
        if (open != null) {
            open.close();
        }
    }

    private void attempt() {
        if (closed) {
            return;
        }
        CompletableFuture<SrtConnection> attempt;
        try {
            attempt = dial.get();
        } catch (RuntimeException e) {
            onFailed.accept(e);
            retryLater();
            return;
        }
        attempt.whenComplete((opened, failure) -> {
            if (failure != null) {
                if (!closed) {
                    onFailed.accept(failure);
                }
                retryLater();
                return;
            }
            if (closed) {
                opened.close();
                return;
            }
            waitNanos = minNanos;
            connection = opened;
            opened.onClose(() -> {
                connection = null;
                if (!closed) {
                    onDisconnected.run();
                    retryLater();
                }
            });
            onConnected.accept(opened);
        });
    }

    private synchronized void retryLater() {
        if (closed) {
            return;
        }
        pending = context.dialer().schedule(this::attempt, waitNanos, TimeUnit.NANOSECONDS);
        waitNanos = Math.min(maxNanos, waitNanos * 2);
    }
}