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

import org.brewstream.barista.EndpointEvent;
import org.brewstream.barista.EndpointHealth;
import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;

/** One source or output of a brew. */
abstract class Leg {

    protected final String id;
    protected final String kind;
    protected final BrewContext context;
    private volatile EndpointState state = EndpointState.STARTING;
    protected volatile String error;
    protected final EventHistory history = new EventHistory();
    private final HealthRule health;

    Leg(String id, String kind, BrewContext context, boolean source) {
        this.id = id;
        this.kind = kind;
        this.context = context;
        this.health = new HealthRule(source);
    }

    /** Binds and connects. Runs on the management thread and may wait. */
    abstract void open() throws Exception;

    /** Stops for good. Runs on the management thread and may wait for ports to be released. */
    abstract void close();

    abstract EndpointStatus status();

    EndpointState state() {
        return state;
    }

    /** The health word for the status. Safe from any thread. */
    final EndpointHealth health() {
        return health.classify(state);
    }

    /** Judges the window just ended. Call on the brew loop, once per health window. */
    final void sampleHealth() {
        health.sample(status(), context.settings().degradedLossPercent());
    }

    final void state(EndpointState next) {
        EndpointState previous = state;
        if (previous != next) {
            state = next;
            context.endpointStateChanged(id, previous, next);
        }
    }

    final void fail(Throwable cause) {
        error = cause.getMessage() != null ? cause.getMessage() : cause.toString();
        event(EndpointEvent.Kind.FAILED, Reasons.openFailed(cause));
        state(EndpointState.FAILED);
    }

    /** Records what happened and why, and tells listeners. Safe from any thread. */
    final void event(EndpointEvent.Kind kind, String reason) {
        context.endpointEvent(id, history.add(kind, reason));
    }
}