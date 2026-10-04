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

import org.brewstream.barista.EndpointState;
import org.brewstream.barista.EndpointStatus;

/** One source or output of a brew. */
abstract class Leg {

    protected final String id;
    protected final String kind;
    protected final BrewContext context;
    private volatile EndpointState state = EndpointState.STARTING;
    protected volatile String error;

    Leg(String id, String kind, BrewContext context) {
        this.id = id;
        this.kind = kind;
        this.context = context;
    }

    /** Binds and connects. Runs on the management thread and may wait. */
    abstract void open() throws Exception;

    /** Stops for good. Runs on the management thread and may wait for ports to be released. */
    abstract void close();

    abstract EndpointStatus status();

    EndpointState state() {
        return state;
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
        state(EndpointState.FAILED);
    }
}