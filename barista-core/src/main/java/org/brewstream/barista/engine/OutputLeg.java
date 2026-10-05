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
import io.netty.channel.EventLoop;
import org.brewstream.barista.spec.OutputSpec;

/** An output: is offered every chunk of the active source, and never makes the source wait. */
abstract class OutputLeg extends Leg {

    final OutputSpec spec;
    protected final EventLoop loop;
    protected final LegMonitor monitor = new LegMonitor(false);

    OutputLeg(OutputSpec spec, String kind, BrewContext context) {
        super(spec.id().value(), kind, context, false);
        this.spec = spec;
        this.loop = context.nextOutputLoop();
    }

    /** Takes ownership of {@code chunk}. Called on the brew loop; must not wait. */
    abstract void offer(ByteBuf chunk);

    /** How many bytes this output may hold for a slow peer. */
    abstract void capacity(long bytes);
}