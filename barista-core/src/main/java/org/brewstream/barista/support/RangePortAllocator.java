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

package org.brewstream.barista.support;

import org.brewstream.barista.spi.PortAllocator;
import org.brewstream.barista.spi.PortKind;

import java.util.BitSet;

/**
 * Allocates from two configured ranges: single ports for SRT, and blocks of five
 * (P to P+4) for RTP, whose bases step by {@link #RTP_BLOCK_STEP} so a block's
 * ports read as one group (5000, 5010, 5020...). Lowest free first, so a node's
 * addresses are predictable.
 */
public final class RangePortAllocator implements PortAllocator {

    /** Distance between RTP block bases. Ten keeps P to P+4 inside one tidy group. */
    public static final int RTP_BLOCK_STEP = 10;

    private final PortRange srt;
    private final PortRange rtp;
    private final BitSet srtUsed = new BitSet();
    private final BitSet rtpUsed = new BitSet();

    /**
     * @param srt single ports for SRT listeners
     * @param rtp ports for RTP blocks: bases from its first port, each block's P+4 within it
     */
    public RangePortAllocator(PortRange srt, PortRange rtp) {
        if (rtp.last() - rtp.first() < 4) {
            throw new IllegalArgumentException("RTP range " + rtp + " cannot hold one block of five ports");
        }
        if (srt.overlaps(rtp)) {
            throw new IllegalArgumentException("SRT range " + srt + " overlaps RTP range " + rtp);
        }
        this.srt = srt;
        this.rtp = rtp;
    }

    @Override
    public synchronized int allocate(PortKind kind) {
        if (kind == PortKind.SRT) {
            int index = srtUsed.nextClearBit(0);
            if (srt.first() + index > srt.last()) {
                throw new IllegalStateException("no free SRT port in " + srt);
            }
            srtUsed.set(index);
            return srt.first() + index;
        }
        for (int index = 0; rtp.first() + index * RTP_BLOCK_STEP + 4 <= rtp.last(); index++) {
            if (!rtpUsed.get(index)) {
                rtpUsed.set(index);
                return rtp.first() + index * RTP_BLOCK_STEP;
            }
        }
        throw new IllegalStateException("no free RTP block in " + rtp);
    }

    @Override
    public synchronized void reserve(PortKind kind, int port) {
        if (kind == PortKind.SRT) {
            if (srt.contains(port)) {
                if (srtUsed.get(port - srt.first())) {
                    throw new IllegalStateException("SRT port " + port + " is already taken");
                }
                srtUsed.set(port - srt.first());
            }
            return; // outside the range: not ours to track
        }
        if (rtp.contains(port)) {
            if ((port - rtp.first()) % RTP_BLOCK_STEP != 0) {
                throw new IllegalStateException("RTP port " + port + " is not a block base in this range");
            }
            int index = (port - rtp.first()) / RTP_BLOCK_STEP;
            if (rtpUsed.get(index)) {
                throw new IllegalStateException("RTP block " + port + " is already taken");
            }
            rtpUsed.set(index);
        }
    }

    @Override
    public synchronized void release(PortKind kind, int port) {
        if (kind == PortKind.SRT) {
            if (srt.contains(port)) {
                srtUsed.clear(port - srt.first());
            }
        } else if (rtp.contains(port) && (port - rtp.first()) % RTP_BLOCK_STEP == 0) {
            rtpUsed.clear((port - rtp.first()) / RTP_BLOCK_STEP);
        }
    }
}