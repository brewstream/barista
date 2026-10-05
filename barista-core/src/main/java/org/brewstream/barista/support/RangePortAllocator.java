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
import java.util.HashSet;
import java.util.Set;

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
    /** Ports held by fixed reservations outside the ranges. */
    private final Set<Integer> fixed = new HashSet<>();

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
            for (int port = srt.first(); port <= srt.last(); port++) {
                if (!occupied(port)) {
                    srtUsed.set(port - srt.first());
                    return port;
                }
            }
            throw new IllegalStateException("no free SRT port in " + srt);
        }
        for (int index = 0; rtp.first() + index * RTP_BLOCK_STEP + 4 <= rtp.last(); index++) {
            int base = rtp.first() + index * RTP_BLOCK_STEP;
            if (!anyOccupied(base, base + 4)) {
                rtpUsed.set(index);
                return base;
            }
        }
        throw new IllegalStateException("no free RTP block in " + rtp);
    }

    /**
     * Takes a specific port, or for RTP the block P to P+4, wherever it is.
     * Inside the ranges it is marked like an allocation; outside them it is
     * remembered all the same, because SRT and RTP share one UDP port space and a
     * port named twice fails at bind time otherwise. Any overlap with a port
     * already taken, of either kind, is refused.
     */
    @Override
    public synchronized void reserve(PortKind kind, int port) {
        int last = kind == PortKind.SRT ? port : port + 4;
        if (kind == PortKind.RTP_BLOCK && rtp.contains(port) && (port - rtp.first()) % RTP_BLOCK_STEP != 0) {
            throw new IllegalStateException("RTP port " + port + " is not a block base in " + rtp);
        }
        if (anyOccupied(port, last)) {
            throw new IllegalStateException(describe(kind, port) + " overlaps a port already taken");
        }
        if (kind == PortKind.SRT && srt.contains(port)) {
            srtUsed.set(port - srt.first());
        } else if (kind == PortKind.RTP_BLOCK && rtp.contains(port)) {
            rtpUsed.set((port - rtp.first()) / RTP_BLOCK_STEP);
        } else {
            for (int p = port; p <= last; p++) {
                fixed.add(p);
            }
        }
    }

    @Override
    public synchronized void release(PortKind kind, int port) {
        if (kind == PortKind.SRT && srt.contains(port)) {
            srtUsed.clear(port - srt.first());
        } else if (kind == PortKind.RTP_BLOCK && rtp.contains(port) && (port - rtp.first()) % RTP_BLOCK_STEP == 0) {
            rtpUsed.clear((port - rtp.first()) / RTP_BLOCK_STEP);
        } else {
            int last = kind == PortKind.SRT ? port : port + 4;
            for (int p = port; p <= last; p++) {
                fixed.remove(p);
            }
        }
    }

    /** Whether a port is taken by anything this allocator knows of, of either kind. */
    private boolean occupied(int port) {
        if (fixed.contains(port)) {
            return true;
        }
        if (srt.contains(port) && srtUsed.get(port - srt.first())) {
            return true;
        }
        if (rtp.contains(port)) {
            int index = (port - rtp.first()) / RTP_BLOCK_STEP;
            int base = rtp.first() + index * RTP_BLOCK_STEP;
            return port <= base + 4 && rtpUsed.get(index);
        }
        return false;
    }

    private boolean anyOccupied(int first, int last) {
        for (int port = first; port <= last; port++) {
            if (occupied(port)) {
                return true;
            }
        }
        return false;
    }

    private static String describe(PortKind kind, int port) {
        return kind == PortKind.SRT ? "SRT port " + port : "RTP block " + port + "-" + (port + 4);
    }
}
