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

package org.brewstream.barista;

/** Valid TS packets on one PID with continuity counters in order, each payload distinct. */
public final class TsFixtures {

    private TsFixtures() {
    }

    /** {@code count} packets, starting from packet number {@code first}. */
    public static byte[] packets(int first, int count) {
        byte[] ts = new byte[count * 188];
        for (int i = 0; i < count; i++) {
            int n = first + i;
            int at = i * 188;
            ts[at] = 0x47;
            ts[at + 1] = 0x01;              // PID 0x100
            ts[at + 2] = 0x00;
            ts[at + 3] = (byte) (0x10 | (n & 0x0F)); // payload only, continuity counter
            for (int j = 4; j < 188; j++) {
                ts[at + j] = (byte) (n * 7 + j);
            }
        }
        return ts;
    }
}