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

import java.util.Arrays;

/**
 * The latest self-contained keyframe of a brew's active source: its video's
 * parameter sets and one complete IDR picture, in Annex B form. Nothing is decoded
 * in Barista; hand {@code data} and {@code codec} to a decoder, such as a browser's
 * WebCodecs {@code VideoDecoder}, and it produces one frame on its own.
 *
 * @param codec            the RFC 6381 codec string, e.g. {@code avc1.64000c}
 * @param capturedAtMillis when Barista captured it (epoch milliseconds)
 * @param data             parameter sets, then the IDR's NAL units, with start codes. Shared,
 *                         not copied: do not modify it
 */
public record BrewKeyframe(String codec, long capturedAtMillis, byte[] data) {

    @Override
    public boolean equals(Object other) {
        return other instanceof BrewKeyframe that && capturedAtMillis == that.capturedAtMillis
                && codec.equals(that.codec) && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return 31 * codec.hashCode() + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "BrewKeyframe[codec=" + codec + ", capturedAtMillis=" + capturedAtMillis + ", " + data.length
                + " bytes]";
    }
}
