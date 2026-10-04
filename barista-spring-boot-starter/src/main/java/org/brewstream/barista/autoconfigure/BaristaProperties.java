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

package org.brewstream.barista.autoconfigure;

import org.brewstream.barista.support.PortRange;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * {@code barista.*} configuration.
 *
 * @param node              who this node is
 * @param ports             where listening endpoints get their ports
 * @param eventLoopThreads  threads in the shared event loop group; 0 for Netty's default
 *                          (twice the processors)
 * @param queueTime         how much input each output may hold for a slow peer
 * @param sourceLossTimeout silence after which the active source counts as lost
 */
@ConfigurationProperties("barista")
public record BaristaProperties(
        @DefaultValue Node node,
        @DefaultValue Ports ports,
        @DefaultValue("0") int eventLoopThreads,
        @DefaultValue("2s") Duration queueTime,
        @DefaultValue("2s") Duration sourceLossTimeout) {

    /**
     * @param id            stable for the life of the node; stored brews are keyed by it.
     *                      Set it: the default suits one node on one machine only
     * @param publishedHost the host callers dial for this node's listening endpoints
     */
    public record Node(@DefaultValue("barista") String id, @DefaultValue("127.0.0.1") String publishedHost) {
    }

    /**
     * Written as ranges, {@code srt: 9000-9099}.
     *
     * @param srt single ports for SRT listeners
     * @param rtp ports for RTP blocks: P to P+4, bases ten apart
     */
    public record Ports(
            @DefaultValue("9000-9099") PortRange srt,
            @DefaultValue("5000-5999") PortRange rtp) {
    }
}