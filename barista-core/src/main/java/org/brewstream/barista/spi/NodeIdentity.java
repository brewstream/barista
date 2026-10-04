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

package org.brewstream.barista.spi;

/**
 * Who this Barista is. Configured, never derived from the hostname or an IP
 * address, which containers and DHCP change: stored brews are keyed by it.
 */
public interface NodeIdentity {

    /** Stable for the life of the node, across restarts. */
    String nodeId();

    /**
     * The host callers should dial for this node's listening endpoints. Behind
     * NAT or a cloud public IP this differs from any local address.
     */
    String publishedHost();
}