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

/**
 * One connection's transport statistics, in Barista's own vocabulary: a record
 * per transport, each carrying only what that transport measures. Nothing here
 * exposes a Roast or Press type, so consumers do not depend on either.
 */
public sealed interface TransportStats permits SrtStats, RtpReceiveStats, RtpSendStats {
}