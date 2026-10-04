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

package org.brewstream.barista.spec;

import java.util.Objects;

/**
 * SRT encryption: a passphrase and an AES key length. Stored in the spec as it
 * is given; a repository that persists specs decides how to protect it.
 *
 * @param keyLength 16, 24 or 32 bytes
 */
public record SrtSecurity(String passphrase, int keyLength) {

    public SrtSecurity {
        Objects.requireNonNull(passphrase, "passphrase");
        if (passphrase.length() < 10 || passphrase.length() > 79) {
            throw new IllegalArgumentException("an SRT passphrase is 10 to 79 characters");
        }
        if (keyLength != 16 && keyLength != 24 && keyLength != 32) {
            throw new IllegalArgumentException("key length must be 16, 24 or 32 bytes, not " + keyLength);
        }
    }

    @Override
    public String toString() {
        return "SrtSecurity[keyLength=" + keyLength + "]"; // never the passphrase
    }
}