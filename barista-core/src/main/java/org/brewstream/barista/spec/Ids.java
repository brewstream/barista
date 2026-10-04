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

import java.util.regex.Pattern;

/** Validation shared by the identifier types. */
final class Ids {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private Ids() {
    }

    static String check(String kind, String value) {
        if (value == null || !VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(kind + " must be 1-64 characters of letters, digits, '.', '_' or '-': "
                    + value);
        }
        return value;
    }
}