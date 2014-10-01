/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.featurestore.support;

import static org.apache.commons.lang.StringUtils.join;
import static org.calrissian.accumulorecipes.commons.support.Constants.NULL_BYTE;

public class Utilities {
    private Utilities() {
    }

    public static String combine(String... items) {
        if (items == null)
            return null;
        return join(items, NULL_BYTE);
    }
}
