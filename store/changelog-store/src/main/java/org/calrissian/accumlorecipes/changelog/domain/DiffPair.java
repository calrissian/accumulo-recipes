/*
 * Copyright (C) 2013 The Calrissian Authors
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
package org.calrissian.accumlorecipes.changelog.domain;

import org.calrissian.mango.hash.tree.HashLeaf;

public class DiffPair {

    HashLeaf minus;
    HashLeaf plus;

    public DiffPair(HashLeaf minus, HashLeaf plus) {

        this.minus = minus;
        this.plus = plus;
    }

    @Override
    public String toString() {
        return "{-" + minus + ", +" + plus + "}";
    }
}
