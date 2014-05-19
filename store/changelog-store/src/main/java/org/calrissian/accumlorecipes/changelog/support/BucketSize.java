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
package org.calrissian.accumlorecipes.changelog.support;

public enum BucketSize {

    FIVE_MINS(5 * 1000 * 60),
    TEN_MINS(10 * 1000 * 60),
    FIFTEEN_MINS(15 * 1000 * 60),
    HALF_HOUR(30 * 1000 * 60),
    ONE_HOUR(60 * 1000 * 60),
    DAY(60 * 1000 * 60 * 24);

    private long ms;

    private BucketSize(long ms) {
        this.ms = ms;
    }

    public long getMs() {
        return ms;
    }
}
