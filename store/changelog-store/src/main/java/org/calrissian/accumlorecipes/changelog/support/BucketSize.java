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

    ONE_MIN(60000),
    FIVE_MINS(300000),
    TEN_MINS(600000),
    FIFTEEN_MINS(900000),
    HALF_HOUR(900000 * 2),
    ONE_HOUR(900000 * 4),
    DAY(900000 * 4 * 24);

    private long ms;
    private BucketSize(long ms) {
        this.ms = ms;
    }

    public long getMs() {
        return ms;
    }
}
