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
package org.calrissian.accumulorecipes.rangestore.support;

/**
 * Specifies a bounded range of values for a given type
 */
public class ValueRange<T> {

    private T start;
    private T stop;

    public ValueRange(T start, T stop) {
        this.start = start;
        this.stop = stop;
    }

    public ValueRange() {
        start = null;
        stop = null;
    }

    public T getStart() {
        return start;
    }

    public void setStart(T start) {
        this.start = start;
    }

    public T getStop() {
        return stop;
    }

    public void setStop(T stop) {
        this.stop = stop;
    }
}
