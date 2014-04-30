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
package org.calrissian.accumulorecipes.commons.domain;

public class StoreConfig {

    private final int maxQueryThreads;

    private final long maxMemory;
    private final long maxLatency;
    private final int maxWriteThreads;

    /**
     * Default config that defaults the all the store config values.
     */
    public StoreConfig() {
        this(1);
    }

    /**
     * Query only config that will default any writes to one thread and 10000 max memory.
     *
     * @param maxQueryThreads the number of concurrent threads to spawn for querying
     */
    public StoreConfig(int maxQueryThreads) {
        this(maxQueryThreads, 10000, -1, 1);
    }

    /**
     * Write only constructor that will default all batch scans to a single thread.
     *
     * @param maxMemory size in bytes of the maximum memory to batch before writing
     * @param maxLatency time in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a batch before writing
     * @param maxWriteThreads the maximum number of threads to use for writing data to the tablet servers
     */
    public StoreConfig(long maxMemory, long maxLatency, int maxWriteThreads) {
        this(1, maxMemory, maxLatency, maxWriteThreads);
    }

    /**
     * Allows the user to set the criteria and write options for the store.
     *
     * @param maxQueryThreads
     * @param maxMemory size in bytes of the maximum memory to batch before writing
     * @param maxLatency time in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time to hold a batch before writing
     * @param maxWriteThreads the maximum number of threads to use for writing data to the tablet servers
     */
    public StoreConfig(int maxQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads) {
        this.maxQueryThreads = maxQueryThreads;
        this.maxMemory = maxMemory;
        this.maxLatency = maxLatency;
        this.maxWriteThreads = maxWriteThreads;
    }

    /**
     * The number of concurrent threads to spawn for querying
     */
    public int getMaxQueryThreads() {
        return maxQueryThreads;
    }

    /**
     * The size in bytes of the maximum memory to batch before writing
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * time in milliseconds to hold a batch before writing.  If set to 0 or Long.MAX_VALUE it will use the maximum time
     */
    public long getMaxLatency() {
        return maxLatency;
    }

    /**
     * The maximum number of threads to use for writing data to the tablet servers
     * @return
     */
    public int getMaxWriteThreads() {
        return maxWriteThreads;
    }
}