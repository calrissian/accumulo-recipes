/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.eventstore.support;


import java.text.SimpleDateFormat;
import java.util.Date;

public class Shard {

    protected final Integer numPartitions;

    protected String delimiter = "_";

    /**
     * A proper date format should be lexicographically sortable
     */
    protected String dateFormat = "yyyyMMddhh";

    public Shard(Integer numPartitions) {
        this.numPartitions = numPartitions;
    }

    public String buildShard(long timestamp, String uuid) {

        int partitionWidth = String.valueOf(numPartitions).length();
        Date date = new Date(timestamp);
        return String.format("%s%s%0" + partitionWidth + "d", new SimpleDateFormat(dateFormat).format(date),
                delimiter, (Math.abs(uuid.hashCode()) % numPartitions));
    }

    public String[] getRange(Date start, Date end) {

        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);

        return new String[] { sdf.format(start), sdf.format(end)};
    }

    public Integer getNumPartitions() {
        return numPartitions;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
}
