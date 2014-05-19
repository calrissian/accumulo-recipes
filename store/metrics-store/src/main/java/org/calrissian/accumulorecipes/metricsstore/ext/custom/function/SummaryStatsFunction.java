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
package org.calrissian.accumulorecipes.metricsstore.ext.custom.function;


import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SummaryStatsFunction implements MetricFunction<SummaryStatistics> {

    SummaryStatistics stats;

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        stats = new SummaryStatistics();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(long value) {
        stats.addValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void merge(SummaryStatistics value) {
        //This can be a problem if the data that we are aggregating is spread out across tablet servers.
        throw new UnsupportedOperationException("Can't merge data for normal dist");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream byteArrStream = new ByteArrayOutputStream();
            ObjectOutputStream oStream = new ObjectOutputStream(byteArrStream);
            oStream.writeObject(stats);
            oStream.close();
            byteArrStream.close();

            return byteArrStream.toByteArray();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SummaryStatistics deserialize(byte[] data) {
        try {

            ByteArrayInputStream byteArrStream = new ByteArrayInputStream(data);
            ObjectInputStream istream = new ObjectInputStream(byteArrStream);
            SummaryStatistics retVal = (SummaryStatistics) istream.readObject();
            istream.close();
            byteArrStream.close();
            return retVal;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
