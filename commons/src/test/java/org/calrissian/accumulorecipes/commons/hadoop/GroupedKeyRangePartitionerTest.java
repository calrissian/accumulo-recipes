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
package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class GroupedKeyRangePartitionerTest {

    private static Text[] cutArray = new Text[]{new Text("a\u0000\u0000"), new Text("a\u0000A"), new Text("a\u0000B"),
            new Text("a\u0000\uffff"), new Text("b\u0000\u0000"), new Text("b\u0000C"),
            new Text("b\u0000\uffff")};

    @Test
    public void testNoSubBins() {
        for (int i = -2; i < 2; ++i) {
            checkExpectedBins("a", i, new String[]{"A", "B", "C"}, new int[]{1, 2, 3});
            checkExpectedBins("b", i, new String[]{"C", "A", "B"}, new int[]{5, 5, 5});
            checkExpectedBins("a", i, new String[]{"", "AA", "BB", "CC"}, new int[]{0, 2, 3, 3});
        }
    }

    @Test
    public void testSubBins() {
        checkExpectedRangeBins("a", 2, new String[]{"A", "B", "C"}, new int[]{3, 5, 7});
        checkExpectedRangeBins("a", 2, new String[]{"C", "A", "B"}, new int[]{7, 3, 5});
        checkExpectedRangeBins("a", 2, new String[]{"", "AA", "BB", "CC"}, new int[]{1, 5, 6, 7});

        checkExpectedRangeBins("b", 3, new String[]{"A", "B", "C"}, new int[]{17, 17, 17});
        checkExpectedRangeBins("b", 3, new String[]{"C", "A", "B"}, new int[]{17, 17, 17});
        checkExpectedRangeBins("b", 3, new String[]{"", "AA", "BB", "CC"}, new int[]{14, 17, 17, 20});

        checkExpectedRangeBins("a", 10, new String[]{"A", "B", "C"}, new int[]{19, 29, 39});
        checkExpectedRangeBins("a", 10, new String[]{"C", "A", "B"}, new int[]{39, 19, 29});
        checkExpectedRangeBins("a", 10, new String[]{"", "AA", "BB", "CC"}, new int[]{9, 29, 39, 39});

    }

    private GroupedKeyRangePartitioner prepPartitioner(int numSubBins) {
        JobContext job = new JobContext(new Configuration(), new JobID());
        GroupedKeyRangePartitioner.setNumSubBins(job, numSubBins);
        GroupedKeyRangePartitioner rp = new GroupedKeyRangePartitioner();
        rp.setConf(job.getConfiguration());
        return rp;
    }

    private void checkExpectedRangeBins(String group, int numSubBins, String[] strings, int[] rangeEnds) {
        assertTrue(strings.length == rangeEnds.length);
        for (int i = 0; i < strings.length; i++) {
            int endRange = rangeEnds[i];
            int startRange = endRange + 1 - numSubBins;
            int part = prepPartitioner(numSubBins).findPartition(group, new Text(strings[i]), cutArray, numSubBins);
            assertTrue(part >= startRange);
            assertTrue(part <= endRange);
        }
    }

    private void checkExpectedBins(String group, int numSubBins, String[] strings, int[] bins) {
        assertTrue(strings.length == bins.length);
        for (int i = 0; i < strings.length; i++) {
            int bin = bins[i], part = prepPartitioner(numSubBins).findPartition(group, new Text(strings[i]), cutArray, numSubBins);
            assertTrue(bin == part);
        }
    }
}
