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
package org.calrissian.accumulorecipes.commons.hadoop;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Hadoop partitioner that uses ranges, and optionally sub-bins based on hashing. This range partitioner will use mutliple
 * groups to determine the partition, therefore allowing several reducers to represent several different writes to files
 * for different tables. It could be used, for instance, for a multi-table bulk ingest.
 */
public class GroupedKeyRangePartitioner extends Partitioner<GroupedKey, Writable> implements Configurable {
    public static final String DELIM = "\0";
    private static final String PREFIX = GroupedKeyRangePartitioner.class.getName();
    private static final String CUTFILE_KEY = PREFIX + ".cutFile";
    private static final String NUM_SUBBINS = PREFIX + ".subBins";
    private static final String GROUPS_KEY = ".groups";
    private Configuration conf;
    private int _numSubBins = 0;
    private Text cutPointArray[] = null;

    /**
     * Sets the hdfs file name to use, containing a newline separated list of Base64 encoded split points that represent ranges for partitioning
     */
    public static void addSplitFile(JobContext job, String group, String file) {
        URI uri = new Path(file).toUri();
        DistributedCache.addCacheFile(uri, job.getConfiguration());
        String[] groups = job.getConfiguration().getStrings(GROUPS_KEY);
        if (groups == null || Arrays.binarySearch(groups, group) == -1) {
            String[] newGroups = groups != null ? Arrays.copyOf(groups, groups.length + 1) : new String[]{};
            newGroups[newGroups.length - 1] = group;
            job.getConfiguration().setStrings(GROUPS_KEY, newGroups);
            job.getConfiguration().set(GROUPS_KEY + "." + group, file);
        }
    }

    /**
     * Sets the number of random sub-bins per range
     */
    public static void setNumSubBins(JobContext job, int num) {
        job.getConfiguration().setInt(NUM_SUBBINS, num);
    }

    public int getPartition(GroupedKey key, Writable value, int numPartitions) {
        try {
            return findPartition(key.getGroup(), key.getKey().getRow(), getCutPoints(), getNumSubBins());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    int findPartition(String group, Text key, Text[] array, int numSubBins) {

        // find the bin for the range, and guarantee it is positive
        int index = Arrays.binarySearch(array, new Text(group + DELIM + key));
        index = index < 0 ? (index + 1) * -1 : index;

        // both conditions work with numSubBins == 1, but this check is to avoid
        // hashing, when we don't need to, for speed
        if (numSubBins < 2)
            return index;
        return (key.toString().hashCode() & Integer.MAX_VALUE) % numSubBins + index * numSubBins;
    }

    private synchronized int getNumSubBins() {
        if (_numSubBins < 1) {
            // get number of sub-bins and guarantee it is positive
            _numSubBins = Math.max(1, getConf().getInt(NUM_SUBBINS, 1));
        }
        return _numSubBins;
    }

    private synchronized Text[] getCutPoints() throws IOException {
        if (cutPointArray == null) {

            Path[] cf = DistributedCache.getLocalCacheFiles(conf);
            if (cf != null) {
                Map<String, String> curFilesAndGroups = getCurFilesAndGroups();
                SortedMap<String, SortedSet<String>> cutPointMap = new TreeMap<String, SortedSet<String>>();
                for (Path path : cf) {
                    String group = null;
                    for (Map.Entry<String, String> groupSplits : curFilesAndGroups.entrySet()) {
                        if (path.toString().endsWith(groupSplits.getKey()))
                            group = groupSplits.getValue();
                    }


                    if (group != null) {
                        Scanner in = new Scanner(new BufferedReader(new FileReader(path.toString())));

                        try {
                            while (in.hasNextLine()) {
                                String split = new String(Base64.decodeBase64(in.nextLine().getBytes()));

                                SortedSet<String> splits = cutPointMap.get(group);
                                if (splits == null) {
                                    splits = new TreeSet<String>();
                                    cutPointMap.put(group, splits);
                                }
                            }

                            SortedSet<Text> treeSet = new TreeSet<Text>();
                            for (Map.Entry<String, SortedSet<String>> entry : cutPointMap.entrySet()) {
                                treeSet.add(new Text(entry.getKey() + DELIM + DELIM));

                                for (String string : entry.getValue())
                                    treeSet.add(new Text(entry.getKey() + DELIM + string));

                                treeSet.add(new Text(entry.getKey() + DELIM + "\uffff"));
                            }

                            cutPointArray = treeSet.toArray(new Text[]{});
                        } finally {
                            in.close();
                        }

                        break;
                    } else {
                        throw new FileNotFoundException("A file was not found in distribution cache files: " + path.toString());
                    }
                }
            }
        }
        return cutPointArray;
    }

    private Map<String, String> getCurFilesAndGroups() {
        Map<String, String> retMap = new TreeMap<String, String>();
        String[] groups = conf.getStrings(GROUPS_KEY);
        for (String group : groups)
            retMap.put(conf.get(GROUPS_KEY + "." + group), group);

        return retMap;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
