/*
 * Copyright (C) 2015 The Calrissian Authors
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
package org.calrissian.accumulorecipes.spark.support

import org.apache.accumulo.core.data.Key
import org.calrissian.accumulorecipes.commons.hadoop.GroupedKey
import org.junit.{Assert, Test}

import scala.collection.{SortedSet, SortedMap}

class GroupedKeyPartitionerTest {

  @Test
  def test = {

    val testData = SortedMap(
      ("table1", SortedSet("1", "4", "5")),
      ("table2", SortedSet("1", "4", "5"))
    )

    val partitioner = new GroupedKeyPartitioner(testData)

    Assert.assertEquals(1, partitioner.getPartition(new GroupedKey("table1", new Key("0"))))
    Assert.assertEquals(1, partitioner.getPartition(new GroupedKey("table1", new Key("1"))))
    Assert.assertEquals(2, partitioner.getPartition(new GroupedKey("table1", new Key("3"))))
    Assert.assertEquals(2, partitioner.getPartition(new GroupedKey("table1", new Key("4"))))
    Assert.assertEquals(3, partitioner.getPartition(new GroupedKey("table1", new Key("5"))))
    Assert.assertEquals(4, partitioner.getPartition(new GroupedKey("table1", new Key("6"))))
    Assert.assertEquals(6, partitioner.getPartition(new GroupedKey("table2", new Key("0"))))
    Assert.assertEquals(6, partitioner.getPartition(new GroupedKey("table2", new Key("1"))))
    Assert.assertEquals(7, partitioner.getPartition(new GroupedKey("table2", new Key("3"))))
    Assert.assertEquals(7, partitioner.getPartition(new GroupedKey("table2", new Key("4"))))
    Assert.assertEquals(8, partitioner.getPartition(new GroupedKey("table2", new Key("5"))))
    Assert.assertEquals(9, partitioner.getPartition(new GroupedKey("table2", new Key("6"))))
    Assert.assertEquals(9, partitioner.getPartition(new GroupedKey("table2", new Key("500"))))
  }
}
