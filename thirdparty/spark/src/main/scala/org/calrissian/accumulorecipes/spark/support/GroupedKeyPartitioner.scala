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

import java.util.{Collections, List => JList}

import org.apache.spark.Partitioner
import org.calrissian.accumulorecipes.commons.hadoop.GroupedKey
import org.calrissian.accumulorecipes.commons.support.Constants

import scala.collection.JavaConversions._
import scala.collection.{SortedMap, SortedSet}

/**
 * A partitioner for Spark to allow an RDD[(GroupedKey, Value)] to be partitioned both by
 * group (table name) and into buckets defined by the row id of the key. The buckets, or splits,
 * for each table are passed into the constructor of this class. The buckets for each table are
 * bounded on upper and lower sides to make sure that partitions for a row never cross group
 * boundaries (e.g. a key meant for 'table 1' never gets placed on a partition for 'table 2' just
 * because a key had a row id which was larger than the highest split point set for that table.
 * @param groupsAndSplits
 */
class GroupedKeyPartitioner(groupsAndSplits: SortedMap[String, SortedSet[String]]) extends Partitioner {

  class SearchableSeq[T](a: Seq[T])(implicit ordering: Ordering[T]) {
    val list: JList[T] = a.toList
    def binarySearch(key: T): Int = Collections.binarySearch(list, key, ordering)
  }
  implicit def seqToSearchable[T](a: Seq[T])(implicit ordering: Ordering[T]) =
    new SearchableSeq(a)(ordering)

  private lazy val internalSplits: Seq[String] = groupsAndSplits.flatMap(it =>
    (Set("\u0000", "\uffff") ++ it._2).toSeq.sorted.map(it._1 + Constants.NULL_BYTE + _)
  ).toSeq

  private lazy val numSplits = internalSplits.size

  println(internalSplits)

  override def numPartitions: Int = numPartitions

  override def getPartition(key: Any): Int = {

    key match {
      case gk: GroupedKey => {
        var index = internalSplits.binarySearch(gk.getGroup + Constants.NULL_BYTE + gk.getKey.getRow.toString)
        index = if (index < 0) (index + 1) * -1 else index
        index
      }
      case _ => throw new IllegalArgumentException(s"Expected key type was ${classOf[GroupedKey]} but was ${key.getClass} instead")
    }
  }
}


