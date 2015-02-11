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
package org.calrissian.accumulorecipes.spark.sql

import java.util.Date

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.calrissian.accumulorecipes.commons.domain.Auths
import org.calrissian.accumulorecipes.commons.hadoop.{BaseQfdInputFormat, EventWritable}
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore
import org.calrissian.accumulorecipes.eventstore.support.EventKeyValueIndex
import org.calrissian.mango.collect.CloseableIterable
import org.calrissian.mango.criteria.domain.Node
import org.calrissian.mango.domain.Pair
import org.calrissian.mango.domain.event.Event
import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
 * A RelationProvider allowing the {@link EventStore} to be integrated directly into SparkSQL.
 * Some lightweight setup needs to be done in order to properly wire up the input format which
 * will ultimately be used
 *
 * CREATE TEMPORARY TABLE events
 * USING org.calrissian.accumulorecipes.spark.EventStore
 * OPTIONS (
 *  inst  'instanceName',
 *  zk    'zookeepers',
 *  user  'username',
 *  pass  'password',
 *  start '2014-01-02',
 *  end   '2014-01-15',
 *  type  'eventType'
 * )
 **/
class EventStore extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new EventStoreScan(parameters("inst"), parameters("zk"), parameters("user"), parameters("pass"),
                        DateTime.parse(parameters("start")), DateTime.parse(parameters("end")), parameters("type"), sqlContext)
  }
}

class EventStoreScan(inst: String, zk: String, user: String, pass: String,
                               start: DateTime, stop: DateTime, eventType: String,
                               @transient val innerSqlContext: SQLContext) extends QfdScan(inst, zk, user, pass, eventType, innerSqlContext) {

  type T = Event
  type V = EventWritable
  type I = EventInputFormat

  override def uniqueKeys(connector: Connector): CloseableIterable[Pair[String, String]] =
    EventKeyValueIndex.uniqueKeys(connector, AccumuloEventStore.DEFAULT_IDX_TABLE_NAME, "", eventType, 10, new Auths)


  override def buildRDD(columns: Array[String], filters: Array[Filter], query: Node): RDD[T] = {
    val conf = sqlContext.sparkContext.hadoopConfiguration
    val job = new Job(conf)
    EventInputFormat.setInputInfo(job, user, pass.getBytes, new Authorizations)
    EventInputFormat.setZooKeeperInstanceInfo(job, inst, zk)
    if(filters.size > 0)
      EventInputFormat.setQueryInfo(job, start.toDate, stop.toDate, Set(eventType), query)
    else
      EventInputFormat.setQueryInfo(job, start.toDate, stop.toDate, Set(eventType))
    BaseQfdInputFormat.setSelectFields(job.getConfiguration, setAsJavaSet(columns.toSet))

    sqlContext.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[I], classOf[Key], classOf[V])
      .map(_._2.get())
  }

}

