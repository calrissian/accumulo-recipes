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

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.calrissian.accumulorecipes.commons.domain.Auths
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat
import org.calrissian.accumulorecipes.entitystore.hadoop.EntityInputFormat
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable
import org.calrissian.mango.collect.CloseableIterable
import org.calrissian.mango.criteria.domain.Node
import org.calrissian.mango.domain.Pair
import org.calrissian.mango.domain.entity.Entity

import scala.collection.JavaConversions._

/**
 * A RelationProvider allowing the {@link EntityStore} to be integrated directly into SparkSQL.
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
 *  type  'entityType'
 * )
 */
class EntityStore extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    EntityStoreScan(parameters("inst"), parameters("zk"), parameters("user"), parameters("pass"),
      parameters("type"), sqlContext)
  }
}

case class EntityStoreScan(inst: String, zk: String, user: String, pass: String,
                           entityType: String,
                           @transient val innerSqlContext: SQLContext) extends QfdScan(inst, zk, user, pass, entityType, innerSqlContext) {
  override type T = Entity
  override type V = EntityWritable
  override type I = EntityInputFormat

  override def buildRDD(columns: Array[String], filters: Array[Filter], query: Node): RDD[T] = {
    val conf = sqlContext.sparkContext.hadoopConfiguration
    val job = new Job(conf)
    EntityInputFormat.setInputInfo(job, user, pass.getBytes, new Authorizations)
    EntityInputFormat.setZooKeeperInstanceInfo(job, inst, zk)
    if(filters.size > 0)
      EntityInputFormat.setQueryInfo(job, Set(entityType), query)
    else
      EntityInputFormat.setQueryInfo(job, Set(entityType))
    BaseQfdInputFormat.setSelectFields(job.getConfiguration, setAsJavaSet(columns.toSet))

    sqlContext.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[I], classOf[Key], classOf[V])
      .map(_._2.get())
  }

  override def uniqueKeys(connector: Connector): CloseableIterable[Pair[String, String]] = new AccumuloEntityStore(connector).keys(entityType, new Auths)
}
