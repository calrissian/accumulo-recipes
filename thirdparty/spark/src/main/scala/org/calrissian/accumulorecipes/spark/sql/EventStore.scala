/*
 * Copyright (C) 2014 The Calrissian Authors
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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.{StructType, Row, SQLContext}
import org.apache.spark.rdd.RDD
import org.calrissian.mango.criteria.builder.QueryBuilder
import org.calrissian.accumulorecipes.eventstore.hadoop.EventInputFormat
import org.apache.accumulo.core.security.Authorizations
import org.joda.time.DateTime
import org.calrissian.accumulorecipes.commons.hadoop.{EventWritable, BaseQfdInputFormat}
import org.apache.accumulo.core.data.Key
import org.calrissian.accumulorecipes.eventstore.support.EventKeyValueIndex
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore
import org.calrissian.accumulorecipes.commons.domain.Auths
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.setAsJavaSetConverter
import org.calrissian.mango.types.encoders.AliasConstants._
import org.apache.spark.sql.catalyst.types._
import org.calrissian.mango.domain.event.Event
import java.util.Date
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.GreaterThan
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.LessThanOrEqual
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.hadoop.mapreduce.Job
import org.calrissian.mango.domain.TupleStore

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
 */
class EventStore extends RelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    EventStoreTableScan(parameters("inst"), parameters("zk"), parameters("user"), parameters("pass"),
                        DateTime.parse(parameters("start")), DateTime.parse(parameters("end")), parameters("type"), sqlContext)
  }
}

case class EventStoreTableScan(inst: String, zk: String, user: String, pass: String,
                               start: DateTime, stop: DateTime, eventType: String, @transient val sqlContext: SQLContext) extends PrunedFilteredScan {

  private val instance = new ZooKeeperInstance(inst, zk)
  private val connector = instance.getConnector(user, new PasswordToken(pass))
  private val keys = EventKeyValueIndex.uniqueKeys(connector, AccumuloEventStore.DEFAULT_IDX_TABLE_NAME, "", eventType, 10, new Auths)

  private val internalSchema: StructType = parseSchema()

  override def schema: StructType = internalSchema

  private def parseSchema(): StructType = {
    // turn keys into StructType
    StructType(keys.map(it => (it.getOne, it.getTwo)).collect(_ match {
      case (key, value) if value == INTEGER_ALIAS => StructField(key, IntegerType)
      case (key, value) if value == BOOLEAN_ALIAS => StructField(key, BooleanType)
      case (key, value) if value == BYTE_ALIAS => StructField(key, ByteType)
      case (key, value) if value == DATE_ALIAS => StructField(key, DateType)
      case (key, value) if value == DOUBLE_ALIAS => StructField(key, DoubleType)
      case (key, value) if value == FLOAT_ALIAS => StructField(key, FloatType)
      case (key, value) if value == LONG_ALIAS => StructField(key, LongType)
      case (key, value) if value == STRING_ALIAS => StructField(key, StringType)
    }).toSeq)
  }

  override def buildScan(columns: Array[String], filters: Array[Filter]): RDD[Row] = {

    var andNode = new QueryBuilder().and()

    filters.foreach(it => it match {
      case EqualTo(attr, value) => andNode = andNode.eq(attr, value)
      case GreaterThan(attr, value) => andNode = andNode.greaterThan(attr, value)
      case GreaterThanOrEqual(attr, value) => andNode = andNode.greaterThanEq(attr, value)
      case LessThan(attr, value) => andNode = andNode.lessThan(attr, value)
      case LessThanOrEqual(attr, value) => andNode = andNode.lessThanEq(attr, value)
      case In(attr, values) => andNode = andNode.in(attr, values)
    })

    val conf = sqlContext.sparkContext.hadoopConfiguration
    val job = new Job(conf)
    EventInputFormat.setInputInfo(job, user, pass.getBytes, new Authorizations)
    EventInputFormat.setQueryInfo(job, start.toDate, stop.toDate, Set(eventType), andNode.end.build)
    BaseQfdInputFormat.setSelectFields(conf, columns.toSet.asJava)

    // translate from Event into Row
    sqlContext.sparkContext.newAPIHadoopRDD(conf, classOf[EventInputFormat], classOf[Key], classOf[EventWritable]).map(it => asRow(it._2.get(), schema))
  }

  private def asRow(event: Event, schema: StructType): Row = {
    val row = new TupleStoreRow(schema.fields.length, event)   // Still want to keep the raw event so that we can re-explode any possibly flattened tuples later
    schema.fields.zipWithIndex.foreach {
      case (StructField(name, dataType, _, _), i) =>
        row.update(i, enforceCorrectType(event.get(name).getValue, dataType))
    }

    row
  }

  /**
   * A TupleStoreRow is a wrapper around a GenericMutableRow which applies a given schema (based on the event's type)
   * to the items in an event.
   * @param aSize
   * @param tupleStore
   */
  case class TupleStoreRow(aSize: Int, tupleStore: TupleStore) extends GenericMutableRow(aSize)

  private  def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      desiredType match {
        case StringType => toString(value)
        case _ if value == null || value == "" => null // guard the non string type
        case IntegerType => value.asInstanceOf[IntegerType.type]
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case BooleanType => value.asInstanceOf[BooleanType.type]
        case DateType => toDate(value)
      }
    }
  }


  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
    }
  }

  private def toString(value: Any): String = {
    value match {
      case value => Option(value).map(_.toString).orNull
    }
  }

  private def toDate(value: Any): Date = {
    value match {
      // only support string as date
      case value: java.lang.String => DateTime.parse(value).toDate
      case value: Date => value
    }
  }
}
