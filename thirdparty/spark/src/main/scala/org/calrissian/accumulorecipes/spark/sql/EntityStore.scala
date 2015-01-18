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

import java.util.Date

import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.{StructField, _}
import org.apache.spark.sql.sources.{EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, _}
import org.apache.spark.sql.{Row, SQLContext, StructType}
import org.calrissian.accumulorecipes.commons.domain.Auths
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat
import org.calrissian.accumulorecipes.entitystore.hadoop.EntityInputFormat
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore
import org.calrissian.accumulorecipes.entitystore.model.EntityWritable
import org.calrissian.mango.criteria.builder.QueryBuilder
import org.calrissian.mango.domain.entity.Entity
import org.calrissian.mango.types.encoders.AliasConstants._
import org.joda.time.DateTime

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
                           @transient val sqlContext: SQLContext) extends PrunedFilteredScan {
  val internalSchema: StructType = parseSchema()
  override def schema: StructType = internalSchema

  private def parseSchema(): StructType = {

    val instance = new ZooKeeperInstance(inst, zk)
    val connector = instance.getConnector(user, new PasswordToken(pass))

    val keyValueIndex = new AccumuloEntityStore(connector).keys(entityType, new Auths)
    val keys = keyValueIndex.toList

    keyValueIndex.closeQuietly()

    val schema = StructType(keys.map(it => (it.getOne, it.getTwo)).collect {
      case (key: String, value) if value == INTEGER_ALIAS => StructField(key, IntegerType)
      case (key: String, value) if value == BOOLEAN_ALIAS => StructField(key, BooleanType)
      case (key: String, value) if value == BYTE_ALIAS => StructField(key, ByteType)
      case (key: String, value) if value == DATE_ALIAS => StructField(key, DateType)
      case (key: String, value) if value == DOUBLE_ALIAS => StructField(key, DoubleType)
      case (key: String, value) if value == FLOAT_ALIAS => StructField(key, FloatType)
      case (key: String, value) if value == LONG_ALIAS => StructField(key, LongType)
      case (key: String, value) if value == STRING_ALIAS => StructField(key, StringType)
    }.toList)

    schema
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
    EntityInputFormat.setInputInfo(job, user, pass.getBytes, new Authorizations)
    EntityInputFormat.setZooKeeperInstanceInfo(job, inst, zk)
    if(filters.size > 0)
      EntityInputFormat.setQueryInfo(job, Set(entityType), andNode.end.build)
    else
      EntityInputFormat.setQueryInfo(job, Set(entityType))
    BaseQfdInputFormat.setSelectFields(job.getConfiguration, setAsJavaSet(columns.toSet))

    // translate from Event into Row
    sqlContext.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[EntityInputFormat], classOf[Key], classOf[EntityWritable])
      .map(_._2.get())
      .map(it => {
      asRow(it, schema, columns)
    })
  }

  private def asRow(entity: Entity, schema: StructType, columns: Array[String]): Row = {
    /**
     * The framework depends on the values being placed into the row in the same order in which they appear in the requiredColumns array.
     * Making a note here in case this is changed in the future- because it took a while to figure out.
     */
    val row = new GenericMutableRow(schema.fields.length)   // Still want to keep the raw event so that we can re-explode any possibly flattened tuples later
    columns.zipWithIndex.foreach (it => {
      val schemaField = schema.apply(it._1)
      schemaField match {
        case StructField(name, dataType, _, _)=> {
          val attr = entity.get(name)
          row.update(it._2, enforceCorrectType((if(attr != null) attr.getValue else null), dataType))
        }
      }
    })

    println(row)

    row
  }

  private  def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      desiredType match {
        case StringType => toString(value)
        case _ if value == null || value == "" => null // guard the non string type
        case IntegerType => toInt(value)
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case BooleanType => value.asInstanceOf[java.lang.Boolean].asInstanceOf[Boolean]
        case DateType => toDate(value)
      }
    }
  }


  private def toInt(value: Any): Int = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int]
      case value: java.lang.Long => value.asInstanceOf[Long].toInt
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
