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

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.sources.CatalystScan
import org.apache.spark.sql.{Row, SQLContext}
import org.calrissian.accumulorecipes.commons.domain.Gettable
import org.calrissian.accumulorecipes.commons.hadoop.BaseQfdInputFormat
import org.calrissian.mango.collect.CloseableIterable
import org.calrissian.mango.criteria.builder.QueryBuilder
import org.calrissian.mango.criteria.domain.Node
import org.calrissian.mango.domain.TupleStore
import org.calrissian.mango.types.encoders.AliasConstants._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._



abstract class QfdStoreCatalystScan(inst: String, zk: String, user: String, pass: String, objectType: String,
                       @transient val sqlContext: SQLContext) extends CatalystScan with Serializable {   // TODO: Find out what's forcing this to be serializable

  private val log = LoggerFactory.getLogger(classOf[QfdFilteredScan])

  type T <: TupleStore
  type V <: Gettable[T]
  type I <: BaseQfdInputFormat[T,_]

  val internalSchema: StructType = parseSchema()

  override def schema: StructType = internalSchema

  def uniqueKeys(connector: Connector): CloseableIterable[org.calrissian.mango.domain.Pair[String, String]]

  private def parseSchema(): StructType = {

    val instance = new ZooKeeperInstance(inst, zk)
    val connector = instance.getConnector(user, new PasswordToken(pass))
    val keyValueIndex = uniqueKeys(connector)
    val keys = keyValueIndex.toList

    keyValueIndex.closeQuietly()

    // turn keys into StructType
    val schema = StructType(keys.map(it => (it.getOne, it.getTwo)).collect {
      case (key: String, value) if value == INTEGER_ALIAS => StructField(key, IntegerType)
      case (key: String, value) if value == BOOLEAN_ALIAS => StructField(key, BooleanType)
      case (key: String, value) if value == BYTE_ALIAS => StructField(key, ByteType)
      case (key: String, value) if value == DATE_ALIAS => StructField(key, DateType)
      case (key: String, value) if value == DOUBLE_ALIAS => StructField(key, DoubleType)
      case (key: String, value) if value == FLOAT_ALIAS => StructField(key, FloatType)
      case (key: String, value) if value == LONG_ALIAS => StructField(key, LongType)
      case (key: String, value) if value == STRING_ALIAS => StructField(key, StringType)
      case (key: String, value) => throw new Exception("Cannot map to SparkSQL schema, invalid type was encountered [" + value + "]")
    }.toList)

    schema
  }


  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    val node = buildQuery(new QueryBuilder().and(), filters).end().build()
    buildRDD(requiredColumns, filters, node).map(it => asRow(it, schema, requiredColumns.map(_.name).toArray))
  }


  def buildRDD(requiredColumns: Seq[Attribute], filters: Seq[Expression], node: Node): RDD[T]

  private def buildQuery(qb: QueryBuilder, filters: Seq[Expression]): QueryBuilder = {
    var query = qb
    filters.foreach(it => it match {
      case EqualTo(AttributeReference(key, dt, nb, meta), Literal(value, dataType)) => query = query.eq(key, value)
      case and: And => query = buildQuery(query.and(), Array(and.left, and.right)).end()
      case or: Or => query = buildQuery(query.or(), Array(or.left, or.right)).end()
      case GreaterThan(AttributeReference(key, dt, nb, meta), Literal(right, datType)) => query = query.greaterThan(key, right)
      case LessThan(AttributeReference(key, dt, nb, meta), Literal(right, dataType)) => query = query.lessThan(key, right)
      case LessThanOrEqual(AttributeReference(key, dt, nb, meta), Literal(right, dataType)) => query = query.lessThanEq(key, right)
      case _ => query
    })
    query
  }

  private def asRow(event: T, schema: StructType, columns: Array[String]): Row = {
    /**
     * The framework depends on the values being placed into the row in the same order in which they appear in the requiredColumns array.
     * Making a note here in case this is changed in the future- because it took a while to figure out.
     */
    val row = new GenericMutableRow(schema.fields.length) // Still want to keep the raw event so that we can re-explode any possibly flattened tuples later
    columns.zipWithIndex.foreach(it => {
      val schemaField = schema.apply(it._1)
      schemaField match {
        case StructField(name, dataType, _, _) => {
          val attr = event.get(name)
          row.update(it._2, enforceCorrectType((if (attr != null) attr.getValue else null), dataType))
        }
      }
    })
    row
  }

  private def enforceCorrectType(value: Any, desiredType: DataType): Any = {
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
        case _  => throw new Exception("Type " + desiredType + " is not supported for type coercion.")
      }
    }
  }

  private def toInt(value: Any): Int = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int]
      case value: java.lang.Long => value.asInstanceOf[Long].toInt
      case _ => throw new Exception("Invalid type was encountered [" + value.getClass + "]")
    }
  }

  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
      case _ => throw new Exception("Invalid type was encountered [" + value.getClass + "]")
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
      case _ => throw new Exception("Invalid type was encountered [" + value.getClass + "]")
    }
  }

  private def toString(value: Any): String = {
    value match {
      case value => Option(value).map(_.toString).orNull
      case _ => throw new Exception("Invalid type was encountered [" + value.getClass + "]")
    }
  }

  private def toDate(value: Any): Date = {
    value match {
      // only support string as date
      case value: java.lang.String => DateTime.parse(value).toDate
      case value: Date => value
      case _ => throw new Exception("Invalid type was encountered [" + value.getClass + "]")
    }
  }
}
