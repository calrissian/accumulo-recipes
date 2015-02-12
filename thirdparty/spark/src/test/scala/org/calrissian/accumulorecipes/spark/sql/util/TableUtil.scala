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
package org.calrissian.accumulorecipes.spark.sql.util

import org.apache.spark.sql.SQLContext
import org.calrissian.accumulorecipes.spark.sql.{EventStoreCatalyst, EventStoreFiltered}
import org.joda.time.DateTime

object TableUtil {

  private def registerEventTable(
                    clazz: String,
                    accUser: String,
                    accPass: String,
                    accInst: String,
                    zk: String,
                    startDate: DateTime,
                    endDate: DateTime,
                    eventType: String,
                    tableName: String)
                   (implicit sqlContext: SQLContext): Unit = {
    sqlContext.sql(
      s"""
      |CREATE TEMPORARY TABLE ${tableName}
      |USING ${clazz}
      |OPTIONS (
      | inst  '${accInst}',
      | zk    '${zk}',
      | user  '${accUser}',
      | pass  '${accPass}',
      | type  '${eventType}',
      | start '${startDate}',
      | end   '${endDate}'
      |)
    """.stripMargin)
  }

  def registerEventCatalystTable(accUser: String,
                         accPass: String,
                         accInst: String,
                         zk: String,
                         startDate: DateTime,
                         endDate: DateTime,
                         eventType: String,
                         tableName: String)
                        (implicit sqlContext: SQLContext): Unit = {

    registerEventTable(classOf[EventStoreCatalyst].getName.replace("$", ""),
      accUser, accPass, accInst, zk, startDate, endDate, eventType, tableName)
  }

  def registerEventFilteredTable(accUser: String,
                                 accPass: String,
                                 accInst: String,
                                 zk: String,
                                 startDate: DateTime,
                                 endDate: DateTime,
                                 eventType: String,
                                 tableName: String)
                                (implicit sqlContext: SQLContext): Unit = {

    registerEventTable(classOf[EventStoreFiltered].getName.replace("$", ""),
      accUser, accPass, accInst, zk, startDate, endDate, eventType, tableName)
  }

}
