package org.calrissian.accumulorecipes.spark.sql.util

import org.apache.spark.sql.SQLContext
import org.calrissian.accumulorecipes.spark.sql.EventStore
import org.joda.time.DateTime

object TableUtil {

  def registerEventTable(accUser: String,
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
      |USING ${classOf[EventStore].getName.replace("$", "")}
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
}
