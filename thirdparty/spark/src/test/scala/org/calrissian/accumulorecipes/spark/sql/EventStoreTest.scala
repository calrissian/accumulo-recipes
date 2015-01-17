package org.calrissian.accumulorecipes.spark.sql

import org.junit.Test
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.accumulo.core.client.{Connector, Instance}
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.calrissian.mango.domain.event.BaseEvent
import org.calrissian.mango.domain.Tuple
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore
import java.util.Collections
import org.junit.rules.TemporaryFolder
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl
import org.joda.time.DateTime

class EventStoreTest {

  @Test
  def test() {

    val tempDir = new TemporaryFolder()
    tempDir.create()

    val miniCluster = new MiniAccumuloClusterImpl(tempDir.getRoot, "secret")
    miniCluster.start

    val eventStore = new AccumuloEventStore(miniCluster.getConnector("root", "secret"))
    val event = new BaseEvent("type", "id")
    event.put(new Tuple("key1", "val1"))
    event.put(new Tuple("key2", 5))

    eventStore.save(Collections.singleton(event))
    eventStore.flush()

    val sparkConf = new SparkConf().setMaster("local").setAppName("testSQL")
    val context = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(context)

    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE events
        |USING org.calrissian.accumulorecipes.spark.sql.EventStore
        |OPTIONS (
        | inst '${miniCluster.getInstanceName}',
        | zk '${miniCluster.getZooKeepers}',
        | user 'root',
        | pass 'secret',
        | type 'type',
        | start '${new DateTime(System.currentTimeMillis() - 5000)}',
        | end '${new DateTime(System.currentTimeMillis() + 5000)}'
        |)
      """.stripMargin)

    System.out.println(sqlContext.sql("SELECT key1,key2 from events where key1 = 'val1' and key2 = 5").count())

    miniCluster.stop
    tempDir.delete
  }

}
