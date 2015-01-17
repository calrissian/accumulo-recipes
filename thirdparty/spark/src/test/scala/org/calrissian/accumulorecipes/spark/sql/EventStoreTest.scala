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

import java.util.Collections

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.{SparkConf, SparkContext}
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore
import org.calrissian.accumulorecipes.test.AccumuloTestUtils
import org.calrissian.mango.domain.Tuple
import org.calrissian.mango.domain.event.BaseEvent
import org.joda.time.DateTime
import org.junit._
import org.junit.rules.TemporaryFolder

object EventStoreTest {

  private val tempDir = new TemporaryFolder()
  private var miniCluster: MiniAccumuloClusterImpl = _
  private var eventStore:AccumuloEventStore = _
  private var sqlContext: SQLContext = _

  @BeforeClass
  def setup: Unit = {
    tempDir.create()
    println("WORKING DIRECTORY: " + tempDir.getRoot)
    miniCluster = new MiniAccumuloClusterImpl(tempDir.getRoot, "secret")
    miniCluster.start

    eventStore = new AccumuloEventStore(miniCluster.getConnector("root", "secret"))

    val sparkConf = new SparkConf().setMaster("local").setAppName("TestEventStoreSQL")
    sqlContext = new SQLContext(new SparkContext(sparkConf))
  }

  private def createTempTable = {

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
  }

  private def persistEvents = {
    val event = new BaseEvent("type", "id")
    event.put(new Tuple("key1", "val1"))
    event.put(new Tuple("key2", 5))

    eventStore.save(Collections.singleton(event))
    eventStore.flush()
  }

  @AfterClass
  def tearDown: Unit = {
    miniCluster.stop
    tempDir.delete
  }

}
class EventStoreTest {

  import org.calrissian.accumulorecipes.spark.sql.EventStoreTest._

  @Before
  def setupTest: Unit = {
    persistEvents
    createTempTable
  }

  @After
  def teardownTest: Unit = {
    sqlContext.dropTempTable("events")
    AccumuloTestUtils.clearTable(miniCluster.getConnector("root", "secret"), "eventStore_shard")
    AccumuloTestUtils.clearTable(miniCluster.getConnector("root", "secret"), "eventStore_index")
  }

  @Test
  def testSelectionAndWhereWithAndOperator() {

    val rows = sqlContext.sql("SELECT key2,key1 FROM events WHERE (key1 = 'val1' and key2 >= 5)").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](0))
    Assert.assertEquals("val1", rows(0).getAs(1))
  }

  @Test
  def testSelectAndWhereSingleOperator() {

    val rows = sqlContext.sql("SELECT key1,key2 FROM events WHERE (key1 = 'val1')").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test
  def testSelectionOnlyMultipleFields() {

    val rows = sqlContext.sql("SELECT key1,key2 FROM events").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test
  def testFieldInSchemaMissingFromEventIsNull: Unit = {
    val event = new BaseEvent("type", "id2")
    event.put(new Tuple("key1", "val2"))
    event.put(new Tuple("key2", 10))
    event.put(new Tuple("key3", 5))
    eventStore.save(Collections.singleton(event))
    eventStore.flush

    // Reset temporary table to provide updated schema
    sqlContext.dropTempTable("events")
    createTempTable

    val rows = sqlContext.sql("SELECT * FROM events WHERE key2 >= 5 ORDER BY key2 ASC").collect

    Assert.assertEquals(2, rows.length)
    Assert.assertNull(rows(0).apply(2)) // the event missing key3 should sort first given the orderBy

  }

  @Test
  def testSelectionOnlySingleField = {

    val rows = sqlContext.sql("SELECT key1 FROM events").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test
  def testNoSelectionAndNoWhereClause = {

    val rows = sqlContext.sql("SELECT * FROM events").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test(expected = classOf[TreeNodeException[_]])
  def testSelectFieldNotInSchema: Unit = sqlContext.sql("SELECT doesntExist FROM events").collect

  @Test
  def testWhereClauseNoMatches: Unit = Assert.assertEquals(0, sqlContext.sql("SELECT * FROM events WHERE key2 > 5").collect.length)


}
