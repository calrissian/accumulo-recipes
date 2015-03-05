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

import java.util.Collections

import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.{SparkConf, SparkContext}
import org.calrissian.accumulorecipes.eventstore.impl.AccumuloEventStore
import org.calrissian.accumulorecipes.spark.sql.util.TableUtil
import org.calrissian.accumulorecipes.test.AccumuloTestUtils
import org.calrissian.mango.domain.Attribute
import org.calrissian.mango.domain.event.BaseEvent
import org.joda.time.DateTime
import org.junit._
import org.junit.rules.TemporaryFolder

object EventStoreCatalystTest {

  private val tempDir = new TemporaryFolder()
  private var miniCluster: MiniAccumuloClusterImpl = _
  private var eventStore:AccumuloEventStore = _
  private var sparkContext: SparkContext = _
  private implicit var sqlContext: SQLContext = _

  @BeforeClass
  def setup: Unit = {
    tempDir.create()
    println("WORKING DIRECTORY: " + tempDir.getRoot)
    miniCluster = new MiniAccumuloClusterImpl(tempDir.getRoot, "secret")
    miniCluster.start

    eventStore = new AccumuloEventStore(miniCluster.getConnector("root", "secret"))

    val sparkConf = new SparkConf().setMaster("local").setAppName("TestEventStoreSQL")
    sparkContext = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sparkContext)
  }

  private def createTempTable = {

    TableUtil.registerEventCatalystTable("root",
      "secret",
      miniCluster.getInstanceName,
      miniCluster.getZooKeepers,
      new DateTime(System.currentTimeMillis() - 5000),
      new DateTime(System.currentTimeMillis() + 5000),
      "type",
      "events")
  }

  private def persistEvents = {
    val event = new BaseEvent("type", "id")
    event.put(new Attribute("key1", "val1"))
    event.put(new Attribute("key2", 5))


    val entity2 = new BaseEvent("type2", "id")
    entity2.put(new Attribute("key1", "val1"))
    entity2.put(new Attribute("key3", "val3"))

    eventStore.save(Collections.singleton(event))
    eventStore.save(Collections.singleton(entity2))
    eventStore.flush()
  }

  @AfterClass
  def tearDown: Unit = {
    miniCluster.stop
    tempDir.delete
    sparkContext.stop()
  }

}
class EventStoreCatalystTest {

  import org.calrissian.accumulorecipes.spark.sql.EventStoreCatalystTest._

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

  /**
   * A simple join of two temporarily registered tables representing different logical datasets
   */
  @Test
  def testJoin(): Unit = {
    TableUtil.registerEventCatalystTable("root",
      "secret",
      miniCluster.getInstanceName,
      miniCluster.getZooKeepers,
      new DateTime(System.currentTimeMillis() - 5000),
      new DateTime(System.currentTimeMillis() + 5000),
      "type2",
      "events2")

    AccumuloTestUtils.dumpTable(miniCluster.getConnector("root", "secret"), "eventStore_shard")

    val rows = sqlContext.sql("SELECT e.key1,e.key2,t.key3 FROM events e JOIN events2 t ON e.key1 = t.key1").collect

    System.out.println(rows.toList)
    Assert.assertEquals(1, rows.length)
    Assert.assertEquals("val1", rows(0).getAs[String](0))
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val3", rows(0).getAs[String](2))

    sqlContext.dropTempTable("events2")
  }

  /**
   * Test selection and predicate filtering with and
   */
  @Test
  def testSelectionAndWhereWithAndOperator() {

    val rows = sqlContext.sql("SELECT key2,key1 FROM events WHERE (key1 = 'val1' and key2 >= 5)").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](0))
    Assert.assertEquals("val1", rows(0).getAs(1))
  }

  /**
   * Test selection and predicate filtering with or
   */
  @Test
  def testSelectionAndWhereWithOrOperator() {

    val rows = sqlContext.sql("SELECT key2,key1 FROM events WHERE (key1 = 'val1' or key2 >= 5)").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](0))
    Assert.assertEquals("val1", rows(0).getAs(1))
  }

  /**
   * Test selection and single equals in single predicate
   */
  @Test
  def testSelectAndWhereSingleOperator() {

    val rows = sqlContext.sql("SELECT key1, key2 FROM events WHERE (key1 = 'val1')").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  /**
   * Test selection but no predicates
   */
  @Test
  def testSelectionOnlyMultipleFields() {

    val rows = sqlContext.sql("SELECT key1,key2 FROM events").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  /**
   * Test that a field missing from a schema is null in the output
   */
  @Test
  def testFieldInSchemaMissingFromEventIsNull: Unit = {
    val event = new BaseEvent("type", "id2")
    event.put(new Attribute("key1", "val2"))
    event.put(new Attribute("key2", 10))
    event.put(new Attribute("key3", 5))
    eventStore.save(Collections.singleton(event))
    eventStore.flush

    // Reset temporary table to provide updated schema
    sqlContext.dropTempTable("events")
    createTempTable

    val rows = sqlContext.sql("SELECT * FROM events WHERE key2 >= 5 ORDER BY key2 ASC").collect

    Assert.assertEquals(2, rows.length)
    Assert.assertNull(rows(0).apply(2)) // the event missing key3 should sort first given the orderBy

  }

  /**
   * Test selecting a single field with no predicates
   */
  @Test
  def testSelectionOnlySingleField = {

    val rows = sqlContext.sql("SELECT key1 FROM events").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  /**
   * Test selecting all fields with no predicates
   */
  @Test
  def testNoSelectionAndNoWhereClause = {

    val rows = sqlContext.sql("SELECT * FROM events").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  /**
   * Selecting a field that is not in the schema should fail gracefully
   */
  @Test(expected = classOf[TreeNodeException[_]])
  def testSelectFieldNotInSchema: Unit = sqlContext.sql("SELECT doesntExist FROM events").collect

  /**
   * Test no items get returned
   */
  @Test
  def testWhereClauseNoMatches: Unit = Assert.assertEquals(0, sqlContext.sql("SELECT * FROM events WHERE key2 > 5").collect.length)


}
