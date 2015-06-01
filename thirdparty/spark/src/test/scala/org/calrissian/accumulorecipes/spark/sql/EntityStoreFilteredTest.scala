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
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.calrissian.accumulorecipes.entitystore.impl.AccumuloEntityStore
import org.calrissian.accumulorecipes.test.AccumuloTestUtils
import org.calrissian.mango.domain.Attribute
import org.calrissian.mango.domain.entity.BaseEntity
import org.junit._
import org.junit.rules.TemporaryFolder

object EntityStoreFilteredTest {

  private val tempDir = new TemporaryFolder()
  private var miniCluster: MiniAccumuloClusterImpl = _
  private var entityStore: AccumuloEntityStore = _
  private var sparkContext: SparkContext = _
  private var sqlContext: SQLContext = _

  @BeforeClass
  def setup: Unit = {
    tempDir.create()
    println("WORKING DIRECTORY: " + tempDir.getRoot)
    miniCluster = new MiniAccumuloClusterImpl(tempDir.getRoot, "secret")
    miniCluster.start

    entityStore = new AccumuloEntityStore(miniCluster.getConnector("root", "secret"))

    val sparkConf = new SparkConf().setMaster("local").setAppName("TestEntityStoreSQL")
    sparkContext = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sparkContext)
  }

  private def createTempTable = {

    sqlContext.sql(
      s"""
        |CREATE TEMPORARY TABLE entities
        |USING org.calrissian.accumulorecipes.spark.sql.EntityStoreFiltered
        |OPTIONS (
        | inst '${miniCluster.getInstanceName}',
        | zk '${miniCluster.getZooKeepers}',
        | user 'root',
        | pass 'secret',
        | type 'type'
        |)
      """.stripMargin)
  }

  private def persistEntities = {
    val entity = new BaseEntity("type", "id")
    entity.put(new Attribute("key1", "val1"))
    entity.put(new Attribute("key2", 5))

    entityStore.save(Collections.singleton(entity))
    entityStore.flush()
  }

  @AfterClass
  def tearDown: Unit = {
    miniCluster.stop
    tempDir.delete
    sparkContext.stop()
  }

}
class EntityStoreFilteredTest {

  import org.calrissian.accumulorecipes.spark.sql.EntityStoreFilteredTest._

  @Before
  def setupTest: Unit = {
    persistEntities
    createTempTable
  }

  @After
  def teardownTest: Unit = {
    sqlContext.dropTempTable("entities")
    AccumuloTestUtils.clearTable(miniCluster.getConnector("root", "secret"), "entity_shard")
    AccumuloTestUtils.clearTable(miniCluster.getConnector("root", "secret"), "entity_index")
  }

  @Test
  def testSelectionAndWhereWithAndOperator() {

    val rows = sqlContext.sql("SELECT key2,key1 FROM entities WHERE (key1 = 'val1' and key2 >= 5)").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](0))
    Assert.assertEquals("val1", rows(0).getAs(1))
  }

  @Test
  def testSelectAndWhereSingleOperator() {

    val rows = sqlContext.sql("SELECT key1,key2 FROM entities WHERE (key1 = 'val1')").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test
  def testSelectionOnlyMultipleFields() {

    val rows = sqlContext.sql("SELECT key1,key2 FROM entities").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test
  def testFieldInSchemaMissingFromEventIsNull: Unit = {
    val entity = new BaseEntity("type", "id2")
    entity.put(new Attribute("key1", "val2"))
    entity.put(new Attribute("key2", 10))
    entity.put(new Attribute("key3", 5))
    entityStore.save(Collections.singleton(entity))
    entityStore.flush

    // Reset temporary table to provide updated schema
    sqlContext.dropTempTable("entities")
    createTempTable

    val rows = sqlContext.sql("SELECT * FROM entities WHERE key2 >= 5 ORDER BY key2 ASC").collect

    Assert.assertEquals(2, rows.length)
    Assert.assertNull(rows(0).apply(2)) // the event missing key3 should sort first given the orderBy

  }

  @Test
  def testSelectionOnlySingleField = {

    val rows = sqlContext.sql("SELECT key1 FROM entities").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test
  def testNoSelectionAndNoWhereClause = {

    val rows = sqlContext.sql("SELECT * FROM entities").collect

    Assert.assertEquals(1, rows.length)
    Assert.assertEquals(5, rows(0).getAs[Int](1))
    Assert.assertEquals("val1", rows(0).getAs(0))
  }

  @Test(expected = classOf[AnalysisException])
  def testSelectFieldNotInSchema: Unit = sqlContext.sql("SELECT doesntExist FROM entities").collect

  @Test
  def testWhereClauseNoMatches: Unit = Assert.assertEquals(0, sqlContext.sql("SELECT * FROM entities WHERE key2 > 5").collect.length)


}
