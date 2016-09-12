/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class FederationSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import hiveContext._
  import spark.implicits._

  test("use table") {
    import org.apache.spark.sql.catalyst.catalog._

    val c1 = new InMemoryCatalog() { override val name = "abc" }
    spark.catalog.registerDataSource(c1)

    val c2 = new HiveExternalCatalog(sparkContext) { override val name = "xyz" }
    spark.catalog.registerDataSource(c2)

    assert(spark.catalog.getDataSourceList == List("abc", "hive", "xyz"))

    sql("drop table if exists hive..t1")
    sql("drop table if exists xyz..t2")

    sql("CREATE TABLE hive..t1(key INT, value INT)")

    sql("CREATE TABLE xyz..t2(key INT, value INT)")

    assert(spark.catalog.listTablesByDataSource("hive").map(_.table) == Seq("t1"))
    assert(spark.catalog.listTablesByDataSource("xyz").map(_.table) == Seq("t2"))

    Seq((1, 2), (3, 4), (5, 6), (7, 8)).toDF("key", "value").write.mode("overwrite")
      .insertInto("hive..t1")

    Seq((1, 3), (2, 4), (3, 5), (4, 6)).toDF("key", "value").write.mode("overwrite")
      .insertInto("xyz..t2")

    Seq((1, 3), (2, 4), (3, 5), (4, 6)).toDF("key", "value")
      .write.mode("overwrite").insertInto("xyz..t2")

    checkAnswer(sql("select * from xyz..t2"),
      Row(1, 3) :: Row(2, 4) :: Row(3, 5) :: Row(4, 6) :: Nil)

    checkAnswer(
      sql("select tb1.key, tb2.value from t1 tb1, xyz..t2 tb2 where tb1.key == tb2.key"),
      Row(1, 3) :: Row(3, 5) :: Nil)
  }
}
