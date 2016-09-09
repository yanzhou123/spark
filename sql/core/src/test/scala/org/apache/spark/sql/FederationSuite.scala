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

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class FederationSuite  extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("use table") {
    import org.apache.spark.sql.catalyst.catalog._
    val a = new InMemoryCatalog() { override val name = "abc" }
    spark.catalog.registerDataSource(a)

    assert(spark.catalog.getDataSourceList == List("abc", "in-memory"))

    val df1 = Seq((1, 2), (3, 4), (5, 6), (7, 8)).toDF("key", "value")
    df1.createOrReplaceTempView("abc..t1")

    checkAnswer(sql("select * from abc..t1"),
      Row(1, 2) :: Row(3, 4) :: Row(5, 6) :: Row(7, 8) :: Nil)

    val df2 = Seq((1, 3), (2, 4), (3, 5), (4, 6)).toDF("key", "value")
    df2.createOrReplaceTempView("abc..t2")

    checkAnswer(sql("select * from abc..t2"),
      Row(1, 3) :: Row(2, 4) :: Row(3, 5) :: Row(4, 6) :: Nil)

    sql("select tb1.key, tb2.value from abc..t1 tb1, abc..t2 tb2 where tb1.key == tb2.key").show
  }
}
