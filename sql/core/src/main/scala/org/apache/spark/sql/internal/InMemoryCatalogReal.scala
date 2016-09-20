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

package org.apache.spark.sql.internal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.{ExperimentalMethods, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{DataSourceSessionCatalog, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.execution.{SparkOptimizer, SparkPlanner}
import org.apache.spark.sql.execution.datasources.HiveOnlyCheck

/**
 * An in-memory (ephemeral) implementation of the system catalog.
 *
 * This is a dummy implementation that does not require setting up external systems.
 * It is intended for testing or exploration purposes only and should not be used
 * in production.
 *
 * All public methods should be synchronized for thread-safety.
 */
class InMemoryCatalogReal(hadoopConfig: Configuration = new Configuration)
    extends InMemoryCatalog(hadoopConfig = hadoopConfig) {
  override def getSessionCatalog(sessionCatalog: SessionCatalog): DataSourceSessionCatalog =
    new DataSourceSessionCatalog(sessionCatalog, this,
      sessionCatalog.conf, sessionCatalog.hadoopConf) {

      override lazy val analyzer: Analyzer = new Analyzer(this, conf) {
        override val extendedResolutionRules = Nil
        override val extendedCheckRules = Seq(HiveOnlyCheck)
      }

      override lazy val optimizer: Optimizer = new SparkOptimizer(this,
        sessionCatalog.conf.asInstanceOf[SQLConf], new ExperimentalMethods) {
        override def batches = Nil
      }

      override def planner: Any = new SparkPlanner(
        sessionCatalog.sparkSession.asInstanceOf[SparkSession].sparkContext,
        sessionCatalog.conf.asInstanceOf[SQLConf], Nil) {
        override def strategies: Seq[Strategy] = Nil
      }
    }
}
