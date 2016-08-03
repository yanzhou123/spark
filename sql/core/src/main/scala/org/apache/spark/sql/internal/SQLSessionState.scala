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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{RuleExecutor, Rule}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{ResolveDataSource, DataSourceAnalysis, FindDataSourceTable, PreprocessTableInsertion}

import scala.collection.mutable


/**
 * A composite class that holds all session-specific states in a given [[SparkSession]].
 */
private[sql] class SQLSessionState(sparkSession: SparkSession) extends SessionState(sparkSession) {

  // Note: These are all lazy vals because they depend on each other (e.g. conf) and we
  // want subclasses to override some of the fields. Otherwise, we would get a lot of NPEs.

  private lazy val inMemState = new SessionState(sparkSession, Some(this)) {
    override val analyzer: Analyzer = {
      new Analyzer(catalog, conf) {
        override val extendedResolutionRules =
          PreprocessTableInsertion(conf) ::
            new FindDataSourceTable(sparkSession) ::
            DataSourceAnalysis(conf) ::
            (if (conf.runSQLonFile) new ResolveDataSource(sparkSession) :: Nil else Nil)

        override val extendedCheckRules = Seq(datasources.PreWriteCheck(conf, catalog))
      }
    }

    override def planner: SparkPlanner =
      new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)

  }
  private lazy val sessionStateMap = {
    val result = new mutable.HashMap[String, SessionState]()
    result.put("in-memory", inMemState)
    result
  }

  // combine children rules
  private def combine[T](s: SessionState => Seq[T]): Seq[T] = {
    orderedSessionState.map(s(_)).reduceLeft(_ ++ _)
  }

  // ordered sessions states, currently with in-memory state leading the way
  private lazy val orderedSessionState = {
    List(inMemState) ++ sessionStateMap.clone.remove("in-memory").toList
  }

  /**
   * current active session state: will be made val on in-memory once data source functionalities
   * are supported for Hive and all other data sources
   */
  var currentSessionState = sessionStateMap.get("in-memory")

  /**
   * SQL-specific key-value configurations.
   */
  override def conf: SQLConf = currentSessionState.map(_.conf).getOrElse(null)

  override def newHadoopConfWithOptions(options: Map[String, String]): Configuration =
    currentSessionState.map(_.newHadoopConfWithOptions(options)).getOrElse(null)

  override def experimentalMethods = new ExperimentalMethods {
    override var extraStrategies =
      combine[Strategy]((s: SessionState) => s.experimentalMethods.extraStrategies)
    override var extraOptimizations =
      combine[Rule[LogicalPlan]]((s: SessionState)=> s.experimentalMethods.extraOptimizations)
  }


  /**
   * Internal catalog for managing table and database states.
   */
  override def catalog = currentSessionState.map(_.catalog).getOrElse(null)

  /**
   * Logical query plan analyzer for resolving unresolved attributes and relations.
   */
  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        combine[Rule[LogicalPlan]]((s: SessionState) => s.analyzer.extendedResolutionRules)

      override val extendedCheckRules =
        combine[LogicalPlan => Unit]((s: SessionState) => s.analyzer.extendedCheckRules)
    }
  }

  /**
   * Logical query plan optimizer.
   */
  override lazy val optimizer: Optimizer = new Optimizer(catalog, conf) {
    override def batches = orderedSessionState.map(_.optimizer.batches).reduceLeft(_ ++ _)
  }

  /**
   * Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
   */
  override lazy val sqlParser: ParserInterface = new SparkSqlParser(conf)

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  override def planner: SparkPlanner =
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies) {
      override def strategies = combine[Strategy]((s: SessionState) => s.planner.strategies)
    }

  override def refreshTable(tableName: String): Unit = {
    // TODO: refresh based upon data source specification in tableName
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }
}
