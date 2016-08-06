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

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.internal.config.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.{Strategy, _}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FindDataSourceTable, PreprocessTableInsertion, ResolveDataSource}
import org.apache.spark.util.Utils


/**
 * A composite class that holds all session-specific states in a given [[SparkSession]].
 */
private[sql] class SQLSessionState(sparkSession: SparkSession) extends SessionState(sparkSession) {

  // Note: These are all lazy vals because they depend on each other (e.g. conf) and we
  // want subclasses to override some of the fields. Otherwise, we would get a lot of NPEs.

  private lazy val inMemState = new SessionState(sparkSession) {
    override lazy val parent: Option[SessionState] = Some(sparkSession.sessionState)

    override lazy val analyzer: Analyzer = {
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

  private val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"
  private val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSessionState"

  /**
   * current active session state: will be made val on in-memory once data source functionalities
   * are supported for Hive and all other data sources
   */
  lazy val currentSessionState = sparkSession.sparkContext.conf
    .get(CATALOG_IMPLEMENTATION.key, "in-memory") match {
    case "hive" => val result = sessionStateMap.get("hive").orElse {
        val hiveExternCatalog = reflect[ExternalCatalog, SparkContext](
          HIVE_EXTERNAL_CATALOG_CLASS_NAME, sparkSession.sparkContext
        )
        sparkSession.sharedState.externalCatalog = hiveExternCatalog
        val hiveSessionState = reflect[SessionState, SparkSession](
          HIVE_SESSION_STATE_CLASS_NAME, sparkSession)
        sessionStateMap.put("hive", hiveSessionState)
        Some(hiveSessionState)}
      sparkSession.sharedState.externalCatalog = result.get.catalog.externalCatalog
      result
    case _ => Some(inMemState)
  }

  /**
   * Helper method to create an instance of [[T]] using a single-arg constructor that
   * accepts [[Arg]].
   */
  private def reflect[T, Arg <: AnyRef](className: String, ctorArg: Arg)
                                                        (implicit ctorArgTag: ClassTag[Arg]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  /**
   * SQL-specific key-value configurations.
   */
  override lazy val conf: SQLConf = currentSessionState.map(_.conf).getOrElse(null)

  override def newHadoopConfWithOptions(options: Map[String, String]): Configuration =
    currentSessionState.map(_.newHadoopConfWithOptions(options)).getOrElse(null)

  override lazy val experimentalMethods: ExperimentalMethods = new ExperimentalMethods {
    extraStrategies =
      combine[Strategy]((s: SessionState) => s.experimentalMethods.extraStrategies)
    extraOptimizations =
      combine[Rule[LogicalPlan]]((s: SessionState) => s.experimentalMethods.extraOptimizations)
  }


  /**
   * Internal catalog for managing table and database states.
   */
  override lazy val catalog = currentSessionState.map(_.catalog).getOrElse(null)

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
   * Logical query plan optimizer. Can't make it composable now
    * due to the nested type of Batch per Optimizer instance which
    * makes the concatenation of batches impossible.
   */
    /*
  override lazy val optimizer: Optimizer = new Optimizer(catalog, conf) {
    override def batches = orderedSessionState.map(_.optimizer.batches).reduceLeft(_ ++ _)
  }
  */

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
