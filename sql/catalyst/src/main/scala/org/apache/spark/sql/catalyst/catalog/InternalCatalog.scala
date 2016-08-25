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

package org.apache.spark.sql.catalyst.catalog

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.Utils

/**
 * Interface for the system catalog (of columns, partitions, tables, and databases).
 *
 * This is only used for non-temporary items, and implementations must be thread-safe as they
 * can be accessed in multiple threads. This is an external catalog because it is expected to
 * interact with external systems.
 *
 * The two type parameters are for getSessionState only. Refers to the comments there
 *
 * Implementations should throw [[NoSuchDatabaseException]] when databases don't exist.
 */
class InternalCatalog extends Serializable {

  import CatalogTypes.TablePartitionSpec

  // for persistent purpose later
  var dirty = false

  def this(name: String, externalCatalog: ExternalCatalog) = {
    this
    registerDataSource(name, externalCatalog)
  }

  def this(externalCatalog: ExternalCatalog) = {
    this(SessionCatalog.DEFAULT_DATASOURCE, externalCatalog)
  }

  // TODO: use Guava Cache??
  // mapping from (SessionState, Data source) to SessionCatalog
  @transient private lazy val sessionCatalogMap =
  new ConcurrentHashMap[Any,
    scala.collection.mutable.Map[String, DataSourceSessionCatalog]]().asScala

  // Mapping from Data Source to ExternalCatalog
  private lazy val externalCatalogMap = new ConcurrentHashMap[String, ExternalCatalog]().asScala

  def getDataSourceCatalog(name: String): Option[ExternalCatalog]
  = externalCatalogMap.get(name)

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

  // To register pre-built data sources such as Hive and In-Memory
  //
  // @param name            data source name
  // @param externalCatalog external catalog
  def registerDataSource(name: String, externalCatalog: ExternalCatalog): Unit = {
    var created = false
    externalCatalogMap.getOrElseUpdate(name, {
      created = true
      externalCatalog
    })
    if (!created) {
      throw new DataSourceAlreadyExistsException(name)
    }
  }

  def registerDataSource(name: String, sessionCatalog: DataSourceSessionCatalog): Unit = {
    externalCatalogMap.getOrElseUpdate(name, sessionCatalog.externalCatalog)
    val sparkSession = sessionCatalog.sessionCatalog.sparkSession
    var created = false
    sessionCatalogMap.getOrElseUpdate(sparkSession, {
      new ConcurrentHashMap[String, DataSourceSessionCatalog]().asScala
    }).getOrElseUpdate(name, {
      created = true
      sessionCatalog
    })
    if (!created) {
      throw new DataSourceSessionAlreadyExistsException(name)
    }
  }

  def registerDataSource(name: String, provider: String, properties: Map[String, String],
                         sparkContext: SparkContext): Unit = {
    var created = false
    externalCatalogMap.getOrElseUpdate(name, {
      created = true
      reflect[ExternalCatalog, SparkContext](provider, sparkContext).setProperties(properties)
    })
    if (!created) {
      throw new DataSourceAlreadyExistsException(name)
    }
  }

  // Due to the current module dependence from SQL core to catalyst with
  // SparkSession in the core, the type parameter is used here which would be
  // unnecessary otherwise. Needs to be revisited when and should the module dependency
  // be changed
  //
  // @param sparkSession SparkSession
  // @return SessionState

  def getSessionCatalog(dataSource: String,
                        sessionCatalog: SessionCatalog): DataSourceSessionCatalog = {
    sessionCatalogMap.getOrElseUpdate(sessionCatalog.sparkSession, {
      new ConcurrentHashMap[String, DataSourceSessionCatalog]().asScala
    }).getOrElseUpdate(dataSource, {
      getExternalCatalog(dataSource).getSessionCatalog(sessionCatalog)
    })
  }

  def getSessionCatalogs(sparkSession: Any): Seq[DataSourceSessionCatalog] = {
    sessionCatalogMap.get(sparkSession).map(_.values.toList).getOrElse(Nil)
  }

  def getExternalCatalog(dataSource: String): ExternalCatalog = {
    externalCatalogMap.getOrElse(dataSource, (
      throw new NoSuchDataSourceException(dataSource)))
  }

  def dsExists(dataSource: String): Boolean = {
    externalCatalogMap.contains(dataSource)
  }
}
