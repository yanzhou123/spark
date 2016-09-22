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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis._
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

  // for persistent purpose later
  var dirty = false

  def this(externalCatalog: ExternalCatalog) = {
    this
    registerDataSource(externalCatalog)
  }

  // TODO: use Guava Cache??
  // mapping from (SessionState, Data source) to SessionCatalog
  @transient private lazy val sessionCatalogMap =
  new ConcurrentHashMap[Any,
    scala.collection.mutable.Map[String, DataSourceSessionCatalog]]().asScala

  // Mapping from Data Source to ExternalCatalog
  private lazy val externalCatalogMap = new ConcurrentHashMap[String, ExternalCatalog]().asScala

  def getDataSourceCatalog(name: String): Option[ExternalCatalog] = externalCatalogMap.get(name)

  /**
   * Get a list of registered data source
   * @return the sorted names of registered data source
   */
  def getDataSourceList(): List[String] = {
    externalCatalogMap.keySet.toList.sorted
  }

  /**
   * To unregister a data source
   * @param name the name of the data source
   * @return the removed data source or None if it does not exists
   */
  def unregisterDataSource(name: String): Option[ExternalCatalog] = {
    externalCatalogMap.remove(name)
  }

  /**
   * To register pre-built data sources such as Hive and In-Memory
   * @param externalCatalog external catalog
   */
  def registerDataSource(externalCatalog: ExternalCatalog): Unit = {
    var created = false
    externalCatalogMap.getOrElseUpdate(externalCatalog.name, {
      created = true
      externalCatalog
    })
    if (!created) {
      throw new DataSourceAlreadyExistsException(externalCatalog.name)
    }
  }

  def registerDataSource(name: String, sessionCatalog: DataSourceSessionCatalog): Unit = {
    externalCatalogMap.getOrElseUpdate(name, sessionCatalog.externalCatalog)
    val sparkSession = sessionCatalog.parent.sparkSession
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
      InternalCatalog.reflect[ExternalCatalog, SparkConf, Configuration](provider,
        sparkContext.conf, sparkContext.hadoopConfiguration)
        .setProperties(properties)
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

object InternalCatalog {
  def reflect[T, Arg1 <: AnyRef, Arg2 <: AnyRef](className: String, ctorArg1: Arg1, ctorArg2: Arg2)
    (implicit ctorArg1Tag: ClassTag[Arg1], ctorArg2Tag: ClassTag[Arg2]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArg1Tag.runtimeClass, ctorArg2Tag.runtimeClass)
      ctor.newInstance(ctorArg1, ctorArg2).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
      throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }
}

