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
    val sparkSession = sessionCatalog.sparkSession
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

  protected def requireDbExists(datasource: String, db: String): Unit = {
    val exists = getDataSourceCatalog(datasource).map(_.databaseExists(db)).getOrElse(
      throw new NoSuchDataSourceException(datasource)
    )
    if (exists) {
      throw new NoSuchDatabaseException(db)
    }
  }

  // Due to the current module dependence from SQL core to catalyst with
  // SparkSession in the core, the type parameter is used here which would be
  // unnecessary otherwise. Needs to be revisited when and should the module dependency
  // be changed
  //
  // @param sparkSession SparkSession
  // @return SessionState

  def getSessionCatalog(dataSource: String, sparkSession: Any): DataSourceSessionCatalog = {
    sessionCatalogMap.getOrElseUpdate(sparkSession, {
      new ConcurrentHashMap[String, DataSourceSessionCatalog]().asScala
    }).getOrElseUpdate(dataSource, {
      getExternalCatalog(dataSource).getSessionCatalog(sparkSession)
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

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  def createDatabase(sparkSession: Any, dataSource: String,
                     dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).createDatabase(dbDefinition, ignoreIfExists)
  }

  def dropDatabase(sparkSession: Any, dataSource: String, db: String, ignoreIfNotExists: Boolean,
                   cascade: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).dropDatabase(db, ignoreIfNotExists, cascade)
  }

  // Alter a database whose name matches the one specified in `dbDefinition`,
  // assuming the database exists.
  //
  // Note: If the underlying implementation does not support altering a certain field,
  // this becomes a no-op.
  def alterDatabase(sparkSession: Any, dataSource: String, dbDefinition: CatalogDatabase): Unit = {
    getSessionCatalog(dataSource, sparkSession).alterDatabase(dbDefinition)
  }

  def getDatabaseMetadata(sparkSession: Any, dataSource: String, db: String): CatalogDatabase = {
    getSessionCatalog(dataSource, sparkSession).getDatabaseMetadata(db)
  }

  def databaseExists(sparkSession: Any, dataSource: String, db: String): Boolean = {
    getSessionCatalog(dataSource, sparkSession).databaseExists(db)
  }

  def listDatabases(sparkSession: Any, dataSource: String): Seq[String] = {
    getSessionCatalog(dataSource, sparkSession).listDatabases()
  }

  def listDatabases(sparkSession: Any, dataSource: String, pattern: String): Seq[String] = {
    getSessionCatalog(dataSource, sparkSession).listDatabases(pattern)
  }

  def getCurrentDatabase(sparkSession: Any, dataSource: String): String = {
    getSessionCatalog(dataSource, sparkSession).getCurrentDatabase
  }

  def setCurrentDatabase(sparkSession: Any, dataSource: String, db: String): Unit = {
    getSessionCatalog(dataSource, sparkSession).setCurrentDatabase(db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  def createTable(sparkSession: Any, dataSource: String, tableDefinition: CatalogTable,
                  ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).createTable(tableDefinition, ignoreIfExists)
  }

  def dropTable(sparkSession: Any, dataSource: String, table: TableIdentifier,
                ignoreIfNotExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).dropTable(table, ignoreIfNotExists)
  }

  def renameTable(sparkSession: Any, dataSource: String, oldName: TableIdentifier,
                  newName: TableIdentifier): Unit = {
    getSessionCatalog(dataSource, sparkSession).renameTable(oldName, newName)
  }

  // Alter a table whose name that matches the one specified in `tableDefinition`,
  // assuming the table exists.
  //
  // Note: If the underlying implementation does not support altering a certain field,
  // this becomes a no-op.
  def alterTable(sparkSession: Any, dataSource: String, tableDefinition: CatalogTable): Unit = {
    getSessionCatalog(dataSource, sparkSession).alterTable(tableDefinition)
  }

  def lookupRelation(sparkSession: Any, dataSource: String, name: TableIdentifier,
                     alias: Option[String] = None): LogicalPlan = {
    getSessionCatalog(dataSource, sparkSession).lookupRelation(name, alias)
  }

  def tableExists(sparkSession: Any, dataSource: String, name: TableIdentifier): Boolean = {
    getSessionCatalog(dataSource, sparkSession).tableExists(name)
  }

  def listTables(sparkSession: Any, dataSource: String, db: String): Seq[TableIdentifier] = {
    getSessionCatalog(dataSource, sparkSession).listTables(db)
  }

  def listTables(sparkSession: Any, dataSource: String,
                 db: String, pattern: String): Seq[TableIdentifier] = {
    getSessionCatalog(dataSource, sparkSession).listTables(db, pattern)
  }

  def loadTable(sparkSession: Any, dataSource: String,
                name: TableIdentifier,
                loadPath: String,
                isOverwrite: Boolean,
                holdDDLTime: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).loadTable(name, loadPath, isOverwrite, holdDDLTime)
  }

  def refreshTable(sparkSession: Any, dataSource: String, name: TableIdentifier): Unit = {
    getSessionCatalog(dataSource, sparkSession).refreshTable(name)
  }

  def loadPartition(sparkSession: Any, dataSource: String,
                    name: TableIdentifier,
                    loadPath: String,
                    partition: TablePartitionSpec,
                    isOverwrite: Boolean,
                    holdDDLTime: Boolean,
                    inheritTableSpecs: Boolean,
                    isSkewedStoreAsSubdir: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).loadPartition(name, loadPath,
      partition, isOverwrite, holdDDLTime, inheritTableSpecs, isSkewedStoreAsSubdir)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  def createPartitions(sparkSession: Any, dataSource: String,
                       name: TableIdentifier,
                       parts: Seq[CatalogTablePartition],
                       ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).createPartitions(name, parts, ignoreIfExists)
  }

  def dropPartitions(sparkSession: Any,
                     dataSource: String,
                     name: TableIdentifier,
                     parts: Seq[TablePartitionSpec],
                     ignoreIfNotExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).dropPartitions(name, parts, ignoreIfNotExists)
  }

  // Override the specs of one or many existing table partitions, assuming they exist.
  // This assumes index i of `specs` corresponds to index i of `newSpecs`.
  def renamePartitions(sparkSession: Any, dataSource: String,
                       name: TableIdentifier,
                       specs: Seq[TablePartitionSpec],
                       newSpecs: Seq[TablePartitionSpec]): Unit = {
    getSessionCatalog(dataSource, sparkSession).renamePartitions(name, specs, newSpecs)
  }

  // Alter one or many table partitions whose specs that match those specified in `parts`,
  // assuming the partitions exist.
  //
  // Note: If the underlying implementation does not support altering a certain field,
  // this becomes a no-op.
  def alterPartitions(sparkSession: Any, dataSource: String,
                      name: TableIdentifier,
                      parts: Seq[CatalogTablePartition]): Unit = {
    getSessionCatalog(dataSource, sparkSession).alterPartitions(name, parts)
  }

  def getPartition(sparkSession: Any, dataSource: String, name: TableIdentifier,
                   spec: TablePartitionSpec): CatalogTablePartition = {
    getSessionCatalog(dataSource, sparkSession).getPartition(name, spec)
  }

  def listPartitions(sparkSession: Any, dataSource: String,
                     name: TableIdentifier,
                     partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    getSessionCatalog(dataSource, sparkSession).listPartitions(name, partialSpec)
  }

  def getTableMetadata(sparkSession: Any, dataSource: String,
                       name: TableIdentifier): CatalogTable = {
    getSessionCatalog(dataSource, sparkSession).getTableMetadata(name)
  }

  def getTableMetadataOption(sparkSession: Any, dataSource: String,
                             name: TableIdentifier): Option[CatalogTable] = {
    getSessionCatalog(dataSource, sparkSession).getTableMetadataOption(name)
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  def createFunction(sparkSession: Any, dataSource: String,
                     funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).createFunction(funcDefinition, ignoreIfExists)
  }

  def dropFunction(sparkSession: Any, dataSource: String,
                   name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sparkSession).dropFunction(name, ignoreIfNotExists)
  }

  def getFunctionMetadata(sparkSession: Any, dataSource: String,
                          name: FunctionIdentifier): CatalogFunction = {
    getSessionCatalog(dataSource, sparkSession).getFunctionMetadata(name)
  }

  def functionExists(sparkSession: Any, dataSource: String, name: FunctionIdentifier): Boolean = {
    getSessionCatalog(dataSource, sparkSession).functionExists(name)
  }

  def lookupFunction(sparkSession: Any, dataSource: String,
                     name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    getSessionCatalog(dataSource, sparkSession).lookupFunction(name, children)
  }

  def lookupFunctionInfo(sparkSession: Any, dataSource: String,
                     name: FunctionIdentifier): ExpressionInfo = {
    getSessionCatalog(dataSource, sparkSession).lookupFunctionInfo(name)
  }

  def listFunctions(sparkSession: Any, dataSource: String,
                    db: String, pattern: String): Seq[(FunctionIdentifier, String)] = {
    getSessionCatalog(dataSource, sparkSession).listFunctions(db, pattern)
  }

  def addJar(sparkSession: Any, dataSource: String, path: String): Unit = {
    getSessionCatalog(dataSource, sparkSession).addJar(path)
  }
}
