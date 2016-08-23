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

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  def createDatabase(sessionCatalog: SessionCatalog, dataSource: String,
                     dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).createDatabase(dbDefinition, ignoreIfExists)
  }

  def dropDatabase(sessionCatalog: SessionCatalog, dataSource: String,
    db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).dropDatabase(db, ignoreIfNotExists, cascade)
  }

  // Alter a database whose name matches the one specified in `dbDefinition`,
  // assuming the database exists.
  //
  // Note: If the underlying implementation does not support altering a certain field,
  // this becomes a no-op.
  def alterDatabase(sessionCatalog: SessionCatalog, dataSource: String,
    dbDefinition: CatalogDatabase): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).alterDatabase(dbDefinition)
  }

  def getDatabaseMetadata(sessionCatalog: SessionCatalog, dataSource: String,
    db: String): CatalogDatabase = {
    getSessionCatalog(dataSource, sessionCatalog).getDatabaseMetadata(db)
  }

  def databaseExists(sessionCatalog: SessionCatalog, dataSource: String, db: String): Boolean = {
    getSessionCatalog(dataSource, sessionCatalog).databaseExists(db)
  }

  def listDatabases(sessionCatalog: SessionCatalog, dataSource: String): Seq[String] = {
    getSessionCatalog(dataSource, sessionCatalog).listDatabases()
  }

  def listDatabases(sessionCatalog: SessionCatalog, dataSource: String,
                    pattern: String): Seq[String] = {
    getSessionCatalog(dataSource, sessionCatalog).listDatabases(pattern)
  }

  def getCurrentDatabase(sessionCatalog: SessionCatalog, dataSource: String): String = {
    getSessionCatalog(dataSource, sessionCatalog).getCurrentDatabase
  }

  def setCurrentDatabase(sessionCatalog: SessionCatalog, dataSource: String, db: String): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).setCurrentDatabase(db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  def createTable(sessionCatalog: SessionCatalog, dataSource: String, tableDefinition: CatalogTable,
                  ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).createTable(tableDefinition, ignoreIfExists)
  }

  def dropTable(sessionCatalog: SessionCatalog, dataSource: String, table: TableIdentifier,
                ignoreIfNotExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).dropTable(table, ignoreIfNotExists)
  }

  def renameTable(sessionCatalog: SessionCatalog, dataSource: String, oldName: TableIdentifier,
                  newName: TableIdentifier): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).renameTable(oldName, newName)
  }

  // Alter a table whose name that matches the one specified in `tableDefinition`,
  // assuming the table exists.
  //
  // Note: If the underlying implementation does not support altering a certain field,
  // this becomes a no-op.
  def alterTable(sessionCatalog: SessionCatalog, dataSource: String,
                 tableDefinition: CatalogTable): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).alterTable(tableDefinition)
  }

  def lookupRelation(sessionCatalog: SessionCatalog, dataSource: String, name: TableIdentifier,
                     alias: Option[String] = None): LogicalPlan = {
    getSessionCatalog(dataSource, sessionCatalog).lookupRelation(name, alias)
  }

  def tableExists(sessionCatalog: SessionCatalog, dataSource: String,
                  name: TableIdentifier): Boolean = {
    getSessionCatalog(dataSource, sessionCatalog).tableExists(name)
  }

  def listTables(sessionCatalog: SessionCatalog, dataSource: String,
                 db: String): Seq[TableIdentifier] = {
    getSessionCatalog(dataSource, sessionCatalog).listTables(db)
  }

  def listTables(sessionCatalog: SessionCatalog, dataSource: String,
                 db: String, pattern: String): Seq[TableIdentifier] = {
    getSessionCatalog(dataSource, sessionCatalog).listTables(db, pattern)
  }

  def loadTable(sessionCatalog: SessionCatalog, dataSource: String,
                name: TableIdentifier,
                loadPath: String,
                isOverwrite: Boolean,
                holdDDLTime: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog)
      .loadTable(name, loadPath, isOverwrite, holdDDLTime)
  }

  def refreshTable(sessionCatalog: SessionCatalog, dataSource: String,
                   name: TableIdentifier): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).refreshTable(name)
  }

  def loadPartition(sessionCatalog: SessionCatalog, dataSource: String,
                    name: TableIdentifier,
                    loadPath: String,
                    partition: TablePartitionSpec,
                    isOverwrite: Boolean,
                    holdDDLTime: Boolean,
                    inheritTableSpecs: Boolean,
                    isSkewedStoreAsSubdir: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).loadPartition(name, loadPath,
      partition, isOverwrite, holdDDLTime, inheritTableSpecs, isSkewedStoreAsSubdir)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  def createPartitions(sessionCatalog: SessionCatalog, dataSource: String,
                       name: TableIdentifier,
                       parts: Seq[CatalogTablePartition],
                       ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).createPartitions(name, parts, ignoreIfExists)
  }

  def dropPartitions(sessionCatalog: SessionCatalog,
                     dataSource: String,
                     name: TableIdentifier,
                     parts: Seq[TablePartitionSpec],
                     ignoreIfNotExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).dropPartitions(name, parts, ignoreIfNotExists)
  }

  // Override the specs of one or many existing table partitions, assuming they exist.
  // This assumes index i of `specs` corresponds to index i of `newSpecs`.
  def renamePartitions(sessionCatalog: SessionCatalog, dataSource: String,
                       name: TableIdentifier,
                       specs: Seq[TablePartitionSpec],
                       newSpecs: Seq[TablePartitionSpec]): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).renamePartitions(name, specs, newSpecs)
  }

  // Alter one or many table partitions whose specs that match those specified in `parts`,
  // assuming the partitions exist.
  //
  // Note: If the underlying implementation does not support altering a certain field,
  // this becomes a no-op.
  def alterPartitions(sessionCatalog: SessionCatalog, dataSource: String,
                      name: TableIdentifier,
                      parts: Seq[CatalogTablePartition]): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).alterPartitions(name, parts)
  }

  def getPartition(sessionCatalog: SessionCatalog, dataSource: String, name: TableIdentifier,
                   spec: TablePartitionSpec): CatalogTablePartition = {
    getSessionCatalog(dataSource, sessionCatalog).getPartition(name, spec)
  }

  def listPartitions(sessionCatalog: SessionCatalog, dataSource: String,
                     name: TableIdentifier,
                     partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    getSessionCatalog(dataSource, sessionCatalog).listPartitions(name, partialSpec)
  }

  def getTableMetadata(sessionCatalog: SessionCatalog, dataSource: String,
                       name: TableIdentifier): CatalogTable = {
    getSessionCatalog(dataSource, sessionCatalog).getTableMetadata(name)
  }

  def getTableMetadataOption(sessionCatalog: SessionCatalog, dataSource: String,
                             name: TableIdentifier): Option[CatalogTable] = {
    getSessionCatalog(dataSource, sessionCatalog).getTableMetadataOption(name)
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  def createFunction(sessionCatalog: SessionCatalog, dataSource: String,
                     funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).createFunction(funcDefinition, ignoreIfExists)
  }

  def dropFunction(sessionCatalog: SessionCatalog, dataSource: String,
                   name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).dropFunction(name, ignoreIfNotExists)
  }

  def getFunctionMetadata(sessionCatalog: SessionCatalog, dataSource: String,
                          name: FunctionIdentifier): CatalogFunction = {
    getSessionCatalog(dataSource, sessionCatalog).getFunctionMetadata(name)
  }

  def functionExists(sessionCatalog: SessionCatalog, dataSource: String,
                     name: FunctionIdentifier): Boolean = {
    getSessionCatalog(dataSource, sessionCatalog).functionExists(name)
  }

  def lookupFunction(sessionCatalog: SessionCatalog, dataSource: String,
                     name: FunctionIdentifier, children: Seq[Expression]): Expression = {
    getSessionCatalog(dataSource, sessionCatalog).lookupFunction(name, children)
  }

  def lookupFunctionInfo(sessionCatalog: SessionCatalog, dataSource: String,
                     name: FunctionIdentifier): ExpressionInfo = {
    getSessionCatalog(dataSource, sessionCatalog).lookupFunctionInfo(name)
  }

  def listFunctions(sessionCatalog: SessionCatalog, dataSource: String,
                    db: String, pattern: String): Seq[(FunctionIdentifier, String)] = {
    getSessionCatalog(dataSource, sessionCatalog).listFunctions(db, pattern)
  }

  def addJar(sessionCatalog: SessionCatalog, dataSource: String, path: String): Unit = {
    getSessionCatalog(dataSource, sessionCatalog).addJar(path)
  }
}
