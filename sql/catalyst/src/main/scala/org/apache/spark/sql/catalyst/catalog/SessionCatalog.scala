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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystConf, SimpleCatalystConf}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.StringUtils

object SessionCatalog {
  val DEFAULT_DATABASE = "default"
  val DEFAULT_DATASOURCE = "in-memory"
  val HIVE_EXTERNAL_CATALOG_CLASS_NAME = "org.apache.spark.sql.hive.HiveExternalCatalog"
}

/**
 * An internal catalog that is used by a Spark Session. This internal catalog serves as a
 * proxy to the underlying metastore (e.g. Hive Metastore) and it also manages temporary
 * tables and functions of the Spark Session that it belongs to.
 *
 * This class must be thread-safe.
 */
class SessionCatalog(
    private[sql] val sparkSession: Any,
    val internalCatalog: InternalCatalog,
    private[sql] val functionResourceLoader: FunctionResourceLoader,
    private[sql] val functionRegistry: FunctionRegistry,
    private[sql] val conf: CatalystConf,
    private[sql] val hadoopConf: Configuration) extends Logging {
  self =>

  import SessionCatalog._
  import CatalogTypes.TablePartitionSpec

  // For testing only.
  def this(
      internalCatalog: InternalCatalog,
      functionRegistry: FunctionRegistry,
      conf: CatalystConf) {
    this(
      None,
      internalCatalog,
      DummyFunctionResourceLoader,
      functionRegistry,
      conf,
      new Configuration())
  }

  def this(
            externalCatalog: ExternalCatalog,
            functionRegistry: FunctionRegistry,
            conf: CatalystConf) {
    this(
      None,
      new InternalCatalog(externalCatalog),
      DummyFunctionResourceLoader,
      functionRegistry,
      conf,
      new Configuration())
  }


  def this( sparkSession: Any,
    internalCatalog: InternalCatalog,
    functionRegistry: FunctionRegistry,
    conf: CatalystConf) {
    this(
      sparkSession,
      internalCatalog,
      DummyFunctionResourceLoader,
      functionRegistry,
      conf,
      new Configuration())
  }

  def this( sparkSession: Any,
            externalCatalog: ExternalCatalog,
            functionRegistry: FunctionRegistry,
            conf: CatalystConf) {
    this(
      sparkSession,
      new InternalCatalog(externalCatalog),
      DummyFunctionResourceLoader,
      functionRegistry,
      conf,
      new Configuration())
  }

  // For testing only.
  def this(sparkSession: Any, internalCatalog: InternalCatalog) {
    this(sparkSession, internalCatalog, new SimpleFunctionRegistry, new SimpleCatalystConf(true))
  }

  def this(internalCatalog: InternalCatalog) {
    this(None, internalCatalog)
  }

  def this(externalCatalog: ExternalCatalog) {
    this(new InternalCatalog(externalCatalog))
  }

  /** List of temporary tables, mapping from table name to their logical plan. */
  @GuardedBy("this")
  protected val tempTables = new mutable.HashMap[String, LogicalPlan]

  private[sql] var _currentDataSource: String = DEFAULT_DATASOURCE

  private[sql] def getCurrentDataSourceSessionCatalog = {
    _currentSessionCatalog.getOrElse {
      setCurrentSessionCatalog()
      _currentSessionCatalog.get
    }
  }

  private[sql] var _currentSessionCatalog: Option[DataSourceSessionCatalog] = None

  /**
   * Format table name, taking into account case sensitivity.
   */
  protected[this] def formatTableName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase
  }

  /**
   * Format database name, taking into account case sensitivity.
   */
  protected[this] def formatDatabaseName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase
  }

  /**
   * Format data source name, taking into account case sensitivity.
   */
  protected[this] def formatDataSourceName(name: String): String = {
    if (conf.caseSensitiveAnalysis) name else name.toLowerCase
  }

  /**
   * This method is used to make the given path qualified before we
   * store this path in the underlying external catalog. So, when a path
   * does not contain a scheme, this path will not be changed after the default
   * FileSystem is changed.
   */
  private def makeQualifiedPath(path: String): Path = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    fs.makeQualified(hadoopPath)
  }

  private def requireDataSourceExists(name: String): Unit = {
    if (!internalCatalog.dsExists(name)) {
      throw new NoSuchDataSourceException(name)
    }
  }

  // ----------------------------------------------------------------------------
  // Databases
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // ----------------------------------------------------------------------------

  def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    getCurrentDataSourceSessionCatalog.createDatabase(dbDefinition, ignoreIfExists)
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    getCurrentDataSourceSessionCatalog.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    getCurrentDataSourceSessionCatalog.alterDatabase(dbDefinition)
  }

  def getDatabaseMetadata(db: String): CatalogDatabase = {
    getCurrentDataSourceSessionCatalog.getDatabaseMetadata(db)
  }

  def databaseExists(db: String): Boolean = {
    getCurrentDataSourceSessionCatalog.databaseExists(db)
  }

  def dataSourceExists(dataSource: String): Boolean = {
    getCurrentDataSourceSessionCatalog.dataSourceExists(dataSource)
  }

  def listDatabases(): Seq[String] = {
    getCurrentDataSourceSessionCatalog.listDatabases()
  }

  def listDatabases(pattern: String): Seq[String] = {
    getCurrentDataSourceSessionCatalog.listDatabases(pattern)
  }

  def getCurrentDatabase: String = {
    // caution: has to be used with the current data source properly set up
    // Otherwise may experience inconsistent meta data objects
    getCurrentDataSourceSessionCatalog.getCurrentDatabase
  }

  def setCurrentDatabase(db: String): Unit = {
    // requireDbExists(dbName)
    getCurrentDataSourceSessionCatalog.setCurrentDatabase(db)
  }

  def getCurrentDataSource: String = {
    _currentDataSource
  }

  def setCurrentDataSource(name: String): Unit = {
    val dataSourceName = formatDataSourceName(name)
    requireDataSourceExists(dataSourceName)
    synchronized { _currentDataSource = dataSourceName}
    setCurrentSessionCatalog()
  }

  /**
   * Get the path for creating a non-default database when database location is not provided
   * by users.
   */
  def getDefaultDBPath(db: String): String = {
    val database = formatDatabaseName(db)
    new Path(new Path(conf.warehousePath), database + ".db").toString
  }

  // ----------------------------------------------------------------------------
  // Tables
  // ----------------------------------------------------------------------------
  // There are two kinds of tables, temporary tables and metastore tables.
  // Temporary tables are isolated across sessions and do not belong to any
  // particular database. Metastore tables can be used across multiple
  // sessions as their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // ----------------------------------------------------
  // | Methods that interact with metastore tables only |
  // ----------------------------------------------------

  /**
   * Create a metastore table in the database specified in `tableDefinition`.
   * If no such database is specified, create it in the current database.
   */
  def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val dataSource = tableDefinition.identifier.dataSource
    if (dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.createTable(tableDefinition, ignoreIfExists)
    } else {
      getDataSourceSessionCatalog(dataSource.get).createTable(tableDefinition, ignoreIfExists)
    }
  }

  /**
   * Alter the metadata of an existing metastore table identified by `tableDefinition`.
   *
   * If no database is specified in `tableDefinition`, assume the table is in the
   * current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterTable(tableDefinition: CatalogTable): Unit = {
    val dataSource = tableDefinition.identifier.dataSource
    if (dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.alterTable(tableDefinition)
    } else {
      getDataSourceSessionCatalog(dataSource.get).alterTable(tableDefinition)
    }
  }

  /**
   * Retrieve the metadata of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    val tid = TableIdentifier(table)
    if (isTemporaryTable(name)) {
      CatalogTable(
        identifier = tid,
        tableType = CatalogTableType.VIEW,
        storage = CatalogStorageFormat.empty,
        schema = tempTables(table).output.toStructType,
        properties = Map(),
        viewText = None)
    } else if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.getTableMetadata(name)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).getTableMetadata(name)
    }
  }

  /**
   * Retrieve the metadata of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then return None if it doesn't exist.
   */
  def getTableMetadataOption(name: TableIdentifier): Option[CatalogTable] = {
    if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.getTableMetadataOption(name)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).getTableMetadataOption(name)
    }
  }

  /**
   * Load files stored in given path into an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  def loadTable(
      name: TableIdentifier,
      loadPath: String,
      isOverwrite: Boolean,
      holdDDLTime: Boolean): Unit = {
    if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.loadTable(name, loadPath, isOverwrite, holdDDLTime)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get)
        .loadTable(name, loadPath, isOverwrite, holdDDLTime)
    }
  }

  /**
   * Load files stored in given path into the partition of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  def loadPartition(
      name: TableIdentifier,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.loadPartition(name, loadPath,
        partition, isOverwrite, holdDDLTime, inheritTableSpecs)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).loadPartition(name, loadPath, partition,
        isOverwrite, holdDDLTime, inheritTableSpecs)
    }
  }

  def defaultTablePath(tableIdent: TableIdentifier): String = {
    val dbName = formatDatabaseName(tableIdent.database.getOrElse(getCurrentDatabase))
    val dbLocation = getDatabaseMetadata(dbName).locationUri

    new Path(new Path(dbLocation), formatTableName(tableIdent.table)).toString
  }

  // -------------------------------------------------------------
  // | Methods that interact with temporary and metastore tables |
  // -------------------------------------------------------------

  /**
   * Create a temporary table.
   */
  def createTempView(
      name: String,
      tableDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit = synchronized {
    val table = formatTableName(name)
    if (tempTables.contains(table) && !overrideIfExists) {
      throw new TempTableAlreadyExistsException(name)
    }
    tempTables.put(table, tableDefinition)
  }

  /**
   * Rename a table.
   *
   * If a database is specified in `oldName`, this will rename the table in that database.
   * If no database is specified, this will first attempt to rename a temporary table with
   * the same name, then, if that does not exist, rename the table in the current database.
   */
  def renameTable(oldName: TableIdentifier, newName: String): Unit = synchronized {
    val oldTableName = formatTableName(oldName.table)
    val newTableName = formatTableName(newName)
    if (!oldName.dataSource.isDefined && !oldName.database.isDefined &&
      tempTables.contains(oldTableName)) {
      if (tempTables.contains(newTableName)) {
        throw new AnalysisException(
          s"RENAME TEMPORARY TABLE from '$oldName' to '$newName': destination table already exists")
      }
      val table = tempTables(oldTableName)
      tempTables.remove(oldTableName)
      tempTables.put(newTableName, table)
    } else {
      val oldDataSource = oldName.dataSource.getOrElse(_currentDataSource)
      val newDataSource = _currentDataSource
      require(oldDataSource == newDataSource,
        "data source should be same for renaming the table")
      if (oldName.dataSource.isEmpty) {
        getCurrentDataSourceSessionCatalog.renameTable(oldName, newName)
      } else {
        getDataSourceSessionCatalog(oldName.dataSource.get).renameTable(oldName, newName)
      }
    }
  }

  /**
   * Drop a table.
   *
   * If a database is specified in `name`, this will drop the table from that database.
   * If no database is specified, this will first attempt to drop a temporary table with
   * the same name, then, if that does not exist, drop the table from the current database.
   */
  def dropTable(
      name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    val table = formatTableName(name.table)
    if (!name.dataSource.isDefined && !name.database.isDefined && tempTables.contains(table)) {
      val table = formatTableName(name.table)
      tempTables.remove(table)
    } else {
      if (name.dataSource.isEmpty) {
        getCurrentDataSourceSessionCatalog.dropTable(name, ignoreIfNotExists, purge)
      } else {
        getDataSourceSessionCatalog(name.dataSource.get).dropTable(name, ignoreIfNotExists, purge)
      }
    }
  }

  /**
   * Return a [[LogicalPlan]] that represents the given table or view.
   *
   * If a database is specified in `name`, this will return the table/view from that database.
   * If no database is specified, this will first attempt to return a temporary table/view with
   * the same name, then, if that does not exist, return the table/view from the current database.
   *
   * If the relation is a view, the relation will be wrapped in a [[SubqueryAlias]] which will
   * track the name of the view.
   */
  def lookupRelation(name: TableIdentifier, alias: Option[String] = None): LogicalPlan = {
    synchronized {
      val table = formatTableName(name.table)
      if (!name.dataSource.isDefined && !name.database.isDefined &&
        tempTables.contains(table)) {
        val relationAlias = alias.getOrElse(table)
        SubqueryAlias(relationAlias, tempTables(table), Option(name))
      } else if (name.dataSource.isEmpty) {
        getCurrentDataSourceSessionCatalog.lookupRelation(name, alias)
      } else {
        getDataSourceSessionCatalog(name.dataSource.get).lookupRelation(name, alias)
      }
    }
  }

  /**
   * Return whether a table/view with the specified name exists.
   *
   * Note: If a database is explicitly specified, then this will return whether the table/view
   * exists in that particular database instead. In that case, even if there is a temporary
   * table with the same name, we will return false if the specified database does not
   * contain the table/view.
   */
  def tableExists(name: TableIdentifier): Boolean = synchronized {
    if (isTemporaryTable(name)) {
      true
    } else if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.tableExists(name)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).tableExists(name)
    }
  }

  /**
   * Return whether a table with the specified name is a temporary table.
   *
   * Note: The temporary table cache is checked only when database is not
   * explicitly specified.
   */
  def isTemporaryTable(name: TableIdentifier): Boolean = synchronized {
    name.dataSource.isEmpty && name.database.isEmpty &&
      tempTables.contains(formatTableName(name.table))
  }

  def listTablesByDataSource(dataSource: String): Seq[TableIdentifier] = {
    val ds = getDataSourceSessionCatalog(dataSource)
    ds.listTables(ds.getCurrentDatabase)
  }

  /**
   * List all tables in the specified database, including temporary tables.
   */
  def listTables(db: String): Seq[TableIdentifier] = listTables(db, "*")

  /**
   * List all matching tables in the specified database, including temporary tables.
   */
  def listTables(db: String, pattern: String): Seq[TableIdentifier] = {
    getCurrentDataSourceSessionCatalog.listTables(db, pattern) ++
      synchronized {
        val _tempTables = StringUtils.filterPattern(tempTables.keys.toSeq, pattern)
          .map { t => TableIdentifier(t) }
        _tempTables
      }
  }

  /**
   * Refresh the cache entry for a metastore table, if any.
   */
  def refreshTable(name: TableIdentifier): Unit = {
    if (name.dataSource.isEmpty && name.database.isEmpty) {
      tempTables.get(formatTableName(name.table)).foreach(_.refresh())
    }
    if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.refreshTable(name)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).refreshTable(name)
    }
  }

  /**
   * Drop all existing temporary tables.
   * For testing only.
   */
  def clearTempTables(): Unit = synchronized {
    tempTables.clear()
  }

  /**
   * Return a temporary table exactly as it was stored.
   * For testing only.
   */
  private[catalog] def getTempTable(name: String): Option[LogicalPlan] = synchronized {
    tempTables.get(formatTableName(name))
  }

  // ----------------------------------------------------------------------------
  // Partitions
  // ----------------------------------------------------------------------------
  // All methods in this category interact directly with the underlying catalog.
  // These methods are concerned with only metastore tables.
  // ----------------------------------------------------------------------------

  // TODO: We need to figure out how these methods interact with our data source
  // tables. For such tables, we do not store values of partitioning columns in
  // the metastore. For now, partition values of a data source table will be
  // automatically discovered when we load the table.

  /**
   * Create partitions in an existing table, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    if (tableName.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.createPartitions(tableName, parts, ignoreIfExists)
    } else {
      getDataSourceSessionCatalog(tableName.dataSource.get)
        .createPartitions(tableName, parts, ignoreIfExists)
    }
  }

  /**
   * Drop partitions from a table, assuming they exist.
   * If no database is specified, assume the table is in the current database.
   */
  def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    if (tableName.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.dropPartitions(tableName, specs, ignoreIfNotExists, purge)
    } else {
      getDataSourceSessionCatalog(tableName.dataSource.get)
        .dropPartitions(tableName, specs, ignoreIfNotExists, purge)
    }
  }

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   *
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   * If no database is specified, assume the table is in the current database.
   */
  def renamePartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    if (tableName.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.renamePartitions(tableName, specs, newSpecs)
    } else {
      getDataSourceSessionCatalog(tableName.dataSource.get)
        .renamePartitions(tableName, specs, newSpecs)
    }
  }

  /**
   * Alter one or many table partitions whose specs that match those specified in `parts`,
   * assuming the partitions exist.
   *
   * If no database is specified, assume the table is in the current database.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  def alterPartitions(tableName: TableIdentifier, parts: Seq[CatalogTablePartition]): Unit = {
    if (tableName.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.alterPartitions(tableName, parts)
    } else {
      getDataSourceSessionCatalog(tableName.dataSource.get).alterPartitions(tableName, parts)
    }
  }

  /**
   * Retrieve the metadata of a table partition, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def getPartition(tableName: TableIdentifier, spec: TablePartitionSpec): CatalogTablePartition = {
    if (tableName.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.getPartition(tableName, spec)
    } else {
      getDataSourceSessionCatalog(tableName.dataSource.get).getPartition(tableName, spec)
    }
  }

  /**
   * List the metadata of all partitions that belong to the specified table, assuming it exists.
   *
   * A partial partition spec may optionally be provided to filter the partitions returned.
   * For instance, if there exist partitions (a='1', b='2'), (a='1', b='3') and (a='2', b='4'),
   * then a partial spec of (a='1') will return the first two only.
   */
  def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    if (tableName.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.listPartitions(tableName, partialSpec)
    } else {
      getDataSourceSessionCatalog(tableName.dataSource.get).listPartitions(tableName, partialSpec)
    }
  }

  /**
   * Verify if the input partition spec exactly matches the existing defined partition spec
   * The columns must be the same but the orders could be different.
   */
  private def requireExactMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    val defined = table.partitionColumnNames.sorted
    specs.foreach { s =>
      if (s.keys.toSeq.sorted != defined) {
        throw new AnalysisException(
          s"Partition spec is invalid. The spec (${s.keys.mkString(", ")}) must match " +
            s"the partition spec (${table.partitionColumnNames.mkString(", ")}) defined in " +
            s"table '${table.identifier}'")
      }
    }
  }

  /**
   * Verify if the input partition spec partially matches the existing defined partition spec
   * That is, the columns of partition spec should be part of the defined partition spec.
   */
  private def requirePartialMatchedPartitionSpec(
      specs: Seq[TablePartitionSpec],
      table: CatalogTable): Unit = {
    val defined = table.partitionColumnNames
    specs.foreach { s =>
      if (!s.keys.forall(defined.contains)) {
        throw new AnalysisException(
          s"Partition spec is invalid. The spec (${s.keys.mkString(", ")}) must be contained " +
            s"within the partition spec (${table.partitionColumnNames.mkString(", ")}) defined " +
            s"in table '${table.identifier}'")
      }
    }
  }

  // ----------------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------------
  // There are two kinds of functions, temporary functions and metastore
  // functions (permanent UDFs). Temporary functions are isolated across
  // sessions. Metastore functions can be used across multiple sessions as
  // their metadata is persisted in the underlying catalog.
  // ----------------------------------------------------------------------------

  // -------------------------------------------------------
  // | Methods that interact with metastore functions only |
  // -------------------------------------------------------

  /**
   * Create a metastore function in the database specified in `funcDefinition`.
   * If no such database is specified, create it in the current database.
   */
  def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val dataSource = funcDefinition.identifier.dataSource
    if (dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.createFunction(funcDefinition, ignoreIfExists)
    } else {
      getDataSourceSessionCatalog(dataSource.get).createFunction(funcDefinition, ignoreIfExists)
    }
  }

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    val datasource = name.dataSource.orElse(Some(_currentDataSource))
    val db = formatDatabaseName(name.database.getOrElse(
      getDataSourceSessionCatalog(datasource.get).getCurrentDatabase))
    val identifier = name.copy(database = Some(db))
    if (functionExists(identifier)) {
      // TODO: registry should just take in FunctionIdentifier for type safety
      if (functionRegistry.functionExists(identifier.unquotedString)) {
        // If we have loaded this function into the FunctionRegistry,
        // also drop it from there.
        // For a permanent function, because we loaded it to the FunctionRegistry
        // when it's first used, we also need to drop it from the FunctionRegistry.
        functionRegistry.dropFunction(identifier.unquotedString)
      }
      if (name.dataSource.isEmpty) {
        getCurrentDataSourceSessionCatalog.dropFunction(name, ignoreIfNotExists)
      } else {
        getDataSourceSessionCatalog(name.dataSource.get).dropFunction(name, ignoreIfNotExists)
      }
    } else if (!ignoreIfNotExists) {
      throw new NoSuchFunctionException(datasource =
        name.dataSource.getOrElse(_currentDataSource), db = db, func = identifier.toString)
    }
  }

  /**
   * Retrieve the metadata of a metastore function.
   *
   * If a database is specified in `name`, this will return the function in that database.
   * If no database is specified, this will return the function in the current database.
   */
  def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.getFunctionMetadata(name)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).getFunctionMetadata(name)
    }
  }

  /**
   * Check if the specified function exists.
   */
  def functionExists(name: FunctionIdentifier): Boolean = {
    functionRegistry.functionExists(name.unquotedString) || {
      if (name.dataSource.isEmpty) {
        getCurrentDataSourceSessionCatalog.functionExists(name)
      } else {
        getDataSourceSessionCatalog(name.dataSource.get).functionExists(name)
      }
    }
  }

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  /**
   * Construct a [[FunctionBuilder]] based on the provided class that represents a function.
   *
   * This performs reflection to decide what type of [[Expression]] to return in the builder.
   */
  def makeFunctionBuilder(name: String, functionClassName: String): FunctionBuilder = {
    getCurrentDataSourceSessionCatalog.makeFunctionBuilder(name, functionClassName)
  }

  /**
   * Loads resources such as JARs and Files for a function. Every resource is represented
   * by a tuple (resource type, resource uri).
   */
  def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    resources.foreach(functionResourceLoader.loadResource)
  }

  /**
   * Create a temporary function.
   * This assumes no database is specified in `funcDefinition`.
   */
  def createTempFunction(
      name: String,
      info: ExpressionInfo,
      funcDefinition: FunctionBuilder,
      ignoreIfExists: Boolean): Unit = {
    if (functionRegistry.lookupFunctionBuilder(name).isDefined && !ignoreIfExists) {
      throw new TempFunctionAlreadyExistsException(name)
    }
    functionRegistry.registerFunction(name, info, funcDefinition)
  }

  /**
   * Drop a temporary function.
   */
  def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit = {
    if (!functionRegistry.dropFunction(name) && !ignoreIfNotExists) {
      throw new NoSuchTempFunctionException(name)
    }
  }

  protected def failFunctionLookup(name: String): Nothing = {
    throw new NoSuchFunctionException(_currentDataSource, db = getCurrentDatabase, func = name)
  }

  /**
   * Look up the [[ExpressionInfo]] associated with the specified function, assuming it exists.
   */
  def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    // TODO: just make function registry take in FunctionIdentifier instead of duplicating this
    val datasource = name.dataSource.orElse(Some(_currentDataSource))
    val database = name.database.orElse(
      Some(getDataSourceSessionCatalog(datasource).getCurrentDatabase)).map(formatDatabaseName)
    val qualifiedName = name.copy(dataSource = datasource, database = database)
    functionRegistry.lookupFunction(name.funcName)
      .orElse(functionRegistry.lookupFunction(qualifiedName.unquotedString))
      .getOrElse {
        if (name.dataSource.isEmpty) {
          getCurrentDataSourceSessionCatalog.lookupFunctionInfo(name)
        } else {
          getDataSourceSessionCatalog(name.dataSource.get).lookupFunctionInfo(name)
        }
      }
  }

  /**
   * Return an [[Expression]] that represents the specified function, assuming it exists.
   *
   * For a temporary function or a permanent function that has been loaded,
   * this method will simply lookup the function through the
   * FunctionRegistry and create an expression based on the builder.
   *
   * For a permanent function that has not been loaded, we will first fetch its metadata
   * from the underlying external catalog. Then, we will load all resources associated
   * with this function (i.e. jars and files). Finally, we create a function builder
   * based on the function class and put the builder into the FunctionRegistry.
   * The name of this function in the FunctionRegistry will be `databaseName.functionName`.
   */
  def lookupFunction(
      name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    if (name.dataSource.isEmpty) {
      getCurrentDataSourceSessionCatalog.lookupFunction(name, children)
    } else {
      getDataSourceSessionCatalog(name.dataSource.get).lookupFunction(name, children)
    }
  }

  /**
   * List all functions in the specified database, including temporary functions. This
   * returns the function identifier and the scope in which it was defined (system or user
   * defined).
   */
  def listFunctions(db: String): Seq[(FunctionIdentifier, String)] = listFunctions(db, "*")

  /**
   * List all matching functions in the specified database, including temporary functions. This
   * returns the function identifier and the scope in which it was defined (system or user
   * defined).
   */
  def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] = {
    val loadedFunctions = StringUtils.filterPattern(functionRegistry.listFunction(), pattern)
      .map { f => FunctionIdentifier(f) }
    getCurrentDataSourceSessionCatalog.listFunctions(db, pattern) ++ loadedFunctions.map {
      case f if FunctionRegistry.functionSet.contains(f.funcName) => (f, "SYSTEM")
      case f => (f, "USER")
    }
  }


  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Drop all existing databases (except "default"), tables, partitions and functions,
   * and set the current database to "default".
   *
   * This is mainly used for tests.
   */
  def reset(): Unit = synchronized {
    setCurrentDatabase(DEFAULT_DATABASE)
    listDatabases().filter(_ != DEFAULT_DATABASE).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }
    listTables(DEFAULT_DATABASE).foreach { table =>
      dropTable(table, ignoreIfNotExists = false, purge = false)
    }
    listFunctions(DEFAULT_DATABASE).map(_._1).foreach { func =>
      if (func.database.isDefined) {
        dropFunction(func, ignoreIfNotExists = false)
      } else {
        dropTempFunction(func.funcName, ignoreIfNotExists = false)
      }
    }
    tempTables.clear()
    functionRegistry.clear()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }

  def addJar(path: String): Unit = {
    getCurrentDataSourceSessionCatalog.addJar(path)
  }

  def getDataSourceSessionCatalog(dataSource: String): DataSourceSessionCatalog = {
    internalCatalog.getSessionCatalog(dataSource, this)
  }

  def getDataSourceSessionCatalog(dataSource: Option[String]): DataSourceSessionCatalog = {
    dataSource.map(getDataSourceSessionCatalog(_)).getOrElse(_currentSessionCatalog.get)
  }

  private[sql] def setCurrentSessionCatalog(): Unit = {
    _currentSessionCatalog = Some(getDataSourceSessionCatalog(_currentDataSource))
  }
}
