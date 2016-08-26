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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

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
  private[sql] val tempTables = new mutable.HashMap[String, LogicalPlan]

  private[sql] var currentDataSource: String = DEFAULT_DATASOURCE

  private[sql] def currentSessionCatalog = {
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
    currentSessionCatalog.createDatabase(dbDefinition, ignoreIfExists)
  }

  def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    currentSessionCatalog.dropDatabase(db, ignoreIfNotExists, cascade)
  }

  def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    currentSessionCatalog.alterDatabase(dbDefinition)
  }

  def getDatabaseMetadata(db: String): CatalogDatabase = {
    currentSessionCatalog.getDatabaseMetadata(db)
  }

  def databaseExists(db: String): Boolean = {
    currentSessionCatalog.databaseExists(db)
  }

  def dataSourceExists(dataSource: String): Boolean = {
    currentSessionCatalog.dataSourceExists(dataSource)
  }

  def listDatabases(): Seq[String] = {
    currentSessionCatalog.listDatabases()
  }

  def listDatabases(pattern: String): Seq[String] = {
    currentSessionCatalog.listDatabases(pattern)
  }

  def getCurrentDatabase: String = {
    currentSessionCatalog.getCurrentDatabase
  }

  def setCurrentDatabase(db: String): Unit = {
    // requireDbExists(dbName)
    currentSessionCatalog.setCurrentDatabase(db)
  }

  def getCurrentDataSource: String = {
    currentSessionCatalog.getCurrentDataSource
  }

  def setCurrentDataSource(name: String): Unit = {
    val dataSourceName = formatDataSourceName(name)
    requireDataSourceExists(dataSourceName)
    synchronized { currentDataSource = dataSourceName}
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
    // requireDbExists(db)
    currentSessionCatalog.createTable(tableDefinition, ignoreIfExists)
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
    // requireDbExists(db)
    currentSessionCatalog.alterTable(tableDefinition)
  }

  /**
   * Retrieve the metadata of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then a [[NoSuchTableException]] is thrown.
   */
  def getTableMetadata(name: TableIdentifier): CatalogTable = {
    // requireDbExists(db)
    currentSessionCatalog.getTableMetadata(name)
  }

  /**
   * Retrieve the metadata of an existing metastore table.
   * If no database is specified, assume the table is in the current database.
   * If the specified table is not found in the database then return None if it doesn't exist.
   */
  def getTableMetadataOption(name: TableIdentifier): Option[CatalogTable] = {
    // requireDbExists(db)
    currentSessionCatalog.getTableMetadataOption(name)
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
    // requireDbExists(db)
    currentSessionCatalog.loadTable(name, loadPath, isOverwrite, holdDDLTime)
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
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = {
    // requireDbExists(db)
    currentSessionCatalog.loadPartition(name, loadPath, partition, isOverwrite, holdDDLTime,
      inheritTableSpecs, isSkewedStoreAsSubdir)
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
   *
   * This assumes the database specified in `oldName` matches the one specified in `newName`.
   */
  def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit = synchronized {
    currentSessionCatalog.renameTable(oldName, newName)
  }

  /**
   * Drop a table.
   *
   * If a database is specified in `name`, this will drop the table from that database.
   * If no database is specified, this will first attempt to drop a temporary table with
   * the same name, then, if that does not exist, drop the table from the current database.
   */
  def dropTable(name: TableIdentifier, ignoreIfNotExists: Boolean): Unit = synchronized {
    //  requireDbExists(db)
    currentSessionCatalog.dropTable(name, ignoreIfNotExists)
  }

  /**
   * Return a [[LogicalPlan]] that represents the given table.
   *
   * If a database is specified in `name`, this will return the table from that database.
   * If no database is specified, this will first attempt to return a temporary table with
   * the same name, then, if that does not exist, return the table from the current database.
   */
  def lookupRelation(name: TableIdentifier, alias: Option[String] = None): LogicalPlan = {
    synchronized {
      currentSessionCatalog.lookupRelation(name, alias)
    }
  }

  /**
   * Return whether a table with the specified name exists.
   *
   * Note: If a database is explicitly specified, then this will return whether the table
   * exists in that particular database instead. In that case, even if there is a temporary
   * table with the same name, we will return false if the specified database does not
   * contain the table.
   */
  def tableExists(name: TableIdentifier): Boolean = synchronized {
    currentSessionCatalog.tableExists(name)
  }

  /**
   * Return whether a table with the specified name is a temporary table.
   *
   * Note: The temporary table cache is checked only when database is not
   * explicitly specified.
   */
  def isTemporaryTable(name: TableIdentifier): Boolean = synchronized {
    name.database.isEmpty && tempTables.contains(formatTableName(name.table))
  }

  /**
   * List all tables in the specified database, including temporary tables.
   */
  def listTables(db: String): Seq[TableIdentifier] = listTables(db, "*")

  /**
   * List all matching tables in the specified database, including temporary tables.
   */
  def listTables(db: String, pattern: String): Seq[TableIdentifier] = {
    // requireDbExists(dbName)
    currentSessionCatalog.listTables(db, pattern)
  }

  /**
   * Refresh the cache entry for a metastore table, if any.
   */
  def refreshTable(name: TableIdentifier): Unit = {
    currentSessionCatalog.refreshTable(name)
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
    tempTables.get(name)
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
    // requireDbExists(db)
    currentSessionCatalog.createPartitions(tableName, parts, ignoreIfExists)
  }

  /**
   * Drop partitions from a table, assuming they exist.
   * If no database is specified, assume the table is in the current database.
   */
  def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean): Unit = {
    // requireDbExists(db)
    currentSessionCatalog.dropPartitions(tableName, specs, ignoreIfNotExists)
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
    // requireDbExists(db)
    currentSessionCatalog.renamePartitions(tableName, specs, newSpecs)
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
    // requireDbExists(db)
    currentSessionCatalog.alterPartitions(tableName, parts)
  }

  /**
   * Retrieve the metadata of a table partition, assuming it exists.
   * If no database is specified, assume the table is in the current database.
   */
  def getPartition(tableName: TableIdentifier, spec: TablePartitionSpec): CatalogTablePartition = {
    // requireDbExists(db)
    currentSessionCatalog.getPartition(tableName, spec)
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
    // requireDbExists(db)
    currentSessionCatalog.listPartitions(tableName, partialSpec)
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
    currentSessionCatalog.createFunction(funcDefinition, ignoreIfExists)
  }

  /**
   * Drop a metastore function.
   * If no database is specified, assume the function is in the current database.
   */
  def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    // requireDbExists(db)
    currentSessionCatalog.dropFunction(name, ignoreIfNotExists)
  }

  /**
   * Retrieve the metadata of a metastore function.
   *
   * If a database is specified in `name`, this will return the function in that database.
   * If no database is specified, this will return the function in the current database.
   */
  def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    // requireDbExists(db)
    currentSessionCatalog.getFunctionMetadata(name)
  }

  /**
   * Check if the specified function exists.
   */
  def functionExists(name: FunctionIdentifier): Boolean = {
    // requireDbExists(db)
    currentSessionCatalog.functionExists(name)
  }

  // ----------------------------------------------------------------
  // | Methods that interact with temporary and metastore functions |
  // ----------------------------------------------------------------

  /**
   * Construct a [[FunctionBuilder]] based on the provided class that represents a function.
   *
   * This performs reflection to decide what type of [[Expression]] to return in the builder.
   */
  private[sql] def makeFunctionBuilder(name: String, functionClassName: String): FunctionBuilder = {
    currentSessionCatalog.makeFunctionBuilder(name, functionClassName)
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
    throw new NoSuchFunctionException(currentDataSource, db = getCurrentDatabase, func = name)
  }

  /**
   * Look up the [[ExpressionInfo]] associated with the specified function, assuming it exists.
   */
  private[spark] def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    // requireDbExists(db)
    currentSessionCatalog.lookupFunctionInfo(name)
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
    currentSessionCatalog.lookupFunction(name, children)
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
    currentSessionCatalog.listFunctions(db, pattern)
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
  private[sql] def reset(): Unit = synchronized {
    setCurrentDatabase(DEFAULT_DATABASE)
    listDatabases().filter(_ != DEFAULT_DATABASE).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }
    listTables(DEFAULT_DATABASE).foreach { table =>
      dropTable(table, ignoreIfNotExists = false)
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
    currentSessionCatalog.addJar(path)
  }

  def getDataSourceSessionCatalog(dataSource: String): DataSourceSessionCatalog = {
    internalCatalog.getSessionCatalog(dataSource, this)
  }

  private[sql] def setCurrentSessionCatalog(): Unit = {
    _currentSessionCatalog = Some(getDataSourceSessionCatalog(currentDataSource))
  }
}
