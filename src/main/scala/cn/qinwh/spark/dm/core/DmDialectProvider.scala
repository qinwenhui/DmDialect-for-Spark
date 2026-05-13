package cn.qinwh.spark.dm.core

import java.sql.{Connection, SQLException, Statement}
import java.util.Locale

import cn.qinwh.spark.dm.config.{DmConfigParser, DmDialectConfig}
import cn.qinwh.spark.dm.utils.{DmConstants, DmLogger}

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

/**
 * 达梦方言 SPI 代理类
 *
 * 通过 Java ServiceLoader 注册的方言入口点。在 `createConnectionFactory` 中
 * 完成配置解析和版本选择，后续所有调用委托给 Dm7Dialect 或 Dm8Dialect。
 *
 * ==工作流程==
 * 1. Spark 通过 ServiceLoader 加载本类
 * 2. `canHandle(url)` 匹配 `jdbc:dm://` 前缀的 URL
 * 3. `createConnectionFactory` 解析 JDBCOptions，选择 Dm7Dialect 或 Dm8Dialect
 * 4. 后续所有调用委托给具体的版本方言实现
 *
 * @author qinwh
 */
class DmDialectProvider extends JdbcDialect {

  @volatile private var _logger: DmLogger = DmLogger()

  /** 版本方言代理实例（在 createConnectionFactory 中初始化） */
  @volatile private var _delegate: Option[DmDialect] = None

  private val initLock = new Object()

  // ======================== 方言识别 ========================

  override def canHandle(url: String): Boolean = {
    val result = url != null && url.toLowerCase(Locale.ROOT).startsWith(DmConstants.JDBC_URL_PREFIX)
    if (result) _logger.debug(s"匹配到达梦 JDBC URL: $url")
    result
  }

  // ======================== 代理初始化 ========================

  /**
   * 确保委托方言已初始化
   *
   * 优先在 createConnectionFactory 中通过 JDBCOptions 初始化；
   * 如果尚未初始化（如仅调用类型映射方法），使用默认配置 DM8。
   */
  private def ensureInitialized(): DmDialect = {
    if (_delegate.isEmpty) {
      initLock.synchronized {
        if (_delegate.isEmpty) {
          val dialect = Dm8Dialect(DmDialectConfig.defaultConfig)
          _logger.info(s"达梦方言延迟初始化: ${dialect.getClass.getSimpleName}")
          _delegate = Some(dialect)
        }
      }
    }
    _delegate.get
  }

  private def delegate: DmDialect = ensureInitialized()

  // ======================== 类型映射委托 ========================

  override def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder
  ): Option[DataType] = delegate.getCatalystType(sqlType, typeName, size, md)

  override def getJDBCType(dt: DataType): Option[JdbcType] = delegate.getJDBCType(dt)

  // ======================== 标识符委托 ========================

  override def quoteIdentifier(colName: String): String = delegate.quoteIdentifier(colName)

  // ======================== 表操作委托 ========================

  override def getTableExistsQuery(table: String): String = delegate.getTableExistsQuery(table)
  override def getSchemaQuery(table: String): String = delegate.getSchemaQuery(table)
  override def getTruncateQuery(table: String): String = delegate.getTruncateQuery(table)
  override def getTruncateQuery(
    table: String, cascade: Option[Boolean]
  ): String = delegate.getTruncateQuery(table, cascade)
  override def isCascadingTruncateTable(): Option[Boolean] = delegate.isCascadingTruncateTable()

  // ======================== 分页委托 ========================

  override def supportsLimit: Boolean = true
  override def getLimitClause(limit: Integer): String = delegate.getLimitClause(limit)
  override def supportsOffset: Boolean = true
  override def getOffsetClause(offset: Integer): String = delegate.getOffsetClause(offset)

  // ======================== 表采样委托 ========================

  override def supportsTableSample: Boolean = true
  override def getTableSample(sample: TableSampleInfo): String = delegate.getTableSample(sample)
  override def supportsJoin: Boolean = true

  // ======================== CREATE TABLE 委托 ========================

  override def createTable(
    statement: Statement,
    tableName: String,
    strSchema: String,
    options: JdbcOptionsInWrite
  ): Unit = delegate.createTable(statement, tableName, strSchema, options)

  // ======================== INSERT / DROP / RENAME 委托 ========================

  override def insertIntoTable(table: String, fields: Array[StructField]): String =
    delegate.insertIntoTable(table, fields)
  override def dropTable(table: String): String = delegate.dropTable(table)
  override def renameTable(oldTable: String, newTable: String): String =
    delegate.renameTable(oldTable, newTable)

  // ======================== ALTER TABLE 委托 ========================

  override def alterTable(
    tableName: String, changes: Seq[TableChange], dbMajorVersion: Int
  ): Array[String] = delegate.alterTable(tableName, changes, dbMajorVersion)

  override def getAddColumnQuery(tableName: String, columnName: String, dataType: String): String =
    delegate.getAddColumnQuery(tableName, columnName, dataType)

  override def getRenameColumnQuery(
    tableName: String, columnName: String, newName: String, dbMajorVersion: Int
  ): String = delegate.getRenameColumnQuery(tableName, columnName, newName, dbMajorVersion)

  override def getDeleteColumnQuery(tableName: String, columnName: String): String =
    delegate.getDeleteColumnQuery(tableName, columnName)

  override def getUpdateColumnTypeQuery(
    tableName: String, columnName: String, newDataType: String
  ): String = delegate.getUpdateColumnTypeQuery(tableName, columnName, newDataType)

  override def getUpdateColumnNullabilityQuery(
    tableName: String, columnName: String, isNullable: Boolean
  ): String = delegate.getUpdateColumnNullabilityQuery(tableName, columnName, isNullable)

  // ======================== 注释委托 ========================

  override def getTableCommentQuery(table: String, comment: String): String =
    delegate.getTableCommentQuery(table, comment)

  // ======================== 函数支持委托 ========================

  override def isSupportedFunction(funcName: String): Boolean =
    delegate.isSupportedFunction(funcName)

  // ======================== 异常分类委托 ========================

  override def classifyException(
    e: Throwable, condition: String,
    messageParameters: Map[String, String], description: String, isRuntime: Boolean
  ): Throwable with SparkThrowable =
    delegate.classifyException(e, condition, messageParameters, description, isRuntime)

  @deprecated("Use classifyException with error condition", "4.0.0")
  override def classifyException(message: String, e: Throwable): AnalysisException =
    delegate.classifyException(message, e)

  override def isSyntaxErrorBestEffort(exception: SQLException): Boolean =
    delegate.isSyntaxErrorBestEffort(exception)

  override def isObjectNotFoundException(e: SQLException): Boolean =
    delegate.isObjectNotFoundException(e)

  // ======================== 连接优化委托 ========================

  override def beforeFetch(connection: Connection, properties: Map[String, String]): Unit =
    delegate.beforeFetch(connection, properties)

  // ======================== 值编译委托 ========================

  override def compileValue(value: Any): Any = delegate.compileValue(value)

  // ======================== 时间戳转换委托 ========================

  override def convertJavaTimestampToTimestampNTZ(
    t: java.sql.Timestamp
  ): java.time.LocalDateTime = delegate.convertJavaTimestampToTimestampNTZ(t)

  override def convertTimestampNTZToJavaTimestamp(
    ldt: java.time.LocalDateTime
  ): java.sql.Timestamp = delegate.convertTimestampNTZToJavaTimestamp(ldt)

  // ======================== 连接工厂委托 ========================

  /**
   * 创建达梦 JDBC 连接工厂
   *
   * 这是 Provider 的核心初始化点：
   * 从 JDBCOptions 解析配置，根据 versionHint 选择 Dm7Dialect 或 Dm8Dialect。
   */
  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    val scalaOptions = options.parameters.toMap
    val config = DmConfigParser.parse(scalaOptions)
    _logger = DmLogger(config.loggingEnabled, config.logQueries)

    val versionHint = scalaOptions.getOrElse("spark.dmdialect.versionHint", "8")
    val dialect = versionHint match {
      case "7" =>
        _logger.info("根据配置提示使用 DM7 方言")
        Dm7Dialect(config)
      case _ =>
        _logger.info("根据配置提示使用 DM8 方言")
        Dm8Dialect(config)
    }

    _delegate = Some(dialect)
    _logger.info(s"达梦方言已初始化: ${dialect.getClass.getSimpleName}")
    dialect.createConnectionFactory(options)
  }

  override def toString: String = s"DmDialectProvider(delegate=${_delegate.getOrElse("未初始化")})"
}
