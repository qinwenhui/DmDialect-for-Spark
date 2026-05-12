package cn.qinwh.spark.dm.core

import java.sql.{Connection, SQLException, Statement}
import java.util.Locale

import cn.qinwh.spark.dm.config.{DmConfigParser, DmDialectConfig}
import cn.qinwh.spark.dm.utils.{DmConstants, DmLogger}
import cn.qinwh.spark.dm.version.DmVersionDetector

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
 * 这是通过 Java ServiceLoader 注册的方言入口点。
 * 在 Spark 调用 `canHandle(url)` 时匹配 `jdbc:dm://` 前缀的 URL，
 * 并在首次需要具体操作时根据配置提示创建 Dm7Dialect 或 Dm8Dialect 代理实例。
 *
 * ==SPI 注册==
 * 该类已在 `META-INF/services/org.apache.spark.sql.jdbc.JdbcDialect` 中注册。
 *
 * ==工作流程==
 * 1. Spark 通过 ServiceLoader 加载本类
 * 2. `canHandle(url)` 匹配 `jdbc:dm://` 前缀的 URL
 * 3. 首次需要类型映射/SQL 生成/连接时，根据配置解析创建代理方言
 * 4. 后续所有调用委托给具体的版本方言实现
 *
 * @author qinwh
 */
class DmDialectProvider extends JdbcDialect {

  @volatile private var _logger: DmLogger = DmLogger()

  /** 延迟初始化的版本方言代理实例 */
  @volatile private var _delegate: Option[DmDialect] = None

  private val initLock = new Object()

  // ======================== 方言识别 ========================

  override def canHandle(url: String): Boolean = {
    val result = url != null && url.toLowerCase(Locale.ROOT).startsWith(DmConstants.JDBC_URL_PREFIX)
    if (result) _logger.debug(s"匹配到达梦 JDBC URL: $url")
    result
  }

  // ======================== 代理初始化 ========================

  private def ensureInitialized(jdbcOptions: Option[Map[String, String]] = None): DmDialect = {
    if (_delegate.isEmpty) {
      initLock.synchronized {
        if (_delegate.isEmpty) {
          val options = jdbcOptions.getOrElse(Map.empty)

          // 解析配置
          val config = DmConfigParser.parse(options)
          _logger = DmLogger(config.loggingEnabled, config.logQueries)

          // 根据配置提示选择版本
          val versionHint = options.getOrElse("spark.dmdialect.versionHint", "8")
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
        }
      }
    }
    _delegate.getOrElse {
      // 无配置时默认使用 DM8
      val defaultDialect = Dm8Dialect(DmDialectConfig.defaultConfig)
      _delegate = Some(defaultDialect)
      defaultDialect
    }
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
  ): Unit = {
    import scala.collection.JavaConverters._
    val scalaOptions = options.parameters.asScala.toMap
    ensureInitialized(Some(scalaOptions)).createTable(statement, tableName, strSchema, options)
  }

  // ======================== INSERT 委托 ========================

  override def insertIntoTable(table: String, fields: Array[StructField]): String =
    delegate.insertIntoTable(table, fields)

  // ======================== DROP TABLE 委托 ========================

  override def dropTable(table: String): String = delegate.dropTable(table)

  override def renameTable(oldTable: String, newTable: String): String =
    delegate.renameTable(oldTable, newTable)

  // ======================== ALTER TABLE 委托 ========================

  override def alterTable(
    tableName: String,
    changes: Seq[TableChange],
    dbMajorVersion: Int
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

  // ======================== 表注释委托 ========================

  override def getTableCommentQuery(table: String, comment: String): String =
    delegate.getTableCommentQuery(table, comment)

  // ======================== 函数支持委托 ========================

  override def isSupportedFunction(funcName: String): Boolean =
    delegate.isSupportedFunction(funcName)

  // ======================== 异常分类委托 ========================

  override def classifyException(
    e: Throwable,
    condition: String,
    messageParameters: Map[String, String],
    description: String,
    isRuntime: Boolean
  ): Throwable with SparkThrowable =
    delegate.classifyException(e, condition, messageParameters, description, isRuntime)

  @deprecated("Use classifyException with error condition", "4.0.0")
  override def classifyException(message: String, e: Throwable): AnalysisException =
    delegate.classifyException(message, e)

  override def isSyntaxErrorBestEffort(exception: SQLException): Boolean =
    delegate.isSyntaxErrorBestEffort(exception)

  override def isObjectNotFoundException(e: SQLException): Boolean =
    delegate.isObjectNotFoundException(e)

  // ======================== 连接工厂委托 ========================

  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    import scala.collection.JavaConverters._
    val scalaOptions = options.parameters.asScala.toMap
    ensureInitialized(Some(scalaOptions)).createConnectionFactory(options)
  }

  override def toString: String = s"DmDialectProvider(delegate=${_delegate.getOrElse("未初始化")})"
}
