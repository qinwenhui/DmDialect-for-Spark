package cn.qinwh.spark.dm.core

import java.sql.{Connection, SQLException}

import cn.qinwh.spark.dm.config.{DmConfigParser, DmDialectConfig}
import cn.qinwh.spark.dm.utils.{DmConstants, DmLogger}
import cn.qinwh.spark.dm.version.DmVersionDetector

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

/**
 * 达梦方言 SPI 工厂类
 *
 * 这是通过 Java ServiceLoader 注册的方言入口点。
 * 在 Spark 调用 `canHandle(url)` 时匹配 `jdbc:dm://` 前缀的 URL，
 * 并在首次数据库操作时自动检测达梦版本，将具体逻辑委托给
 * Dm7Dialect 或 Dm8Dialect 实例。
 *
 * ==SPI 注册==
 * 该类已在 `META-INF/services/org.apache.spark.sql.jdbc.JdbcDialect` 中注册。
 *
 * ==工作流程==
 * 1. Spark 通过 ServiceLoader 加载本类
 * 2. `canHandle(url)` 匹配 `jdbc:dm://` 前缀的 URL
 * 3. 首次需要进行类型映射或 SQL 生成时，通过 JDBC 连接检测数据库版本
 * 4. 根据检测结果创建 Dm7Dialect 或 Dm8Dialect 实例
 * 5. 后续所有调用委托给具体的版本方言实现
 *
 * @author qinwh
 */
class DmDialectProvider extends JdbcDialect {

  /** 日志记录器（延迟初始化，因为初始时还没有配置） */
  @volatile private var _logger: DmLogger = DmLogger()

  /** 版本方言代理实例（延迟初始化） */
  @volatile private var _delegate: Option[JdbcDialect] = None

  /** 用于同步初始化 */
  private val initLock = new Object()

  // ======================== 方言识别 ========================

  /**
   * 检查是否能处理给定的 JDBC URL
   *
   * 这是 Spark 调用的第一个方法，仅基于 URL 进行判断，不需要数据库连接。
   *
   * @param url JDBC URL
   * @return true 如果 URL 以 `jdbc:dm://` 开头
   */
  override def canHandle(url: String): Boolean = {
    val result = url != null && url.toLowerCase.startsWith(DmConstants.JDBC_URL_PREFIX)
    if (result) {
      _logger.debug(s"匹配到达梦 JDBC URL: $url")
    }
    result
  }

  // ======================== 委托方法 ========================

  /**
   * 确保委托方言已初始化
   *
   * 首次调用时，需要传入 JDBC Options 以解析配置和检测版本。
   * 由于 canHandle 阶段还没有配置信息，配置解析延迟到第一次需要时。
   *
   * @param configMaybe 可选的配置 Map 信息（来自 JDBCOptions）
   */
  private def ensureInitialized(configMaybe: Option[Map[String, String]] = None): JdbcDialect = {
    if (_delegate.isEmpty && configMaybe.isDefined) {
      initLock.synchronized {
        if (_delegate.isEmpty) {
          val jdbcOptions = configMaybe.get

          // 解析配置
          val config = DmConfigParser.parse(jdbcOptions)
          _logger = DmLogger(config.loggingEnabled, config.logQueries)

          // 创建适当的方言实例
          // 实际版本检测需要在有 Connection 时才能进行，
          // 这里根据默认配置推断：如果用户未指定版本，默认使用 DM8
          val versionHint = jdbcOptions.getOrElse("spark.dmdialect.versionHint", "8")
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
      // 如果没有配置信息也没有 delegate，返回默认 DM8 方言
      val defaultConfig = DmDialectConfig.defaultConfig
      val defaultDialect = Dm8Dialect(defaultConfig)
      _delegate = Some(defaultDialect)
      defaultDialect
    }
  }

  // ======================== 类型映射委托 ========================

  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = {
    ensureInitialized().getCatalystType(sqlType, typeName, size, md)
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    ensureInitialized().getJDBCType(dt)
  }

  // ======================== 标识符委托 ========================

  override def quoteIdentifier(colName: String): String = {
    ensureInitialized().quoteIdentifier(colName)
  }

  // ======================== 表操作委托 ========================

  override def getTableExistsQuery(table: String): String = {
    ensureInitialized().getTableExistsQuery(table)
  }

  override def getSchemaQuery(table: String): String = {
    ensureInitialized().getSchemaQuery(table)
  }

  override def getTruncateQuery(table: String): String = {
    ensureInitialized().getTruncateQuery(table)
  }

  // ======================== 分页委托 ========================

  override def supportsLimit: Boolean = true

  override def getLimitClause(limit: Int): String = {
    ensureInitialized().getLimitClause(limit)
  }

  override def supportsOffset: Boolean = true

  override def getOffsetClause(offset: Int): String = {
    ensureInitialized().getOffsetClause(offset)
  }

  // ======================== 表采样委托 ========================

  override def supportsTableSample: Boolean = true

  override def getTableSample(sample: org.apache.spark.sql.connector.expressions.aggregate.TableSampleInfo): String = {
    ensureInitialized().getTableSample(sample)
  }

  // ======================== DDL 委托 ========================

  override def createTable(
    table: String,
    schema: StructType,
    caseSensitive: Boolean,
    options: java.util.Map[String, String]
  ): String = {
    import scala.collection.JavaConverters._
    val scalaOptions = Option(options).map(_.asScala.toMap).getOrElse(Map.empty)
    ensureInitialized(Some(scalaOptions)).createTable(table, schema, caseSensitive, options)
  }

  override def alterTable(
    tableName: String,
    changes: Array[org.apache.spark.sql.connector.catalog.TableChange],
    caseSensitive: Boolean
  ): String = {
    ensureInitialized().alterTable(tableName, changes, caseSensitive)
  }

  // ======================== 列操作委托 ========================

  override def getAddColumnQuery(tableName: String, columnName: String, dataType: DataType): String = {
    ensureInitialized().getAddColumnQuery(tableName, columnName, dataType)
  }

  override def getRenameColumnQuery(
    tableName: String, columnName: String, newName: String, dataType: DataType
  ): String = {
    ensureInitialized().getRenameColumnQuery(tableName, columnName, newName, dataType)
  }

  override def getDeleteColumnQuery(tableName: String, columnName: String): String = {
    ensureInitialized().getDeleteColumnQuery(tableName, columnName)
  }

  override def getUpdateColumnTypeQuery(
    tableName: String, columnName: String, newDataType: DataType
  ): String = {
    ensureInitialized().getUpdateColumnTypeQuery(tableName, columnName, newDataType)
  }

  override def getUpdateColumnNullabilityQuery(
    tableName: String, columnName: String, isNullable: Boolean
  ): String = {
    ensureInitialized().getUpdateColumnNullabilityQuery(tableName, columnName, isNullable)
  }

  // ======================== 注释委托 ========================

  override def createTableComment(table: String, comment: String): Option[String] = {
    ensureInitialized().createTableComment(table, comment)
  }

  override def createColumnComment(
    table: String, column: String, comment: String
  ): Option[String] = {
    ensureInitialized().createColumnComment(table, column, comment)
  }

  // ======================== 异常分类委托 ========================

  override def classifyException(message: String, e: Throwable): Exception = {
    ensureInitialized().classifyException(message, e)
  }

  // ======================== 连接工厂委托 ========================

  override def createConnectionFactory(options: java.util.Map[String, String]): () => Connection = {
    import scala.collection.JavaConverters._
    val scalaOptions = Option(options).map(_.asScala.toMap).getOrElse(Map.empty)
    ensureInitialized(Some(scalaOptions)).createConnectionFactory(options)
  }

  override def toString: String = s"DmDialectProvider(delegate=${_delegate.getOrElse("未初始化")})"
}
