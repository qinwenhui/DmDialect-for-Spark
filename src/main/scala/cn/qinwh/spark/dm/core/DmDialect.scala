package cn.qinwh.spark.dm.core

import java.sql.{Connection, SQLException, Statement}
import java.util.Locale

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.functions.DmFunctionMapper
import cn.qinwh.spark.dm.sql.DmSqlBuilder
import cn.qinwh.spark.dm.types.DmTypeMapping
import cn.qinwh.spark.dm.utils.{DmConstants, DmExceptionUtils, DmLogger}
import cn.qinwh.spark.dm.version.{DmFeatureMatrix, DmVersion}

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

/**
 * 达梦数据库 JDBC 方言抽象基类
 *
 * 封装所有达梦数据库通用的类型映射、SQL 构建、异常分类和连接管理逻辑。
 * DM7 和 DM8 通过子类化来覆盖版本特有的行为（如 JSON 类型支持）。
 *
 * @param config   方言配置
 * @param version  数据库版本
 * @param features 版本特性矩阵
 *
 * @author qinwh
 */
abstract class DmDialect(
  val config: DmDialectConfig,
  val version: DmVersion,
  val features: DmFeatureMatrix
) extends JdbcDialect with SQLConfHelper {

  /** 日志记录器 */
  protected val logger: DmLogger = DmLogger(config.loggingEnabled, config.logQueries)

  /** 类型映射器 */
  protected val typeMapping: DmTypeMapping = new DmTypeMapping(config)

  /** SQL 构建器 */
  protected val sqlBuilder: DmSqlBuilder = new DmSqlBuilder(
    config, typeMapping, logger, features.storageClause
  )

  // ======================== 方言识别 ========================

  /**
   * 检查是否能处理给定的 JDBC URL
   * 匹配 `jdbc:dm://` 前缀的 URL
   */
  override def canHandle(url: String): Boolean = {
    url != null && url.toLowerCase(Locale.ROOT).startsWith(DmConstants.JDBC_URL_PREFIX)
  }

  // ======================== 类型映射 ========================

  /**
   * 将达梦 JDBC 类型转换为 Spark Catalyst DataType
   */
  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = {
    // 对于无法明确识别的类型返回 None，让 Spark 默认逻辑兜底
    val upperTypeName = if (typeName != null) typeName.toUpperCase.trim else ""
    (sqlType, upperTypeName) match {
      // 达梦特有类型
      case (java.sql.Types.OTHER, t) if t == "JSON" || t == "TEXT" || t == "IMAGE" || t == "VOID" =>
        Some(typeMapping.getCatalystType(sqlType, typeName, size, md))
      case (java.sql.Types.OTHER, _) =>
        // 无法识别的 OTHER 类型，让 Spark 默认处理
        None
      case (java.sql.Types.NULL, _) =>
        Some(NullType)
      case _ =>
        Some(typeMapping.getCatalystType(sqlType, typeName, size, md))
    }
  }

  /**
   * 将 Spark DataType 转换为达梦 JDBC 类型
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    Some(typeMapping.getJDBCType(dt))
  }

  // ======================== 标识符引用 ========================

  /**
   * 使用达梦双引号 `"` 引用标识符
   */
  override def quoteIdentifier(colName: String): String = {
    sqlBuilder.quoteIdentifier(colName)
  }

  // ======================== 表存在性检查 ========================

  override def getTableExistsQuery(table: String): String = {
    sqlBuilder.getTableExistsQuery(table)
  }

  override def getSchemaQuery(table: String): String = {
    sqlBuilder.getSchemaQuery(table)
  }

  override def getTruncateQuery(table: String): String = {
    sqlBuilder.getTruncateQuery(table)
  }

  // ======================== 分页查询 ========================

  /**
   * 达梦支持 LIMIT 分页
   */
  override def supportsLimit: Boolean = true

  override def getLimitClause(limit: Integer): String = {
    sqlBuilder.getLimitClause(limit)
  }

  /**
   * 达梦支持 OFFSET
   */
  override def supportsOffset: Boolean = true

  override def getOffsetClause(offset: Integer): String = {
    sqlBuilder.getOffsetClause(offset)
  }

  // ======================== 表采样 ========================

  override def supportsTableSample: Boolean = true

  override def getTableSample(sample: TableSampleInfo): String = {
    sqlBuilder.getTableSample(sample.lowerBound, sample.upperBound, sample.seed)
  }

  // ======================== JOIN 支持 ========================

  /**
   * 达梦支持 JOIN 操作
   */
  override def supportsJoin: Boolean = true

  // ======================== CREATE TABLE ========================

  /**
   * 创建包含达梦存储选项的 CREATE TABLE 语句并执行
   *
   * Spark 4.1.1 中 createTable 直接在 Statement 上执行，不返回 SQL 字符串。
   */
  override def createTable(
    statement: Statement,
    tableName: String,
    strSchema: String,
    options: JdbcOptionsInWrite
  ): Unit = {
    val userOptions = options.createTableOptions
    val tableOptions = if (userOptions != null && userOptions.nonEmpty) {
      userOptions
    } else {
      // 用户未指定建表选项时，使用达梦默认存储子句
      features.storageClause
    }
    val sql = s"CREATE TABLE $tableName ($strSchema) $tableOptions"
    logger.logQuery(sql)
    statement.executeUpdate(sql)
  }

  // ======================== INSERT INTO ========================

  /**
   * 生成 INSERT INTO 语句模板
   */
  override def insertIntoTable(table: String, fields: Array[StructField]): String = {
    sqlBuilder.buildInsertStatement(table, fields)
  }

  // ======================== DROP TABLE ========================

  override def dropTable(table: String): String = {
    sqlBuilder.dropTable(table)
  }

  // ======================== RENAME TABLE ========================

  override def renameTable(oldTable: String, newTable: String): String = {
    s"ALTER TABLE ${quoteIdentifier(oldTable)} RENAME TO ${quoteIdentifier(newTable)}"
  }

  // ======================== ALTER TABLE ========================

  /**
   * 生成 ALTER TABLE 语句数组
   *
   * @param tableName      表名
   * @param changes        表变更列表
   * @param dbMajorVersion 数据库主版本号
   * @return ALTER TABLE SQL 语句数组
   */
  override def alterTable(
    tableName: String,
    changes: Seq[TableChange],
    dbMajorVersion: Int
  ): Array[String] = {
    import scala.collection.mutable.ArrayBuilder

    val updateClause = ArrayBuilder.make[String]
    for (change <- changes) {
      change match {
        case add: AddColumn if add.fieldNames.length == 1 =>
          val dataType = JdbcUtils.getJdbcType(add.dataType(), this).databaseTypeDefinition
          val name = add.fieldNames
          updateClause += getAddColumnQuery(tableName, name(0), dataType)

        case rename: RenameColumn if rename.fieldNames.length == 1 =>
          val name = rename.fieldNames
          updateClause += getRenameColumnQuery(tableName, name(0), rename.newName, dbMajorVersion)

        case delete: DeleteColumn if delete.fieldNames.length == 1 =>
          val name = delete.fieldNames
          updateClause += getDeleteColumnQuery(tableName, name(0))

        case updateColumnType: UpdateColumnType if updateColumnType.fieldNames.length == 1 =>
          val name = updateColumnType.fieldNames
          val dataType = JdbcUtils.getJdbcType(updateColumnType.newDataType(), this)
            .databaseTypeDefinition
          updateClause += getUpdateColumnTypeQuery(tableName, name(0), dataType)

        case updateNull: UpdateColumnNullability if updateNull.fieldNames.length == 1 =>
          val name = updateNull.fieldNames
          updateClause += getUpdateColumnNullabilityQuery(
            tableName, name(0), updateNull.nullable())

        case updateComment: UpdateColumnComment if updateComment.fieldNames.length == 1 =>
          // 达梦使用 COMMENT ON COLUMN 语法设置列注释
          val name = updateComment.fieldNames
          updateClause += getTableCommentQuery(tableName, updateComment.newComment())
          // 注意: 达梦没有直接的 COMMENT ON COLUMN 在 alterTable 中，
          // 这里通过外部 COMMENT ON 语句实现

        case _ =>
          throw new IllegalArgumentException(
            s"达梦数据库不支持此表变更操作: ${change.getClass.getSimpleName}, 表名: $tableName")
      }
    }
    updateClause.result()
  }

  // ======================== ALTER TABLE 列操作（DM 特定语法） ========================

  /**
   * 添加列 — 达梦使用 ADD（不含 COLUMN 关键字）
   */
  override def getAddColumnQuery(
    tableName: String,
    columnName: String,
    dataType: String
  ): String = {
    sqlBuilder.getAddColumnQuery(tableName, columnName, dataType)
  }

  /**
   * 重命名列
   */
  override def getRenameColumnQuery(
    tableName: String,
    columnName: String,
    newName: String,
    dbMajorVersion: Int
  ): String = {
    sqlBuilder.getRenameColumnQuery(tableName, columnName, newName, dbMajorVersion)
  }

  /**
   * 删除列
   */
  override def getDeleteColumnQuery(
    tableName: String,
    columnName: String
  ): String = {
    sqlBuilder.getDeleteColumnQuery(tableName, columnName)
  }

  /**
   * 修改列类型 — 达梦使用 MODIFY 语法
   */
  override def getUpdateColumnTypeQuery(
    tableName: String,
    columnName: String,
    newDataType: String
  ): String = {
    sqlBuilder.getUpdateColumnTypeQuery(tableName, columnName, newDataType)
  }

  /**
   * 修改列的可为空属性 — 达梦使用 MODIFY ... NULL / NOT NULL 语法
   */
  override def getUpdateColumnNullabilityQuery(
    tableName: String,
    columnName: String,
    isNullable: Boolean
  ): String = {
    sqlBuilder.getUpdateColumnNullabilityQuery(tableName, columnName, isNullable)
  }

  // ======================== 表注释 ========================

  /**
   * 达梦表注释 — COMMENT ON TABLE ... IS ... 语法
   * 注意: Spark 4.1.1 的基类默认实现就是 `COMMENT ON TABLE ... IS ...`，
   * 与达梦一致，但这里显式覆盖以确保兼容性和添加日志。
   */
  override def getTableCommentQuery(table: String, comment: String): String = {
    logger.logQuery(s"COMMENT ON TABLE $table")
    sqlBuilder.buildTableComment(table, comment)
  }

  // ======================== 函数支持 ========================

  /**
   * 检查达梦数据库是否支持给定的函数名
   *
   * 根据 DmFunctionMapper 中的映射表进行判断。
   */
  override def isSupportedFunction(funcName: String): Boolean = {
    // 仅当 Spark 函数名与达梦函数名相同时才允许下推执行
    // 因为 JDBCSQLBuilder 不能翻译函数名（dialectFunctionName 默认返回原名）
    DmFunctionMapper.isDirectMapping(funcName)
  }

  // ======================== 异常分类 ========================

  /**
   * 新签名 (Spark 4.0.0+)：分类并包装达梦异常
   */
  override def classifyException(
    e: Throwable,
    condition: String,
    messageParameters: Map[String, String],
    description: String,
    isRuntime: Boolean
  ): Throwable with SparkThrowable = {
    e match {
      case sqlEx: SQLException =>
        val category = DmExceptionUtils.classify(sqlEx)
        logger.error(s"[${category.categoryName}] $description", sqlEx)
        if (isRuntime) {
          new SparkException(
            errorClass = condition,
            messageParameters = messageParameters,
            cause = e)
        } else {
          new AnalysisException(
            errorClass = condition,
            messageParameters = messageParameters,
            cause = Some(e))
        }

      case _ =>
        if (isRuntime) {
          new SparkException(
            errorClass = condition,
            messageParameters = messageParameters,
            cause = e)
        } else {
          new AnalysisException(
            errorClass = condition,
            messageParameters = messageParameters,
            cause = Some(e))
        }
    }
  }

  /**
   * 旧签名 (deprecated since 4.0.0)：保留以兼容旧版调用
   */
  @deprecated("Use classifyException with error condition", "4.0.0")
  override def classifyException(message: String, e: Throwable): AnalysisException = {
    e match {
      case sqlEx: SQLException =>
        val category = DmExceptionUtils.classify(sqlEx)
        logger.error(s"[${category.categoryName}] $message", sqlEx)
        new AnalysisException(
          errorClass = "FAILED_JDBC.UNCLASSIFIED",
          messageParameters = Map(
            "url" -> DmConstants.JDBC_URL_PREFIX,
            "message" -> message),
          cause = Some(sqlEx))

      case other =>
        new AnalysisException(
          errorClass = "FAILED_JDBC.UNCLASSIFIED",
          messageParameters = Map(
            "url" -> DmConstants.JDBC_URL_PREFIX,
            "message" -> message),
          cause = Some(other))
    }
  }

  /**
   * 达梦语法错误最佳努力检测 (Spark 4.1.0+)
   *
   * 以达梦 errorCode 为主要判断依据。
   * Spark 默认通过 SQLState.startsWith("42") 判断，但达梦的语法错误 SQLState
   * 不可靠，因此必须使用达梦厂商错误码（如 -2007）。
   *
   * 降级：当 errorCode 未命中已知值时，回退到 SQLState "42" 判断。
   */
  override def isSyntaxErrorBestEffort(exception: SQLException): Boolean = {
    DmExceptionUtils.isSyntaxError(exception) ||
    Option(exception.getSQLState).exists(_.startsWith("42"))
  }

  /**
   * 检测是否为对象不存在的异常 (Spark 4.1.0+)
   *
   * 以达梦 errorCode 为主要判断依据。
   * Spark 默认通过 SQLState.startsWith("42") 判断，但达梦"无效的表或视图名"
   * (错误码 -2106) 的 SQLState 并非 "42" 开头，这正是本项目需要自定义方言
   * 的核心原因之一。
   *
   * 降级：当 errorCode 未命中已知值时，回退到 SQLState "42" 判断。
   */
  override def isObjectNotFoundException(e: SQLException): Boolean = {
    DmExceptionUtils.isObjectNotFound(e) ||
    Option(e.getSQLState).exists(_.startsWith("42"))
  }

  // ======================== 连接工厂 ========================

  /**
   * 创建达梦 JDBC 连接工厂
   *
   * @param options JDBC 选项
   * @return 连接工厂函数（参数为 partition ID）
   */
  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    val url = options.parameters.getOrElse("url", "")
    val driverClass = options.driverClass
    val connectionProperties = new java.util.Properties()

    // 将 options.parameters 中非 Spark 内部的配置传递给 JDBC 驱动
    options.parameters.foreach { case (k, v) =>
      if (!k.startsWith("spark.") && k != "url" && k != "driver") {
        connectionProperties.setProperty(k, v)
      }
    }

    logger.debug(s"创建达梦数据库连接: $url")

    (partitionId: Int) => {
      Class.forName(driverClass)
      val connection = java.sql.DriverManager.getConnection(url, connectionProperties)
      require(connection != null,
        s"达梦驱动无法建立 JDBC 连接，请检查 URL: $url")
      logger.debug(s"达梦数据库连接已建立 (partition=$partitionId)")
      connection
    }
  }
  // ======================== 截断表行为 ========================

  /**
   * 达梦 TRUNCATE TABLE 默认不级联
   */
  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  /**
   * 双参数版本的 TRUNCATE（Spark 内部调用的主要版本）
   */
  override def getTruncateQuery(
    table: String,
    cascade: Option[Boolean] = isCascadingTruncateTable()
  ): String = {
    cascade match {
      case Some(true) => s"TRUNCATE TABLE ${quoteIdentifier(table)} CASCADE"
      case _          => s"TRUNCATE TABLE ${quoteIdentifier(table)}"
    }
  }

  // ======================== 连接优化 ========================

  /**
   * 查询前的连接预处理
   *
   * 当 fetchSize > 0 时设置 autocommit=false，使达梦 JDBC 驱动
   * 支持游标模式批量读取，避免一次性加载全部数据到内存。
   */
  override def beforeFetch(connection: Connection, properties: Map[String, String]): Unit = {
    super.beforeFetch(connection, properties)
    val batchFetchSize = properties.getOrElse("fetchsize", properties.getOrElse(
      "batchsize", "0")).toInt
    if (batchFetchSize > 0) {
      connection.setAutoCommit(false)
    }
  }

  // ======================== 值编译（谓词下推中的字面量格式化） ========================

  override def compileValue(value: Any): Any = value match {
    case stringValue: String =>
      s"'${escapeSql(stringValue)}'"
    case binaryValue: Array[Byte] =>
      // 达梦使用 TO_BLOB('hex_string') 函数，或直接用 X'hex' 格式
      binaryValue.map("%02X".format(_)).mkString("X'", "", "'")
    case _ =>
      super.compileValue(value)
  }

  // ======================== 时间戳转换 ========================

  /**
   * 将 JDBC Timestamp 转换为 TimestampNTZ 的 LocalDateTime
   *
   * 与 PostgresDialect 一致，使用 Timestamp.toLocalDateTime() 直接转换，
   * JDBC 驱动返回的时间戳已代表数据库中的 wall-clock 时间。
   */
  override def convertJavaTimestampToTimestampNTZ(t: java.sql.Timestamp): java.time.LocalDateTime = {
    t.toLocalDateTime
  }

  /**
   * 将 TimestampNTZ 的 LocalDateTime 转换为 JDBC Timestamp
   */
  override def convertTimestampNTZToJavaTimestamp(
    ldt: java.time.LocalDateTime
  ): java.sql.Timestamp = {
    java.sql.Timestamp.valueOf(ldt)
  }
}

object DmDialect {
  val JDBC_URL_PREFIX: String = DmConstants.JDBC_URL_PREFIX
}
