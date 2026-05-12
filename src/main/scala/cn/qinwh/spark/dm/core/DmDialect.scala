package cn.qinwh.spark.dm.core

import java.sql.{Connection, SQLException, Types}
import java.util.Properties

import scala.util.Try

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.functions.DmFunctionMapper
import cn.qinwh.spark.dm.sql.DmSqlBuilder
import cn.qinwh.spark.dm.types.DmTypeMapping
import cn.qinwh.spark.dm.utils.{DmConstants, DmExceptionUtils, DmLogger}
import cn.qinwh.spark.dm.version.{DmFeatureMatrix, DmVersion}

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
) extends JdbcDialect {

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
    url != null && url.toLowerCase.startsWith(DmConstants.JDBC_URL_PREFIX)
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
    Some(typeMapping.getCatalystType(sqlType, typeName, size, md))
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

  /**
   * 生成检查表是否存在的查询语句
   */
  override def getTableExistsQuery(table: String): String = {
    sqlBuilder.getTableExistsQuery(table)
  }

  /**
   * 生成获取表 Schema 的查询语句
   */
  override def getSchemaQuery(table: String): String = {
    sqlBuilder.getSchemaQuery(table)
  }

  /**
   * 生成截断表的 SQL
   */
  override def getTruncateQuery(table: String): String = {
    sqlBuilder.getTruncateQuery(table)
  }

  // ======================== 分页查询 ========================

  /**
   * 达梦支持 LIMIT 分页
   */
  override def supportsLimit: Boolean = sqlBuilder.supportsLimit

  /**
   * 生成 LIMIT 子句
   */
  override def getLimitClause(limit: Int): String = {
    sqlBuilder.getLimitClause(limit)
  }

  /**
   * 达梦支持 OFFSET
   */
  override def supportsOffset: Boolean = sqlBuilder.supportsOffset

  /**
   * 生成 OFFSET 子句
   */
  override def getOffsetClause(offset: Int): String = {
    sqlBuilder.getOffsetClause(offset)
  }

  // ======================== 表采样 ========================

  /**
   * 达梦支持 TABLESAMPLE
   */
  override def supportsTableSample: Boolean = sqlBuilder.supportsTableSample

  /**
   * 生成 TABLESAMPLE 子句
   */
  override def getTableSample(sample: org.apache.spark.sql.connector.expressions.aggregate.TableSampleInfo): String = {
    sqlBuilder.getTableSample(sample.lowerBound, sample.upperBound, sample.seed)
  }

  // ======================== DDL 语句 ========================

  /**
   * 生成达梦兼容的 CREATE TABLE 语句
   */
  override def createTable(
    table: String,
    schema: StructType,
    caseSensitive: Boolean,
    options: java.util.Map[String, String]
  ): String = {
    import scala.collection.JavaConverters._
    val scalaOptions = Option(options).map(_.asScala.toMap).getOrElse(Map.empty)
    sqlBuilder.createTable(table, schema, scalaOptions)
  }

  /**
   * 生成达梦兼容的 ALTER TABLE 语句
   */
  override def alterTable(
    tableName: String,
    changes: Array[org.apache.spark.sql.connector.catalog.TableChange],
    caseSensitive: Boolean
  ): String = {
    import org.apache.spark.sql.connector.catalog.TableChange._

    val parts = changes.map {
      case add: AddColumn =>
        val comments = if (add.comment() != null && add.comment().nonEmpty) {
          add.comment()
        } else ""
        sqlBuilder.getAddColumnQuery(tableName, add.fieldNames()(0), add.dataType())

      case delete: DeleteColumn =>
        sqlBuilder.getDeleteColumnQuery(tableName, delete.fieldNames()(0))

      case rename: RenameColumn =>
        sqlBuilder.getRenameColumnQuery(
          tableName, rename.fieldNames()(0), rename.newName(), StringType  // 保留原类型
        )

      case update: UpdateColumnType =>
        sqlBuilder.getUpdateColumnTypeQuery(
          tableName, update.fieldNames()(0), update.newDataType()
        )

      case updateNull: UpdateColumnNullability =>
        sqlBuilder.getUpdateColumnNullabilityQuery(
          tableName, updateNull.fieldNames()(0), updateNull.nullable()
        )

      case updateComment: UpdateColumnComment =>
        sqlBuilder.createColumnComment(
          tableName, updateComment.fieldNames()(0), updateComment.newComment()
        )

      case _ => ""
    }

    parts.filter(_.nonEmpty).mkString(";\n")
  }

  // ======================== 列操作 ========================

  /**
   * 生成添加列的 SQL
   */
  override def getAddColumnQuery(
    tableName: String,
    columnName: String,
    dataType: DataType
  ): String = {
    sqlBuilder.getAddColumnQuery(tableName, columnName, dataType)
  }

  /**
   * 生成重命名列的 SQL
   */
  override def getRenameColumnQuery(
    tableName: String,
    columnName: String,
    newName: String,
    dataType: DataType
  ): String = {
    sqlBuilder.getRenameColumnQuery(tableName, columnName, newName, dataType)
  }

  /**
   * 生成删除列的 SQL
   */
  override def getDeleteColumnQuery(
    tableName: String,
    columnName: String
  ): String = {
    sqlBuilder.getDeleteColumnQuery(tableName, columnName)
  }

  /**
   * 生成修改列类型的 SQL
   */
  override def getUpdateColumnTypeQuery(
    tableName: String,
    columnName: String,
    newDataType: DataType
  ): String = {
    sqlBuilder.getUpdateColumnTypeQuery(tableName, columnName, newDataType)
  }

  /**
   * 生成修改列可为空属性的 SQL
   */
  override def getUpdateColumnNullabilityQuery(
    tableName: String,
    columnName: String,
    isNullable: Boolean
  ): String = {
    sqlBuilder.getUpdateColumnNullabilityQuery(tableName, columnName, isNullable)
  }

  // ======================== 注释 ========================

  /**
   * 达梦数据库表注释 SQL（使用 COMMENT ON TABLE ... IS ... 语法）
   */
  override def createTableComment(table: String, comment: String): Option[String] = {
    Option(comment).filter(_.nonEmpty).map { c =>
      sqlBuilder.createTableComment(table, c)
    }
  }

  /**
   * 达梦数据库列注释 SQL（使用 COMMENT ON COLUMN ... IS ... 语法）
   */
  override def createColumnComment(
    table: String,
    column: String,
    comment: String
  ): Option[String] = {
    Option(comment).filter(_.nonEmpty).map { c =>
      sqlBuilder.createColumnComment(table, column, c)
    }
  }

  // ======================== 异常分类 ========================

  /**
   * 将达梦 JDBC 异常包装为 Spark 友好的异常
   */
  override def classifyException(message: String, e: Throwable): Exception = {
    e match {
      case sqlEx: SQLException =>
        val category = DmExceptionUtils.classify(sqlEx)
        logger.error(s"[${category.categoryName}] $message", sqlEx)
        new org.apache.spark.SparkException(
          s"${category.message}\n原始错误: $message",
          sqlEx
        )

      case other =>
        new org.apache.spark.SparkException(
          s"达梦数据库操作异常: $message",
          other
        )
    }
  }

  // ======================== 连接工厂 ========================

  /**
   * 创建达梦 JDBC 连接工厂，注入达梦特有的连接属性
   */
  override def createConnectionFactory(options: java.util.Map[String, String]): () => Connection = {
    import scala.collection.JavaConverters._

    val scalaOptions = Option(options).map(_.asScala.toMap).getOrElse(Map.empty)
    val url = scalaOptions.getOrElse("url", "")
    val props = new Properties()

    // 设置连接属性
    scalaOptions.foreach { case (key, value) =>
      // 筛选连接相关的配置传递给 JDBC 驱动
      if (key.startsWith("spark.dmdialect.conn.") || key == "user" || key == "password" || key == "driver") {
        // 跳过了，由 JDBC 自动处理
      } else if (!key.startsWith("spark.")) {
        props.setProperty(key, value)
      }
    }

    // 注入达梦推荐的默认连接参数（可被 JDBC URL 中的参数覆盖）
    props.setProperty("useUnicode", config.useUnicode.toString)
    props.setProperty("characterEncoding", config.characterEncoding)

    logger.debug(s"创建达梦数据库连接: $url")

    () => {
      val driverClass = scalaOptions.getOrElse("driver", "dm.jdbc.driver.DmDriver")
      Class.forName(driverClass)
      val connection = java.sql.DriverManager.getConnection(url, props)
      logger.debug("达梦数据库连接已建立")
      connection
    }
  }
}

object DmDialect {
  val JDBC_URL_PREFIX: String = DmConstants.JDBC_URL_PREFIX
}
