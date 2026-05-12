package cn.qinwh.spark.dm.sql

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.types.DmTypeMapping
import cn.qinwh.spark.dm.utils.DmLogger
import cn.qinwh.spark.dm.utils.DmStringUtils

import org.apache.spark.sql.types.{DataType, StructType}

/**
 * 达梦数据库 SQL 构建器
 *
 * 负责生成达梦兼容的 DDL/DML SQL 语句，包括建表、删表、分页查询、表采样等。
 * 所有标识符使用双引号 `"` 引用。
 *
 * @param config      达梦方言配置
 * @param typeMapping 类型映射器
 * @param logger      日志记录器
 * @param storageClause 达梦存储子句（如 "STORAGE(ON \"MAIN\", CLUSTERBTR)"）
 *
 * @author qinwh
 */
class DmSqlBuilder(
  config: DmDialectConfig,
  typeMapping: DmTypeMapping,
  logger: DmLogger,
  storageClause: String = "STORAGE(ON \"MAIN\", CLUSTERBTR)"
) {

  import DmStringUtils._

  // ======================== 标识符引用 ========================

  /**
   * 使用达梦双引号规则引用标识符
   */
  def quoteIdentifier(identifier: String): String = {
    quoteIdentifierSafely(identifier)
  }

  private def quoteIdentifierSafely(identifier: String): String = {
    if (identifier == null || identifier.isEmpty) return "\"\""
    // 去除可能已有的引用，重新加引号
    val unquoted = unquoteIdentifier(identifier)
    "\"" + unquoted.replace("\"", "\"\"") + "\""
  }

  // ======================== 表存在性检查 ========================

  /**
   * 生成检查表是否存在的查询 SQL
   */
  def getTableExistsQuery(table: String): String = {
    val quoted = quoteIdentifier(table)
    s"SELECT 1 FROM $quoted WHERE 1=0"
  }

  /**
   * 生成获取表 Schema 的查询 SQL
   */
  def getSchemaQuery(table: String): String = {
    val quoted = quoteIdentifier(table)
    s"SELECT * FROM $quoted WHERE 1=0"
  }

  /**
   * 生成截断表的 SQL
   */
  def getTruncateQuery(table: String): String = {
    val quoted = quoteIdentifier(table)
    s"TRUNCATE TABLE $quoted"
  }

  // ======================== 分页查询 ========================

  /**
   * 达梦支持 LIMIT 分页
   */
  def supportsLimit: Boolean = true

  /**
   * 生成 LIMIT 子句
   */
  def getLimitClause(limit: Int): String = {
    require(limit >= 0, s"LIMIT 值必须 >= 0，实际: $limit")
    if (limit > 0) {
      s"LIMIT $limit"
    } else {
      ""
    }
  }

  /**
   * 达梦支持 OFFSET
   */
  def supportsOffset: Boolean = true

  /**
   * 生成 OFFSET 子句
   */
  def getOffsetClause(offset: Int): String = {
    require(offset >= 0, s"OFFSET 值必须 >= 0，实际: $offset")
    if (offset > 0) {
      s"OFFSET $offset"
    } else {
      ""
    }
  }

  // ======================== 表采样 ========================

  /**
   * 达梦支持 TABLESAMPLE 语法
   */
  def supportsTableSample: Boolean = true

  /**
   * 生成 TABLESAMPLE 子句
   *
   * @param lowerBound  采样比例下界
   * @param upperBound  采样比例上界
   * @param seed        随机种子
   * @return TABLESAMPLE 子句字符串
   */
  def getTableSample(lowerBound: Double, upperBound: Double, seed: Long): String = {
    val percentage = ((lowerBound + upperBound) / 2.0 * 100).toInt
    require(percentage > 0 && percentage <= 100, s"采样百分比必须在 1-100 之间，实际: $percentage")

    val sampleClause = s"TABLESAMPLE SYSTEM ($percentage)"
    if (seed != 0) {
      s"$sampleClause REPEATABLE($seed)"
    } else {
      sampleClause
    }
  }

  // ======================== DDL 语句生成 ========================

  /**
   * 生成达梦兼容的 CREATE TABLE 语句
   *
   * @param table   表名
   * @param schema  Spark Schema
   * @param options 建表选项
   * @return 完整的 CREATE TABLE SQL
   */
  def createTable(
    table: String,
    schema: StructType,
    options: Map[String, String] = Map.empty
  ): String = {
    val quotedTable = quoteIdentifier(table)
    val columnDefs = schema.fields.map { field =>
      val colName = quoteIdentifier(field.name)
      val jdbcType = typeMapping.getJDBCType(field.dataType)
      val nullable = if (field.nullable) "" else " NOT NULL"
      s"$colName ${jdbcType.databaseTypeDefinition}$nullable"
    }.mkString(",\n  ")

    val storage = options.getOrElse("dbtable.storage", storageClause)

    logger.logQuery(s"CREATE TABLE $table")
    s"""CREATE TABLE $quotedTable (
       |  $columnDefs
       |)
       |$storage""".stripMargin
  }

  /**
   * 生成达梦兼容的 DROP TABLE 语句
   *
   * @param table 表名
   * @return DROP TABLE SQL
   */
  def dropTable(table: String): String = {
    val quotedTable = quoteIdentifier(table)
    s"DROP TABLE IF EXISTS $quotedTable CASCADE"
  }

  /**
   * 生成表注释 SQL（达梦使用 COMMENT ON TABLE ... IS ... 语法）
   *
   * @param table   表名
   * @param comment 注释内容
   * @return COMMENT ON TABLE SQL
   */
  def createTableComment(table: String, comment: String): String = {
    val quotedTable = quoteIdentifier(table)
    val escapedComment = escapeSqlString(comment)
    s"COMMENT ON TABLE $quotedTable IS $escapedComment"
  }

  /**
   * 生成列注释 SQL（达梦使用 COMMENT ON COLUMN ... IS ... 语法）
   *
   * @param table    表名
   * @param column   列名
   * @param comment  注释内容
   * @return COMMENT ON COLUMN SQL
   */
  def createColumnComment(table: String, column: String, comment: String): String = {
    val quotedTable = quoteIdentifier(table)
    val quotedColumn = quoteIdentifier(column)
    val escapedComment = escapeSqlString(comment)
    s"COMMENT ON COLUMN $quotedTable.$quotedColumn IS $escapedComment"
  }

  // ======================== ALTER TABLE 操作 ========================

  /**
   * 生成添加列的 SQL
   */
  def getAddColumnQuery(
    tableName: String,
    columnName: String,
    dataType: DataType
  ): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    val jdbcType = typeMapping.getJDBCType(dataType)
    s"ALTER TABLE $quotedTable ADD $quotedCol ${jdbcType.databaseTypeDefinition}"
  }

  /**
   * 生成重命名列的 SQL
   */
  def getRenameColumnQuery(
    tableName: String,
    oldColumnName: String,
    newColumnName: String,
    dataType: DataType
  ): String = {
    val quotedTable = quoteIdentifier(tableName)
    val oldQuoted = quoteIdentifier(oldColumnName)
    val newQuoted = quoteIdentifier(newColumnName)
    s"ALTER TABLE $quotedTable RENAME COLUMN $oldQuoted TO $newQuoted"
  }

  /**
   * 生成删除列的 SQL
   */
  def getDeleteColumnQuery(
    tableName: String,
    columnName: String
  ): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    s"ALTER TABLE $quotedTable DROP COLUMN $quotedCol CASCADE"
  }

  /**
   * 生成修改列类型的 SQL
   */
  def getUpdateColumnTypeQuery(
    tableName: String,
    columnName: String,
    newDataType: DataType
  ): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    val jdbcType = typeMapping.getJDBCType(newDataType)
    s"ALTER TABLE $quotedTable MODIFY $quotedCol ${jdbcType.databaseTypeDefinition}"
  }

  /**
   * 生成修改列是否可为空的 SQL
   *
   * 达梦使用 ALTER TABLE ... MODIFY ... NULL / NOT NULL 语法
   */
  def getUpdateColumnNullabilityQuery(
    tableName: String,
    columnName: String,
    isNullable: Boolean
  ): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    val nullClause = if (isNullable) "NULL" else "NOT NULL"
    s"ALTER TABLE $quotedTable MODIFY $quotedCol $nullClause"
  }

  /**
   * 生成用于获取列 Schema 的查询
   */
  def getColumnsQuery(tableName: String): String = {
    val quotedTable = quoteIdentifier(tableName)
    s"SELECT * FROM $quotedTable WHERE 1=0"
  }

  /**
   * 生成用于查询表列表的 SQL
   */
  def getTableListQuery(schemaPattern: String): String = {
    val whereClause = if (schemaPattern != null && schemaPattern.nonEmpty) {
      s" WHERE OWNER = '${schemaPattern.toUpperCase}'"
    } else {
      ""
    }
    s"SELECT TABLE_NAME FROM ALL_TABLES$whereClause"
  }

  // ======================== 特殊查询适配 ========================

  /**
   * 生成获取自增列插入后 ID 的 SQL
   */
  def getIdentityInsertQuery(): String = {
    "SELECT @@IDENTITY"
  }

  /**
   * 生成获取最后插入行 ID 的 SQL
   */
  def getRowCountQuery(table: String): String = {
    val quotedTable = quoteIdentifier(table)
    s"SELECT COUNT(*) FROM $quotedTable"
  }
}

object DmSqlBuilder {
  def apply(
    config: DmDialectConfig,
    typeMapping: DmTypeMapping,
    logger: DmLogger,
    storageClause: String = "STORAGE(ON \"MAIN\", CLUSTERBTR)"
  ): DmSqlBuilder = new DmSqlBuilder(config, typeMapping, logger, storageClause)
}
