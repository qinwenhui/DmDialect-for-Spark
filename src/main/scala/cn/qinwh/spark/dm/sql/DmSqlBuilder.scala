package cn.qinwh.spark.dm.sql

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.types.DmTypeMapping
import cn.qinwh.spark.dm.utils.DmLogger
import cn.qinwh.spark.dm.utils.DmStringUtils

import org.apache.spark.sql.types.StructType

/**
 * 达梦数据库 SQL 构建器
 *
 * 负责生成达梦兼容的 DDL/DML SQL 语句，包括建表列定义、删表、分页查询、表采样等。
 * 所有标识符使用双引号 `"` 引用。
 *
 * 注意：`createTable` 在 Spark 4.1.1 中不再返回 SQL 字符串，
 * 而是直接在 Statement 上执行，因此本构建器提供 `buildCreateTableSchema`
 * 辅助方法用于生成列定义部分。
 *
 * @param config        达梦方言配置
 * @param typeMapping  类型映射器
 * @param logger       日志记录器
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
    if (identifier == null || identifier.isEmpty) return "\"\""
    val unquoted = unquoteIdentifier(identifier)
    "\"" + unquoted.replace("\"", "\"\"") + "\""
  }

  // ======================== 建表 ========================

  /**
   * 生成达梦 CREATE TABLE 的列定义部分（不含 CREATE TABLE 关键字）
   *
   * @param schema Spark Schema
   * @return 列定义字符串，如 `"col1" INT NOT NULL, "col2" VARCHAR2(255)`
   */
  def buildCreateTableSchema(schema: StructType): String = {
    schema.fields.map { field =>
      val colName = quoteIdentifier(field.name)
      val jdbcType = typeMapping.getJDBCType(field.dataType)
      val nullable = if (field.nullable) "" else " NOT NULL"
      s"$colName ${jdbcType.databaseTypeDefinition}$nullable"
    }.mkString(", ")
  }

  /**
   * 生成 CREATE TABLE 的表选项部分
   */
  def buildCreateTableOptions(options: Map[String, String] = Map.empty): String = {
    options.getOrElse("createTableOptions", storageClause)
  }

  // ======================== DROP TABLE ========================

  /**
   * 生成达梦兼容的 DROP TABLE 语句
   */
  def dropTable(table: String): String = {
    val quotedTable = quoteIdentifier(table)
    s"DROP TABLE IF EXISTS $quotedTable CASCADE"
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
    val quotedTable = quoteIdentifier(table)
    s"TRUNCATE TABLE $quotedTable"
  }

  // ======================== 分页查询 ========================

  /**
   * 达梦支持 LIMIT 分页
   */
  def supportsLimit: Boolean = true

  /**
   * 生成 LIMIT 子句
   */
  def getLimitClause(limit: Integer): String = {
    if (limit > 0) s"LIMIT $limit" else ""
  }

  /**
   * 达梦支持 OFFSET
   */
  def supportsOffset: Boolean = true

  /**
   * 生成 OFFSET 子句
   */
  def getOffsetClause(offset: Integer): String = {
    if (offset > 0) s"OFFSET $offset" else ""
  }

  // ======================== 表采样 ========================

  /**
   * 达梦支持 TABLESAMPLE 语法
   */
  def supportsTableSample: Boolean = true

  /**
   * 生成 TABLESAMPLE 子句
   *
   * @param lowerBound 采样比例下界
   * @param upperBound 采样比例上界
   * @param seed       随机种子
   * @return TABLESAMPLE 子句字符串
   */
  def getTableSample(lowerBound: Double, upperBound: Double, seed: Long): String = {
    val percentage = ((upperBound - lowerBound) * 100).toInt
    require(percentage > 0 && percentage <= 100, s"采样百分比必须在 1-100 之间，实际: $percentage")

    val sampleClause = s"TABLESAMPLE SYSTEM ($percentage)"
    if (seed != 0) s"$sampleClause REPEATABLE($seed)" else sampleClause
  }

  // ======================== ALTER TABLE 列操作 ========================

  /**
   * 生成添加列的 SQL（达梦使用 ADD 而非 ADD COLUMN）
   */
  def getAddColumnQuery(tableName: String, columnName: String, dataType: String): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    s"ALTER TABLE $quotedTable ADD $quotedCol $dataType"
  }

  /**
   * 生成重命名列的 SQL
   */
  def getRenameColumnQuery(
    tableName: String,
    columnName: String,
    newName: String,
    dbMajorVersion: Int
  ): String = {
    val quotedTable = quoteIdentifier(tableName)
    val oldQuoted = quoteIdentifier(columnName)
    val newQuoted = quoteIdentifier(newName)
    s"ALTER TABLE $quotedTable RENAME COLUMN $oldQuoted TO $newQuoted"
  }

  /**
   * 生成删除列的 SQL
   */
  def getDeleteColumnQuery(tableName: String, columnName: String): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    s"ALTER TABLE $quotedTable DROP COLUMN $quotedCol CASCADE"
  }

  /**
   * 生成修改列类型的 SQL
   * 达梦使用 MODIFY 而非 ALTER COLUMN
   */
  def getUpdateColumnTypeQuery(tableName: String, columnName: String, newDataType: String): String = {
    val quotedTable = quoteIdentifier(tableName)
    val quotedCol = quoteIdentifier(columnName)
    s"ALTER TABLE $quotedTable MODIFY $quotedCol $newDataType"
  }

  /**
   * 生成修改列可为空属性的 SQL
   * 达梦使用 MODIFY ... NULL/NOT NULL 而非 ALTER COLUMN ... SET
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

  // ======================== 注释 ========================

  /**
   * 生成表注释 SQL（达梦使用 COMMENT ON TABLE ... IS ... 语法）
   */
  def buildTableComment(table: String, comment: String): String = {
    val quotedTable = quoteIdentifier(table)
    val escapedComment = escapeSqlString(comment)
    s"COMMENT ON TABLE $quotedTable IS $escapedComment"
  }

  /**
   * 生成列注释 SQL（达梦使用 COMMENT ON COLUMN ... IS ... 语法）
   */
  def buildColumnComment(table: String, column: String, comment: String): String = {
    val quotedTable = quoteIdentifier(table)
    val quotedColumn = quoteIdentifier(column)
    val escapedComment = escapeSqlString(comment)
    s"COMMENT ON COLUMN $quotedTable.$quotedColumn IS $escapedComment"
  }

  // ======================== INSERT ========================

  /**
   * 生成 INSERT INTO 语句模板
   */
  def buildInsertStatement(table: String, fields: Array[org.apache.spark.sql.types.StructField]): String = {
    val quotedTable = quoteIdentifier(table)
    val columns = fields.map(f => quoteIdentifier(f.name)).mkString(", ")
    val placeholders = fields.map(_ => "?").mkString(", ")
    s"INSERT INTO $quotedTable ($columns) VALUES ($placeholders)"
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
