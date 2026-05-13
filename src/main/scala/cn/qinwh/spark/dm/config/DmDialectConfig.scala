package cn.qinwh.spark.dm.config

/**
 * 达梦方言配置项定义
 *
 * 包含所有 `spark.dmdialect.*` 配置键和默认值。
 * 配置优先级：DataFrame Options > Spark Conf > 默认值
 *
 * @param preferTimestampNTZ      是否优先将 TIMESTAMP 映射为 TimestampNTZType
 * @param legacyTimestampBehavior 是否启用旧的 Timestamp 处理方式
 * @param batchSize               JDBC 批量写入大小
 * @param fetchSize               JDBC 读取 fetchSize
 * @param useUnicode              连接参数：是否使用 Unicode
 * @param characterEncoding       连接参数：字符编码
 * @param loggingEnabled          是否开启方言内部的 DEBUG 日志
 * @param logQueries              是否在日志中打印生成的 SQL
 * @param caseSensitive           达梦数据库是否区分标识符大小写（默认 false）
 *
 * @author qinwh
 */
case class DmDialectConfig(
  preferTimestampNTZ: Boolean = DmDialectConfig.DEFAULT_PREFER_TIMESTAMP_NTZ,
  legacyTimestampBehavior: Boolean = DmDialectConfig.DEFAULT_LEGACY_TIMESTAMP,
  batchSize: Int = DmDialectConfig.DEFAULT_BATCH_SIZE,
  fetchSize: Int = DmDialectConfig.DEFAULT_FETCH_SIZE,
  useUnicode: Boolean = DmDialectConfig.DEFAULT_USE_UNICODE,
  characterEncoding: String = DmDialectConfig.DEFAULT_CHAR_ENCODING,
  loggingEnabled: Boolean = DmDialectConfig.DEFAULT_LOGGING_ENABLED,
  logQueries: Boolean = DmDialectConfig.DEFAULT_LOG_QUERIES,
  caseSensitive: Boolean = DmDialectConfig.DEFAULT_CASE_SENSITIVE
)

object DmDialectConfig {

  /** 配置键前缀 */
  val PREFIX: String = "spark.dmdialect"

  // ======================== 配置键常量 ========================

  val KEY_PREFER_TIMESTAMP_NTZ: String = s"$PREFIX.preferTimestampNTZ"
  val KEY_LEGACY_TIMESTAMP: String = s"$PREFIX.legacyTimestampBehavior"
  val KEY_BATCH_SIZE: String = s"$PREFIX.performance.batchSize"
  val KEY_FETCH_SIZE: String = s"$PREFIX.performance.fetchSize"
  val KEY_USE_UNICODE: String = s"$PREFIX.conn.useUnicode"
  val KEY_CHAR_ENCODING: String = s"$PREFIX.conn.characterEncoding"
  val KEY_LOGGING_ENABLED: String = s"$PREFIX.logging.enabled"
  val KEY_LOG_QUERIES: String = s"$PREFIX.logging.logQueries"
  val KEY_CASE_SENSITIVE: String = s"$PREFIX.caseSensitive"

  // ======================== 默认值 ========================

  val DEFAULT_PREFER_TIMESTAMP_NTZ: Boolean = false
  val DEFAULT_LEGACY_TIMESTAMP: Boolean = false
  val DEFAULT_BATCH_SIZE: Int = 1000
  val DEFAULT_FETCH_SIZE: Int = 1000
  val DEFAULT_USE_UNICODE: Boolean = true
  val DEFAULT_CHAR_ENCODING: String = "UTF-8"
  val DEFAULT_LOGGING_ENABLED: Boolean = false
  val DEFAULT_LOG_QUERIES: Boolean = false
  val DEFAULT_CASE_SENSITIVE: Boolean = false

  /** 创建使用所有默认值的配置实例 */
  def defaultConfig: DmDialectConfig = DmDialectConfig()
}
