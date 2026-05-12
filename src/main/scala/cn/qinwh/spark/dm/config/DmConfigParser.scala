package cn.qinwh.spark.dm.config

import cn.qinwh.spark.dm.config.DmDialectConfig._

/**
 * 达梦方言配置解析器
 *
 * 实现三级优先级配置解析：
 * 1. DataFrame API 传入的 Options（最高优先级）
 * 2. Spark Conf 配置
 * 3. 默认值（最低优先级）
 *
 * @author qinwh
 */
object DmConfigParser {

  /**
   * 从 JDBC Options 和 Spark Conf 中解析配置
   *
   * @param jdbcOptions JDBC 选项参数（来自 DataFrame Options，优先级最高）
   * @param sparkConf   Spark 配置参数（优先级次之，可选）
   * @return 解析后的 DmDialectConfig 实例
   */
  def parse(
    jdbcOptions: Map[String, String],
    sparkConf: Map[String, String] = Map.empty
  ): DmDialectConfig = {
    DmDialectConfig(
      preferTimestampNTZ = getBoolean(KEY_PREFER_TIMESTAMP_NTZ, DEFAULT_PREFER_TIMESTAMP_NTZ, jdbcOptions, sparkConf),
      legacyTimestampBehavior = getBoolean(KEY_LEGACY_TIMESTAMP, DEFAULT_LEGACY_TIMESTAMP, jdbcOptions, sparkConf),
      batchSize = getInt(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE, jdbcOptions, sparkConf),
      fetchSize = getInt(KEY_FETCH_SIZE, DEFAULT_FETCH_SIZE, jdbcOptions, sparkConf),
      useUnicode = getBoolean(KEY_USE_UNICODE, DEFAULT_USE_UNICODE, jdbcOptions, sparkConf),
      characterEncoding = getString(KEY_CHAR_ENCODING, DEFAULT_CHAR_ENCODING, jdbcOptions, sparkConf),
      loggingEnabled = getBoolean(KEY_LOGGING_ENABLED, DEFAULT_LOGGING_ENABLED, jdbcOptions, sparkConf),
      logQueries = getBoolean(KEY_LOG_QUERIES, DEFAULT_LOG_QUERIES, jdbcOptions, sparkConf)
    )
  }

  /**
   * 按优先级解析 Boolean 配置项
   */
  private def getBoolean(
    key: String,
    default: Boolean,
    jdbcOptions: Map[String, String],
    sparkConf: Map[String, String]
  ): Boolean = {
    jdbcOptions.get(key)
      .map(_.toBoolean)
      .getOrElse {
        sparkConf.get(key)
          .map(_.toBoolean)
          .getOrElse(default)
      }
  }

  /**
   * 按优先级解析 Int 配置项
   */
  private def getInt(
    key: String,
    default: Int,
    jdbcOptions: Map[String, String],
    sparkConf: Map[String, String]
  ): Int = {
    jdbcOptions.get(key)
      .map(_.toInt)
      .getOrElse {
        sparkConf.get(key)
          .map(_.toInt)
          .getOrElse(default)
      }
  }

  /**
   * 按优先级解析 String 配置项
   */
  private def getString(
    key: String,
    default: String,
    jdbcOptions: Map[String, String],
    sparkConf: Map[String, String]
  ): String = {
    jdbcOptions.getOrElse(key, sparkConf.getOrElse(key, default))
  }
}
