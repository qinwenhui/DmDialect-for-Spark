package cn.qinwh.spark.dm.utils

/**
 * 达梦方言字符串工具类
 *
 * 提供标识符引用、SQL 字符串转义等工具方法。
 * 达梦数据库使用双引号 `"` 作为标识符引用符。
 *
 * @author qinwh
 */
object DmStringUtils {

  /** 达梦标识符引号 */
  private val QUOTE_CHAR: String = "\""

  /**
   * 使用达梦双引号规则引用标识符（列名、表名等）。
   *
   * - 将标识符用双引号包裹
   * - 正确处理关键字转义和嵌套引号（内部双引号转义为两个双引号）
   *
   * @param identifier 原始标识符
   * @return 带双引号的标识符
   */
  def quoteIdentifier(identifier: String): String = {
    if (identifier == null || identifier.isEmpty) {
      QUOTE_CHAR + QUOTE_CHAR
    } else if (identifier.startsWith(QUOTE_CHAR) && identifier.endsWith(QUOTE_CHAR)) {
      // 已经引用过，直接返回
      identifier
    } else {
      val escaped = identifier.replace(QUOTE_CHAR, QUOTE_CHAR + QUOTE_CHAR)
      QUOTE_CHAR + escaped + QUOTE_CHAR
    }
  }

  /**
   * 去除双引号引用
   *
   * @param identifier 带引号的标识符
   * @return 去除引号后的标识符
   */
  def unquoteIdentifier(identifier: String): String = {
    if (identifier == null || identifier.isEmpty) {
      ""
    } else if (identifier.startsWith(QUOTE_CHAR) && identifier.endsWith(QUOTE_CHAR)) {
      val inner = identifier.substring(1, identifier.length - 1)
      inner.replace(QUOTE_CHAR + QUOTE_CHAR, QUOTE_CHAR)
    } else {
      identifier
    }
  }

  /**
   * 转义 SQL 字符串字面量中的单引号
   *
   * @param value 原始字符串
   * @return 转义后的字符串
   */
  def escapeSqlString(value: String): String = {
    if (value == null) {
      "NULL"
    } else {
      "'" + value.replace("'", "''") + "'"
    }
  }

  /**
   * 判断字符串是否为空白或空
   *
   * @param str 待检查字符串
   * @return true 如果为空或仅包含空白字符
   */
  def isBlank(str: String): Boolean = {
    str == null || str.trim.isEmpty
  }

  /**
   * 安全地提取 SQLException 的错误消息
   *
   * @param message 原始消息
   * @return 清洗后的消息，若为空则返回默认值
   */
  def safeMessage(message: String, default: String = "达梦数据库未知错误"): String = {
    if (isBlank(message)) default else message
  }
}
