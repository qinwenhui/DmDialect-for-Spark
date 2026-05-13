package cn.qinwh.spark.dm.version

/**
 * 达梦数据库版本特性矩阵
 *
 * 定义各版本支持的特性差异，用于在 DmDialect 中根据版本做出条件化行为。
 *
 * @param version 数据库版本
 *
 * @author qinwh
 */
class DmFeatureMatrix(val version: DmVersion) extends Serializable {

  /**
   * 是否支持 JSON 数据类型
   * DM8 开始支持 JSON 类型
   */
  def supportsJsonType: Boolean = version.major >= 8

  /**
   * 是否支持 INTERVAL 数据类型
   * DM8 原生支持
   */
  def supportsIntervalType: Boolean = version.major >= 8

  /**
   * 是否支持 BOOLEAN 类型
   * DM8 新增 BOOLEAN 类型，DM7 仅支持 BIT
   */
  def supportsBooleanType: Boolean = version.major >= 8

  /**
   * 是否支持 MERGE INTO 语法
   */
  def supportsMergeInto: Boolean = version.major >= 8

  /**
   * 是否支持 WINDOW 函数
   */
  def supportsWindowFunctions: Boolean = true

  /**
   * 是否支持 TABLESAMPLE
   */
  def supportsTableSample: Boolean = true

  /**
   * LISTAGG 函数行为：
   *   - DM7: LISTAGG 分隔符参数不支持空字符串
   *   - DM8: LISTAGG 分隔符参数支持空字符串
   */
  def listaggSupportsEmptyDelimiter: Boolean = version.major >= 8

  /**
   * 标识符大小写敏感性
   * 达梦默认大小写不敏感，除非使用双引号
   */
  def isCaseSensitiveIdentifiers: Boolean = false

  /**
   * 达梦存储子句中的 CLUSTERBTR 索引类型
   * DM8 推荐使用
   */
  def storageClause: String = {
    if (version.isDM8) {
      "STORAGE(ON \"MAIN\", CLUSTERBTR)"
    } else {
      "STORAGE(ON \"MAIN\")"
    }
  }
}

object DmFeatureMatrix {

  /** 创建特性矩阵 */
  def apply(version: DmVersion): DmFeatureMatrix = new DmFeatureMatrix(version)
}
