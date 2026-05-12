package cn.qinwh.spark.dm.core

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.version.{DmFeatureMatrix, DmVersion}

/**
 * DM7 方言特化实现
 *
 * DM7 与 DM8 的主要差异：
 *   - 不支持 JSON 数据类型
 *   - 不支持 BOOLEAN 原生类型（仅支持 BIT）
 *   - 不支持 INTERVAL 原生类型
 *   - 存储子句不支持 CLUSTERBTR
 *   - LISTAGG 分隔符参数不支持空字符串
 *
 * @param config 方言配置
 *
 * @author qinwh
 */
class Dm7Dialect(
  config: DmDialectConfig
) extends DmDialect(
  config = config,
  version = DmVersion.UNKNOWN.copy(major = 7),
  features = DmFeatureMatrix(DmVersion.UNKNOWN.copy(major = 7))
) {

  override def toString: String = s"Dm7Dialect(v${version})"
}

object Dm7Dialect {
  def apply(config: DmDialectConfig): Dm7Dialect = new Dm7Dialect(config)
}
