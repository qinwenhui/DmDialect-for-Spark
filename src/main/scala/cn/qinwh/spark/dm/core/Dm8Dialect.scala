package cn.qinwh.spark.dm.core

import java.sql.Types

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.utils.DmConstants._
import cn.qinwh.spark.dm.version.{DmFeatureMatrix, DmVersion}

import org.apache.spark.sql.types._

/**
 * DM8 方言特化实现
 *
 * DM8 相比 DM7 新增特性：
 *   - 支持 JSON 数据类型，映射为 Spark StringType
 *   - 支持 BOOLEAN 原生类型
 *   - 支持 INTERVAL YEAR TO MONTH / INTERVAL DAY TO SECOND 原生类型
 *   - 存储子句支持 CLUSTERBTR 索引
 *   - LISTAGG 分隔符参数支持空字符串
 *
 * @param config 方言配置
 *
 * @author qinwh
 */
class Dm8Dialect(
  config: DmDialectConfig
) extends DmDialect(
  config = config,
  version = DmVersion.UNKNOWN,
  features = DmFeatureMatrix(DmVersion.UNKNOWN)
) {

  /**
   * DM8 类型映射覆盖，专门处理 JSON 类型
   *
   * 达梦的 JSON 类型在 JDBC 元数据中通常标记为 Types.OTHER，
   * 通过类型名称 "JSON" 来识别。
   */
  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = {
    val upperTypeName = if (typeName != null) typeName.toUpperCase.trim else ""

    (sqlType, upperTypeName) match {
      // DM8 JSON 类型 -> StringType
      case (Types.OTHER, DM_TYPE_JSON) =>
        Some(StringType)

      // DM8 BOOLEAN 类型（如果 jdbc 驱动报告为 Types.BIT 但类型名为 BOOLEAN）
      case (Types.BIT, "BOOLEAN") if features.supportsBooleanType =>
        Some(BooleanType)

      // 默认委托给基类处理
      case _ =>
        super.getCatalystType(sqlType, typeName, size, md)
    }
  }

  override def toString: String = s"Dm8Dialect(v${version})"
}

object Dm8Dialect {
  def apply(config: DmDialectConfig): Dm8Dialect = new Dm8Dialect(config)
}
