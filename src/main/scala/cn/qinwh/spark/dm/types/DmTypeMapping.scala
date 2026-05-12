package cn.qinwh.spark.dm.types

import java.sql.Types._

import cn.qinwh.spark.dm.config.DmDialectConfig
import cn.qinwh.spark.dm.utils.DmConstants._

import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types._

/**
 * 达梦数据库类型映射器
 *
 * 负责 Spark DataType 与 达梦 JDBC 类型之间的双向转换。
 * 支持通过 DmDialectConfig 控制 TimestampNTZ 等映射行为。
 *
 * @param config 达梦方言配置
 *
 * @author qinwh
 */
class DmTypeMapping(config: DmDialectConfig) {

  /**
   * 将达梦 JDBC 类型转换为 Spark Catalyst DataType
   *
   * @param sqlType  JDBC 类型常量 (java.sql.Types)
   * @param typeName 数据库类型名称
   * @param size     类型大小/精度
   * @param md       MetadataBuilder，用于设置 Decimal 精度等元数据
   * @return 对应的 Spark DataType
   */
  def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): DataType = {
    val upperTypeName = if (typeName != null) typeName.toUpperCase.trim else ""

    sqlType match {
      // 布尔类型
      case BIT | BOOLEAN =>
        BooleanType

      // 整数类型
      case TINYINT =>
        ByteType
      case SMALLINT =>
        ShortType
      case INTEGER =>
        IntegerType
      case BIGINT =>
        LongType

      // 浮点类型
      case REAL =>
        FloatType
      case FLOAT | DOUBLE =>
        DoubleType

      // 精确数值类型
      case NUMERIC | DECIMAL =>
        val scale = if (size < 0) 0 else scaleOfMd(md)
        val precision = if (size < 0) 10 else size
        DecimalType(precision, scale)

      // 字符串类型
      case CHAR | NCHAR | VARCHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR =>
        StringType

      // 大对象类型
      case CLOB | NCLOB =>
        StringType

      // 二进制类型
      case BINARY | VARBINARY | LONGVARBINARY | BLOB =>
        BinaryType

      // 日期时间类型
      case DATE =>
        DateType

      case TIME =>
        // 达梦的 TIME 类型，映射为 TimestampType 以兼容 Spark
        TimestampType

      case TIMESTAMP =>
        if (config.preferTimestampNTZ) {
          TimestampNTZType
        } else {
          TimestampType
        }

      // 其他尝试通过类型名匹配
      case OTHER =>
        resolveByTypeName(upperTypeName)

      // 空类型
      case NULL =>
        NullType

      // 间隔类型 (java.sql.Types 可能未定义标准值)
      case _ =>
        resolveByTypeName(upperTypeName)
    }
  }

  /**
   * 根据达梦数据库类型名称进行二次匹配
   *
   * 处理一些达梦特有的类型名称（如 JSON、TEXT、IMAGE 等）
   */
  private def resolveByTypeName(upperTypeName: String): DataType = {
    upperTypeName match {
      case DM_TYPE_JSON | DM_TYPE_TEXT =>
        StringType
      case DM_TYPE_IMAGE | DM_TYPE_BLOB =>
        BinaryType
      case name if name.contains("INTERVAL YEAR") || name.contains("INTERVAL MONTH") =>
        YearMonthIntervalType()
      case name if name.contains("INTERVAL DAY") || name.contains("INTERVAL HOUR") ||
        name.contains("INTERVAL MINUTE") || name.contains("INTERVAL SECOND") =>
        DayTimeIntervalType()
      case _ =>
        // 默认按字符串处理
        StringType
    }
  }

  /**
   * 将 Spark DataType 转换为达梦 JDBC 类型
   *
   * @param dt Spark DataType
   * @return 达梦 JDBC 类型定义
   */
  def getJDBCType(dt: DataType): JdbcType = dt match {
    case BooleanType =>
      JdbcType(s"$DM_TYPE_BIT", BIT)

    case ByteType =>
      JdbcType("TINYINT", TINYINT)

    case ShortType =>
      JdbcType("SMALLINT", SMALLINT)

    case IntegerType =>
      JdbcType("INT", INTEGER)

    case LongType =>
      JdbcType("BIGINT", BIGINT)

    case FloatType =>
      JdbcType("FLOAT", FLOAT)

    case DoubleType =>
      JdbcType("DOUBLE PRECISION", DOUBLE)

    case DecimalType.Fixed(precision, scale) =>
      JdbcType(s"DECIMAL($precision,$scale)", DECIMAL)

    case DecimalType.Unlimited =>
      JdbcType("NUMBER", DECIMAL)

    case StringType =>
      JdbcType(s"VARCHAR2($DEFAULT_VARCHAR2_SIZE)", VARCHAR)

    case BinaryType =>
      JdbcType("BLOB", BLOB)

    case DateType =>
      JdbcType("DATE", DATE)

    case TimestampNTZType =>
      JdbcType("TIMESTAMP", TIMESTAMP)

    case TimestampType =>
      JdbcType("TIMESTAMP", TIMESTAMP)

    case YearMonthIntervalType() =>
      JdbcType("INTERVAL YEAR TO MONTH", VARCHAR)

    case DayTimeIntervalType() =>
      JdbcType("INTERVAL DAY TO SECOND", VARCHAR)

    case _: CharType =>
      JdbcType("CHAR(1)", CHAR)

    case _: VarcharType =>
      JdbcType(s"VARCHAR2($DEFAULT_VARCHAR2_SIZE)", VARCHAR)

    case _: NullType =>
      JdbcType("NULL", NULL)

    case _ =>
      // 未知类型默认按 VARCHAR 处理
      JdbcType(s"VARCHAR2($DEFAULT_VARCHAR2_SIZE)", VARCHAR)
  }

  /**
   * 从 MetadataBuilder 中提取 scale 值
   */
  private def scaleOfMd(md: MetadataBuilder): Int = {
    try {
      md.build().getLong("scale").toInt
    } catch {
      case _: Exception => 0
    }
  }
}

object DmTypeMapping {
  def apply(config: DmDialectConfig): DmTypeMapping = new DmTypeMapping(config)
}
