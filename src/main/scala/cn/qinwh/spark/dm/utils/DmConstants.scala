package cn.qinwh.spark.dm.utils

/**
 * 达梦方言全局常量定义
 *
 * @author qinwh
 */
object DmConstants {

  /** JDBC URL 前缀，用于方言识别 */
  val JDBC_URL_PREFIX: String = "jdbc:dm://"

  /** 配置键前缀 */
  val CONFIG_PREFIX: String = "spark.dmdialect"

  // ======================== 性能配置默认值 ========================

  /** 默认 JDBC 批量写入大小 */
  val DEFAULT_BATCH_SIZE: Int = 1000

  /** 默认 JDBC 读取 fetchSize */
  val DEFAULT_FETCH_SIZE: Int = 1000

  // ======================== 连接配置默认值 ========================

  /** 默认字符编码 */
  val DEFAULT_CHARACTER_ENCODING: String = "UTF-8"

  /** 默认是否使用 Unicode */
  val DEFAULT_USE_UNICODE: Boolean = true

  // ======================== 字符串/类型配置 ========================

  /** VARCHAR2 默认长度 */
  val DEFAULT_VARCHAR2_SIZE: Int = 255

  /** 超过此阈值自动使用 CLOB 类型 (字节数) */
  val CLOB_THRESHOLD_BYTES: Int = 8188

  // ======================== 达梦系统查询 ========================

  /** 查询达梦数据库版本的 SQL */
  val VERSION_QUERY: String = "SELECT * FROM V$VERSION"

  /** 查询达梦实例信息的 SQL（备用版本检测） */
  val INSTANCE_QUERY: String = "SELECT id_code FROM V$INSTANCE"

  // ======================== 达梦错误码范围 ========================

  /** 语法错误 SQLState 前缀 */
  val SQL_STATE_SYNTAX_ERROR_PREFIX: String = "42"

  /** 数据异常 SQLState 前缀 */
  val SQL_STATE_DATA_EXCEPTION_PREFIX: String = "22"

  /** 完整性约束违反 SQLState 前缀 */
  val SQL_STATE_INTEGRITY_PREFIX: String = "23"

  /** 连接异常 SQLState 前缀 */
  val SQL_STATE_CONNECTION_PREFIX: String = "08"

  // ======================== 达梦 JDBC 类型常量 ========================

  /** 达梦 JSON 类型名称（DM8 新增） */
  val DM_TYPE_JSON: String = "JSON"

  /** 达梦 TEXT 类型名称 */
  val DM_TYPE_TEXT: String = "TEXT"

  /** 达梦 IMAGE 类型名称 */
  val DM_TYPE_IMAGE: String = "IMAGE"

  /** 达梦 CLOB 类型名称 */
  val DM_TYPE_CLOB: String = "CLOB"

  /** 达梦 BLOB 类型名称 */
  val DM_TYPE_BLOB: String = "BLOB"

  /** 达梦 BIT 类型名称 */
  val DM_TYPE_BIT: String = "BIT"
}
