package cn.qinwh.spark.dm.utils

/**
 * 达梦方言全局常量定义
 *
 * 错误码基于达梦官方文档，通过 `SQLException.getErrorCode()` 获取。
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

  // ======================== 达梦错误码（通过 SQLException.getErrorCode() 获取） ========================
  //
  // 说明：达梦 JDBC 驱动返回的 SQLException.getErrorCode() 为负整数（如 -2007），
  // 而 getSQLState() 并非可靠的错误分类手段（如 "无效的表或者视图名" 错误码 -2106
  // 的 SQLState 并非 "42" 开头，导致 Spark 默认的 isObjectNotFoundException 误判失败）。
  // 因此所有异常分类以 errorCode 为唯一依据，SQLState 仅作为未知错误码时的降级兜底。

  // ---- 对象不存在 ----

  /** 无效的表或者视图名 */
  val ERR_TABLE_OR_VIEW_NOT_FOUND: Int = -2106

  /** 无效的模式名 */
  val ERR_SCHEMA_NOT_FOUND: Int = -2103

  /** 无效的用户名 */
  val ERR_INVALID_USER: Int = -2101

  /** 非法的基类名 */
  val ERR_INVALID_BASE_CLASS: Int = -3719

  /** 模式不属于当前用户 */
  val ERR_SCHEMA_NOT_OWNED: Int = -2510

  // ---- 语法/语义错误 ----

  /** 语法分析出错 */
  val ERR_SYNTAX_ERROR: Int = -2007

  /** 无效的 pivot 子句 */
  val ERR_INVALID_PIVOT: Int = -2038

  /** 不是 group by 表达式 */
  val ERR_NOT_GROUP_BY: Int = -4080

  /** 无法解析的成员表达式 */
  val ERR_UNRESOLVED_MEMBER: Int = -2207

  /** 无效的函数参数 */
  val ERR_INVALID_FUNC_ARG: Int = -3503

  /** 参数不兼容 */
  val ERR_INCOMPATIBLE_PARAM: Int = -5403

  /** 约束表达式无效 */
  val ERR_INVALID_CONSTRAINT: Int = -2670

  /** 无效的存储参数 */
  val ERR_INVALID_STORAGE_PARAM: Int = -3209

  /** 此查询表达式不允许 FOR UPDATE */
  val ERR_FOR_UPDATE_NOT_ALLOWED: Int = -4596

  /** 嵌套层次太深 */
  val ERR_NESTING_TOO_DEEP: Int = -3528

  // ---- 连接/认证异常 ----

  /** 无法连接到指定主机 */
  val ERR_CANNOT_CONNECT_HOST: Int = -6012

  /** 口令重复次数超限 */
  val ERR_PASSWORD_REUSE_LIMIT: Int = -2154

  /** 密钥长度过短 */
  val ERR_KEY_TOO_SHORT: Int = -2304

  /** RESTORE/RECOVER 相关 */
  val ERR_RESTORE_DB_MAGIC: Int = -129

  /** 服务器版本不一致，系统函数未找到 */
  val ERR_SERVER_VERSION_MISMATCH: Int = -3947

  // ---- 数据类型/值错误 ----

  /** 字符串转换出错 */
  val ERR_STRING_CONVERSION: Int = -6111

  /** 数据精度超出范围 */
  val ERR_DATA_PRECISION_OUT_OF_RANGE: Int = -6121

  /** 列长度超出定义 */
  val ERR_COLUMN_LENGTH_EXCEEDED: Int = -6169

  /** 数据类型的变更无效 */
  val ERR_INVALID_TYPE_CHANGE: Int = -6160

  /** 记录超长 */
  val ERR_RECORD_TOO_LONG: Int = -2665

  /** 试图在 blob 或者 clob 列上排序或比较 */
  val ERR_LOB_SORT_COMPARE: Int = -2685

  /** UTF string not integrated */
  val ERR_UTF_STRING_NOT_INTEGRATED: Int = -70009

  // ---- 约束/完整性违反 ----

  /** 试图删除被依赖对象 */
  val ERR_DROP_DEPENDENT_OBJECT: Int = -2639

  /** 试图删除被依赖列 */
  val ERR_DROP_DEPENDENT_COLUMN: Int = -2661

  /** 此列列表已索引 */
  val ERR_COLUMN_ALREADY_INDEXED: Int = -3236

  /** 分区列更新将引起分区的更改 */
  val ERR_PARTITION_COL_UPDATE: Int = -2167

  /** 同时包含聚集 KEY 和大字段 */
  val ERR_CLUSTER_KEY_AND_BIG_FIELD: Int = -3243

  /** 随机分布表不支持 UNIQUE 索引 */
  val ERR_UNIQUE_INDEX_NOT_SUPPORTED: Int = -2750

  /** 用户数据中的 CONNECT BY 循环 */
  val ERR_CONNECT_BY_LOOP: Int = -4030

  // ---- 超时/锁 ----

  /** 锁超时 */
  val ERR_LOCK_TIMEOUT: Int = -6407

  /** 当前对象被占用 */
  val ERR_OBJECT_OCCUPIED: Int = -6509

  /** 试图在事务运行中，改变其属性 */
  val ERR_TRANSACTION_PROPERTY_CHANGE: Int = -6510

  /** 检测到活动的自治事务，已回滚 */
  val ERR_AUTONOMOUS_TRANSACTION_ROLLBACK: Int = -6512

  // ---- 权限错误 ----

  /** 没有创建或修改视图权限 */
  val ERR_NO_VIEW_PRIVILEGE: Int = -5516

  /** 用户不能自己为自己 GRANT/REVOKE 权限 */
  val ERR_CANNOT_GRANT_SELF: Int = -5723

  // ---- IDENTITY 相关 ----

  /** 仅当 SET IDENTITY_INSERT 为 ON 时，才能对自增列赋值 */
  val ERR_IDENTITY_INSERT_OFF: Int = -2723

  // ---- 其他运行时错误 ----

  /** 加载第三方库失败 */
  val ERR_THIRD_PARTY_LIB_LOAD_FAILED: Int = -2870

  /** 数据文件大小无效 */
  val ERR_INVALID_DATA_FILE_SIZE: Int = -2410

  /** 对象处于无效状态 */
  val ERR_OBJECT_INVALID_STATE: Int = -7106

  /** 触发器运行时出错 */
  val ERR_TRIGGER_RUNTIME: Int = -7071

  /** 收集下标越界 */
  val ERR_COLLECT_INDEX_OUT_OF_BOUNDS: Int = -7198

  /** DBLINK 远程服务器获取对象失败 */
  val ERR_DBLINK_REMOTE_OBJECT_FAILED: Int = -2251

  // ---- to_date 相关错误码 ----

  val ERR_TO_DATE_6132: Int = -6132
  val ERR_TO_DATE_6133: Int = -6133
  val ERR_TO_DATE_6134: Int = -6134
  val ERR_TO_DATE_6136: Int = -6136
  val ERR_TO_DATE_6137: Int = -6137

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
