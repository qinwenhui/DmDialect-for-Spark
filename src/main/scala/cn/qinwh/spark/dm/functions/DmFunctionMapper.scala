package cn.qinwh.spark.dm.functions

/**
 * 达梦函数映射器
 *
 * 维护 Spark SQL 内置函数与达梦数据库函数的映射关系。
 * 处理函数名差异、参数顺序不一致等情况。
 *
 * @author qinwh
 */
object DmFunctionMapper {

  /**
   * 函数映射条目
   *
   * @param sparkFunction   Spark SQL 中的函数名
   * @param dmFunction      达梦数据库中对应的函数名
   * @param note            备注说明
   */
  case class FunctionMapping(
    sparkFunction: String,
    dmFunction: String,
    note: String = ""
  )

  /**
   * Spark SQL 函数到达梦函数的映射表
   *
   * 键为 Spark SQL 函数名（小写），值为对应的达梦函数名。
   */
  private val mappings: Map[String, FunctionMapping] = Map(
    // ======================== 日期时间函数 ========================
    "datediff"    -> FunctionMapping("datediff",  "DATEDIFF",    "日期差"),
    "date_add"    -> FunctionMapping("date_add",  "DATEADD",     "日期加间隔"),
    "date_sub"    -> FunctionMapping("date_sub",  "DATESUB",     "日期减间隔"),
    "add_months"  -> FunctionMapping("add_months","ADD_MONTHS",  "日期加月份"),
    "months_between" -> FunctionMapping("months_between", "MONTHS_BETWEEN", "月份差"),
    "last_day"    -> FunctionMapping("last_day",  "LAST_DAY",    "月最后一天"),
    "next_day"    -> FunctionMapping("next_day",  "NEXT_DAY",    "下一个指定星期"),
    "trunc"       -> FunctionMapping("trunc",     "TRUNC",       "日期截断"),
    "dayofyear"   -> FunctionMapping("dayofyear", "DAYOFYEAR",   "年中第几天"),
    "weekofyear"  -> FunctionMapping("weekofyear","WEEKOFYEAR",  "年中第几周"),
    "dayofmonth"  -> FunctionMapping("dayofmonth","DAY",         "月中第几天"),
    "dayofweek"   -> FunctionMapping("dayofweek", "DAYOFWEEK",   "周中第几天"),
    "day"         -> FunctionMapping("day",       "DAY",         "提取日期天"),
    "month"       -> FunctionMapping("month",     "MONTH",       "提取日期月"),
    "year"        -> FunctionMapping("year",      "YEAR",        "提取日期年"),
    "hour"        -> FunctionMapping("hour",      "HOUR",        "提取小时"),
    "minute"      -> FunctionMapping("minute",    "MINUTE",      "提取分钟"),
    "second"      -> FunctionMapping("second",    "SECOND",      "提取秒"),
    "current_date"    -> FunctionMapping("current_date",    "CURRENT_DATE",    "当前日期"),
    "current_timestamp" -> FunctionMapping("current_timestamp", "CURRENT_TIMESTAMP", "当前时间戳"),
    "unix_timestamp"   -> FunctionMapping("unix_timestamp",  "UNIX_TIMESTAMP", "Unix 时间戳"),
    "from_unixtime"    -> FunctionMapping("from_unixtime",   "FROM_UNIXTIME",  "Unix 时间戳转日期"),

    // ======================== 字符串函数 ========================
    "concat"      -> FunctionMapping("concat",    "CONCAT",      "字符串连接"),
    "concat_ws"   -> FunctionMapping("concat_ws", "CONCAT",      "带分隔符字符串连接（达梦 CONCAT 支持多参数）"),
    "substring"   -> FunctionMapping("substring", "SUBSTRING",   "子字符串"),
    "substr"      -> FunctionMapping("substr",    "SUBSTR",      "子字符串（别名）"),
    "instr"       -> FunctionMapping("instr",     "INSTR",       "查找子串位置"),
    "length"      -> FunctionMapping("length",    "LENGTH",      "字符串长度"),
    "char_length" -> FunctionMapping("char_length","CHAR_LENGTH", "字符长度"),
    "lower"       -> FunctionMapping("lower",     "LOWER",       "转小写"),
    "lcase"       -> FunctionMapping("lcase",     "LOWER",       "转小写（别名）"),
    "upper"       -> FunctionMapping("upper",     "UPPER",       "转大写"),
    "ucase"       -> FunctionMapping("ucase",     "UPPER",       "转大写（别名）"),
    "trim"        -> FunctionMapping("trim",      "TRIM",        "去除两端空格"),
    "ltrim"       -> FunctionMapping("ltrim",     "LTRIM",       "去除左侧空格"),
    "rtrim"       -> FunctionMapping("rtrim",     "RTRIM",       "去除右侧空格"),
    "replace"     -> FunctionMapping("replace",   "REPLACE",     "字符串替换"),
    "reverse"     -> FunctionMapping("reverse",   "REVERSE",     "字符串反转"),
    "repeat"      -> FunctionMapping("repeat",    "REPEAT",      "重复字符串"),
    "lpad"        -> FunctionMapping("lpad",      "LPAD",        "左填充"),
    "rpad"        -> FunctionMapping("rpad",      "RPAD",        "右填充"),
    "ascii"       -> FunctionMapping("ascii",     "ASCII",       "获取 ASCII 码"),
    "initcap"     -> FunctionMapping("initcap",   "INITCAP",     "首字母大写"),
    "translate"   -> FunctionMapping("translate", "TRANSLATE",   "字符翻译"),
    "regexp_replace" -> FunctionMapping("regexp_replace", "REGEXP_REPLACE", "正则替换"),
    "regexp_like"    -> FunctionMapping("regexp_like",    "REGEXP_LIKE",    "正则匹配"),
    "regexp_substr"  -> FunctionMapping("regexp_substr",  "REGEXP_SUBSTR",  "正则提取"),
    "space"       -> FunctionMapping("space",     "SPACE",       "生成空格字符串"),

    // ======================== 数学函数 ========================
    "abs"         -> FunctionMapping("abs",       "ABS",         "绝对值"),
    "ceil"        -> FunctionMapping("ceil",      "CEIL",        "向上取整"),
    "ceiling"     -> FunctionMapping("ceiling",   "CEIL",        "向上取整（别名）"),
    "floor"       -> FunctionMapping("floor",     "FLOOR",       "向下取整"),
    "round"       -> FunctionMapping("round",     "ROUND",       "四舍五入"),
    "mod"         -> FunctionMapping("mod",       "MOD",         "取模"),
    "power"       -> FunctionMapping("power",     "POWER",       "幂运算"),
    "pow"         -> FunctionMapping("pow",       "POWER",       "幂运算（别名）"),
    "sqrt"        -> FunctionMapping("sqrt",      "SQRT",        "平方根"),
    "exp"         -> FunctionMapping("exp",       "EXP",         "e 的幂"),
    "ln"          -> FunctionMapping("ln",        "LN",          "自然对数"),
    "log"         -> FunctionMapping("log",       "LOG",         "对数"),
    "sign"        -> FunctionMapping("sign",      "SIGN",        "符号函数"),
    "sin"         -> FunctionMapping("sin",       "SIN",         "正弦"),
    "cos"         -> FunctionMapping("cos",       "COS",         "余弦"),
    "tan"         -> FunctionMapping("tan",       "TAN",         "正切"),
    "asin"        -> FunctionMapping("asin",      "ASIN",        "反正弦"),
    "acos"        -> FunctionMapping("acos",      "ACOS",        "反余弦"),
    "atan"        -> FunctionMapping("atan",      "ATAN",        "反正切"),
    "degrees"     -> FunctionMapping("degrees",   "DEGREES",     "弧度转角度"),
    "radians"     -> FunctionMapping("radians",   "RADIANS",     "角度转弧度"),
    "rand"        -> FunctionMapping("rand",      "RAND",        "随机数"),
    "random"      -> FunctionMapping("random",    "RAND",        "随机数（别名）"),
    "greatest"    -> FunctionMapping("greatest",  "GREATEST",    "最大值"),
    "least"       -> FunctionMapping("least",     "LEAST",       "最小值"),

    // ======================== 条件/分支函数 ========================
    "if"          -> FunctionMapping("if",        "DECODE",      "条件判断（达梦无原生 IF 函数，使用 DECODE 或 CASE WHEN）"),
    "coalesce"    -> FunctionMapping("coalesce",  "COALESCE",    "返回第一个非空值"),
    "nullif"      -> FunctionMapping("nullif",    "NULLIF",      "相等返回 NULL"),
    "nvl"         -> FunctionMapping("nvl",       "NVL",         "空值替换"),
    "nvl2"        -> FunctionMapping("nvl2",      "NVL2",        "空值替换（三参数版）"),
    "decode"      -> FunctionMapping("decode",    "DECODE",      "条件值匹配"),

    // ======================== 聚合/窗口函数 ========================
    "count"       -> FunctionMapping("count",     "COUNT",       "计数"),
    "sum"         -> FunctionMapping("sum",       "SUM",         "求和"),
    "avg"         -> FunctionMapping("avg",       "AVG",         "平均值"),
    "min"         -> FunctionMapping("min",       "MIN",         "最小值"),
    "max"         -> FunctionMapping("max",       "MAX",         "最大值"),
    "stddev"      -> FunctionMapping("stddev",    "STDDEV",      "标准差"),
    "variance"    -> FunctionMapping("variance",  "VARIANCE",    "方差"),
    "listagg"     -> FunctionMapping("listagg",   "LISTAGG",     "字符串聚合"),

    // ======================== 类型转换函数 ========================
    "cast"        -> FunctionMapping("cast",      "CAST",        "类型转换"),
    "to_char"     -> FunctionMapping("to_char",   "TO_CHAR",     "转字符串"),
    "to_number"   -> FunctionMapping("to_number", "TO_NUMBER",   "转数值"),
    "to_date"     -> FunctionMapping("to_date",   "TO_DATE",     "转日期"),

    // ======================== 其他工具函数 ========================
    "md5"         -> FunctionMapping("md5",       "MD5",         "MD5 哈希"),
    "sha"         -> FunctionMapping("sha",       "SHA",         "SHA 哈希"),
    "sha1"        -> FunctionMapping("sha1",      "SHA1",        "SHA1 哈希"),
    "sha2"        -> FunctionMapping("sha2",      "SHA2",        "SHA2 哈希"),
    "crc32"       -> FunctionMapping("crc32",     "CRC32",       "CRC32 校验"),
    "uuid"        -> FunctionMapping("uuid",      "SYS_GUID",    "生成 UUID")
  )

  /**
   * 查找达梦数据库中对应的函数名
   *
   * @param sparkFunction Spark SQL 函数名
   * @return 达梦函数名，如果未映射则返回原始函数名
   */
  def getDmFunction(sparkFunction: String): String = {
    mappings.get(sparkFunction.toLowerCase).map(_.dmFunction).getOrElse(sparkFunction)
  }

  /**
   * 获取完整的函数映射信息
   *
   * @param sparkFunction Spark SQL 函数名
   * @return 映射信息（含备注），如果未找到则返回 None
   */
  def getMapping(sparkFunction: String): Option[FunctionMapping] = {
    mappings.get(sparkFunction.toLowerCase)
  }

  /**
   * 检查给定函数是否需要特殊参数处理
   *
   * @param sparkFunction Spark SQL 函数名
   * @return true 如果函数需要非标准的参数转换
   */
  def needsSpecialHandling(sparkFunction: String): Boolean = {
    sparkFunction.toLowerCase match {
      case "if" | "concat_ws" => true
      case _ => false
    }
  }

  /**
   * 检查函数是否为同名映射（忽略大小写）
   *
   * 仅当 Spark 函数名与达梦函数名完全一致（忽略大小写）时返回 true。
   * 对于名称不同的函数（如 datediff->DATEDIFF），返回 false。
   *
   * 此方法用于 isSupportedFunction，因为 JDBCSQLBuilder 只能
   * 检查函数是否支持，无法翻译函数名（内建 dialectFunctionName
   * 默认返回原名）。因此只有同名函数才能安全地推送到数据库执行。
   *
   * @param sparkFunction Spark SQL 函数名
   * @return 如果是同名映射则为 true
   */
  def isDirectMapping(sparkFunction: String): Boolean = {
    mappings.get(sparkFunction.toLowerCase).exists(m =>
      m.sparkFunction.equalsIgnoreCase(m.dmFunction))
  }

  /**
   * 获取所有已注册的函数映射
   *
   * @return 所有函数映射的 Map
   */
  def allMappings: Map[String, FunctionMapping] = mappings
}
