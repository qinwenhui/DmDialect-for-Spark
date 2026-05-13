package cn.qinwh.spark.dm.utils

import java.sql.{SQLException, SQLTimeoutException}

import cn.qinwh.spark.dm.utils.DmConstants._

/**
 * 达梦数据库异常分类工具
 *
 * ==分类策略==
 * 以 `SQLException.getErrorCode()`（达梦厂商错误码，负整数）为主要分类依据。
 * 达梦的 `getSQLState()` 并不可靠——例如"无效的表或者视图名" (错误码 -2106)
 * 的 SQLState 并非 "42" 开头，导致 Spark 默认的 `isObjectNotFoundException`
 * 判断失败。这正是本项目存在的主要原因之一。
 *
 * 降级策略：当 errorCode 为 0 或未匹配到已知错误码时，退而使用 SQLState 兜底分类。
 *
 * @author qinwh
 */
object DmExceptionUtils {

  /**
   * 根据达梦异常信息返回异常分类标签
   *
   * 分类优先级：达梦 errorCode > SQLState > 消息关键词 > 默认未知
   *
   * @param ex 达梦 JDBC 驱动抛出的 SQLException
   * @return 异常分类标签，用于上游进一步包装为 Spark 异常
   */
  def classify(ex: SQLException): DmExceptionCategory = {
    val errorCode = ex.getErrorCode

    // 优先使用达梦 errorCode 分类
    val byErrorCode = classifyByErrorCode(ex, errorCode)
    if (byErrorCode.categoryName != "未知错误") return byErrorCode

    // errorCode 未识别，降级到 SQLState 分类
    val bySqlState = classifyBySqlState(ex)
    if (bySqlState.categoryName != "未知错误") return bySqlState

    // 最后尝试通过消息关键词判断
    classifyByMessage(ex)
  }

  /**
   * 通过达梦 errorCode 分类
   */
  private def classifyByErrorCode(ex: SQLException, errorCode: Int): DmExceptionCategory = {
    errorCode match {
      // ---- 对象不存在 ----
      case ERR_TABLE_OR_VIEW_NOT_FOUND => DmExceptionCategory.ObjectNotFound(ex)
      case ERR_SCHEMA_NOT_FOUND        => DmExceptionCategory.ObjectNotFound(ex)
      case ERR_INVALID_USER            => DmExceptionCategory.ObjectNotFound(ex)
      case ERR_INVALID_BASE_CLASS      => DmExceptionCategory.ObjectNotFound(ex)
      case ERR_DBLINK_REMOTE_OBJECT_FAILED => DmExceptionCategory.ObjectNotFound(ex)

      // ---- 语法/语义错误 ----
      case ERR_SYNTAX_ERROR            => DmExceptionCategory.SyntaxError(ex)
      case ERR_INVALID_PIVOT           => DmExceptionCategory.SyntaxError(ex)
      case ERR_NOT_GROUP_BY            => DmExceptionCategory.SyntaxError(ex)
      case ERR_UNRESOLVED_MEMBER       => DmExceptionCategory.SyntaxError(ex)
      case ERR_INVALID_FUNC_ARG        => DmExceptionCategory.SyntaxError(ex)
      case ERR_INCOMPATIBLE_PARAM      => DmExceptionCategory.SyntaxError(ex)
      case ERR_INVALID_CONSTRAINT      => DmExceptionCategory.SyntaxError(ex)
      case ERR_INVALID_STORAGE_PARAM   => DmExceptionCategory.SyntaxError(ex)
      case ERR_FOR_UPDATE_NOT_ALLOWED  => DmExceptionCategory.SyntaxError(ex)
      case ERR_NESTING_TOO_DEEP        => DmExceptionCategory.SyntaxError(ex)

      // ---- 连接/认证异常 ----
      case ERR_CANNOT_CONNECT_HOST     => DmExceptionCategory.ConnectionError(ex)
      case ERR_PASSWORD_REUSE_LIMIT    => DmExceptionCategory.AuthError(ex)
      case ERR_KEY_TOO_SHORT           => DmExceptionCategory.AuthError(ex)
      case ERR_RESTORE_DB_MAGIC        => DmExceptionCategory.ConnectionError(ex)
      case ERR_SERVER_VERSION_MISMATCH => DmExceptionCategory.ConnectionError(ex)

      // ---- 数据类型/值错误 ----
      case ERR_STRING_CONVERSION       => DmExceptionCategory.DataError(ex)
      case ERR_DATA_PRECISION_OUT_OF_RANGE => DmExceptionCategory.NumericOverflow(ex)
      case ERR_COLUMN_LENGTH_EXCEEDED  => DmExceptionCategory.DataTruncation(ex)
      case ERR_INVALID_TYPE_CHANGE     => DmExceptionCategory.TypeError(ex)
      case ERR_RECORD_TOO_LONG         => DmExceptionCategory.DataTruncation(ex)
      case ERR_LOB_SORT_COMPARE        => DmExceptionCategory.UnsupportedOperation(ex)
      case ERR_UTF_STRING_NOT_INTEGRATED => DmExceptionCategory.DataError(ex)

      // ---- 约束/完整性违反 ----
      case ERR_DROP_DEPENDENT_OBJECT    => DmExceptionCategory.ConstraintViolation(ex)
      case ERR_DROP_DEPENDENT_COLUMN    => DmExceptionCategory.ConstraintViolation(ex)
      case ERR_COLUMN_ALREADY_INDEXED   => DmExceptionCategory.ConstraintViolation(ex)
      case ERR_PARTITION_COL_UPDATE     => DmExceptionCategory.ConstraintViolation(ex)
      case ERR_CLUSTER_KEY_AND_BIG_FIELD => DmExceptionCategory.UnsupportedOperation(ex)
      case ERR_UNIQUE_INDEX_NOT_SUPPORTED => DmExceptionCategory.UnsupportedOperation(ex)
      case ERR_CONNECT_BY_LOOP         => DmExceptionCategory.DataError(ex)

      // ---- 超时/锁 ----
      case ERR_LOCK_TIMEOUT              => DmExceptionCategory.Timeout(ex)
      case ERR_OBJECT_OCCUPIED           => DmExceptionCategory.LockError(ex)
      case ERR_TRANSACTION_PROPERTY_CHANGE => DmExceptionCategory.LockError(ex)
      case ERR_AUTONOMOUS_TRANSACTION_ROLLBACK => DmExceptionCategory.Timeout(ex)

      // ---- 权限错误 ----
      case ERR_NO_VIEW_PRIVILEGE       => DmExceptionCategory.PermissionError(ex)
      case ERR_CANNOT_GRANT_SELF       => DmExceptionCategory.PermissionError(ex)
      case ERR_SCHEMA_NOT_OWNED        => DmExceptionCategory.PermissionError(ex)

      // ---- IDENTITY ----
      case ERR_IDENTITY_INSERT_OFF     => DmExceptionCategory.UnsupportedOperation(ex)

      // ---- to_date 相关 ----
      case ERR_TO_DATE_6132 | ERR_TO_DATE_6133 | ERR_TO_DATE_6134 |
           ERR_TO_DATE_6136 | ERR_TO_DATE_6137 => DmExceptionCategory.SyntaxError(ex)

      // ---- 其他 ----
      case ERR_THIRD_PARTY_LIB_LOAD_FAILED => DmExceptionCategory.InternalError(ex)
      case ERR_INVALID_DATA_FILE_SIZE      => DmExceptionCategory.InternalError(ex)
      case ERR_OBJECT_INVALID_STATE        => DmExceptionCategory.InternalError(ex)
      case ERR_TRIGGER_RUNTIME             => DmExceptionCategory.InternalError(ex)
      case ERR_COLLECT_INDEX_OUT_OF_BOUNDS => DmExceptionCategory.InternalError(ex)

      // 未识别
      case _ => DmExceptionCategory.UnknownError(ex)
    }
  }

  /**
   * 降级：通过 SQLState 分类（仅当 errorCode 未命中时使用）
   */
  private def classifyBySqlState(ex: SQLException): DmExceptionCategory = {
    val sqlState = Option(ex.getSQLState).getOrElse("")

    sqlState match {
      case s if s.startsWith("08") => DmExceptionCategory.ConnectionError(ex)
      case s if s.startsWith("42") => DmExceptionCategory.SyntaxError(ex)
      case s if s.startsWith("22") => classifyDataException(ex)
      case s if s.startsWith("23") => DmExceptionCategory.ConstraintViolation(ex)
      case s if s.startsWith("28") => DmExceptionCategory.PermissionError(ex)
      case _ if ex.isInstanceOf[SQLTimeoutException] => DmExceptionCategory.Timeout(ex)
      case _ => DmExceptionCategory.UnknownError(ex)
    }
  }

  /**
   * 降级：通过异常消息关键词判断
   */
  private def classifyByMessage(ex: SQLException): DmExceptionCategory = {
    val msg = Option(ex.getMessage).getOrElse("").toLowerCase
    if (msg.contains("超时") || msg.contains("timeout")) {
      DmExceptionCategory.Timeout(ex)
    } else if (msg.contains("连接") || msg.contains("connection") || msg.contains("connect")) {
      DmExceptionCategory.ConnectionError(ex)
    } else {
      DmExceptionCategory.UnknownError(ex)
    }
  }

  /**
   * 细分 SQLState 数据异常类型
   */
  private def classifyDataException(ex: SQLException): DmExceptionCategory = {
    ex.getSQLState match {
      case "22001" => DmExceptionCategory.DataTruncation(ex)
      case "22003" => DmExceptionCategory.NumericOverflow(ex)
      case "22005" => DmExceptionCategory.InvalidValue(ex)
      case "22012" => DmExceptionCategory.DivideByZero(ex)
      case "22018" => DmExceptionCategory.InvalidCharacter(ex)
      case _       => DmExceptionCategory.DataError(ex)
    }
  }

  // ======================== Spark 兼容判断方法 ========================

  /**
   * 判断是否为对象不存在的错误（供 isObjectNotFoundException 使用）
   *
   * Spark 4.1.0+ 默认通过 SQLState.startsWith("42") 判断，
   * 但达梦的该错误码（如 -2106）SQLState 不是 42 开头。
   * 此处使用达梦 errorCode 进行精确匹配。
   */
  def isObjectNotFound(ex: SQLException): Boolean = {
    val errorCode = ex.getErrorCode
    errorCode == ERR_TABLE_OR_VIEW_NOT_FOUND ||
    errorCode == ERR_SCHEMA_NOT_FOUND ||
    errorCode == ERR_INVALID_USER ||
    errorCode == ERR_INVALID_BASE_CLASS
  }

  /**
   * 判断是否为语法错误（供 isSyntaxErrorBestEffort 使用）
   *
   * Spark 4.1.0+ 默认通过 SQLState.startsWith("42") 判断，
   * 此处使用达梦 errorCode 进行精确匹配。
   */
  def isSyntaxError(ex: SQLException): Boolean = {
    val errorCode = ex.getErrorCode
    errorCode == ERR_SYNTAX_ERROR ||
    errorCode == ERR_INVALID_PIVOT ||
    errorCode == ERR_NOT_GROUP_BY ||
    errorCode == ERR_UNRESOLVED_MEMBER ||
    errorCode == ERR_INVALID_FUNC_ARG ||
    errorCode == ERR_INCOMPATIBLE_PARAM ||
    errorCode == ERR_INVALID_CONSTRAINT ||
    errorCode == ERR_INVALID_STORAGE_PARAM ||
    errorCode == ERR_FOR_UPDATE_NOT_ALLOWED ||
    errorCode == ERR_NESTING_TOO_DEEP ||
    errorCode == ERR_TO_DATE_6132 ||
    errorCode == ERR_TO_DATE_6133 ||
    errorCode == ERR_TO_DATE_6134 ||
    errorCode == ERR_TO_DATE_6136 ||
    errorCode == ERR_TO_DATE_6137
  }
}

// ======================== 异常分类定义 ========================

/**
 * 达梦异常分类的密封特质
 */
sealed trait DmExceptionCategory {
  val exception: SQLException
  def categoryName: String
  def message: String
}

object DmExceptionCategory {

  /** 对象不存在（表、视图、模式等） */
  case class ObjectNotFound(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "对象不存在"
    override val message: String = s"达梦数据库对象不存在: ${DmStringUtils.safeMessage(exception.getMessage, "指定的表、视图或模式不存在")}"
  }

  /** 连接异常 */
  case class ConnectionError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "连接异常"
    override val message: String = s"达梦数据库连接异常: ${DmStringUtils.safeMessage(exception.getMessage, "无法连接到数据库")}"
  }

  /** 认证异常 */
  case class AuthError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "认证异常"
    override val message: String = s"达梦数据库认证异常: ${DmStringUtils.safeMessage(exception.getMessage, "认证失败")}"
  }

  /** 语法错误 */
  case class SyntaxError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "语法错误"
    override val message: String = s"达梦数据库 SQL 语法错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 数据类型错误 */
  case class TypeError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "类型错误"
    override val message: String = s"达梦数据库类型错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 数据截断 */
  case class DataTruncation(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "数据截断"
    override val message: String = s"达梦数据库数据截断错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 数值溢出 */
  case class NumericOverflow(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "数值溢出"
    override val message: String = s"达梦数据库数值溢出: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 除零错误 */
  case class DivideByZero(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "除零错误"
    override val message: String = s"达梦数据库除零错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 无效值 */
  case class InvalidValue(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "无效值"
    override val message: String = s"达梦数据库无效值: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 无效字符 */
  case class InvalidCharacter(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "无效字符"
    override val message: String = s"达梦数据库无效字符值: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 约束违反 */
  case class ConstraintViolation(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "约束违反"
    override val message: String = s"达梦数据库约束违反: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 通用数据异常 */
  case class DataError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "数据异常"
    override val message: String = s"达梦数据库数据异常: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 超时异常 */
  case class Timeout(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "超时异常"
    override val message: String = s"达梦数据库操作超时: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 锁异常 */
  case class LockError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "锁异常"
    override val message: String = s"达梦数据库锁异常: ${DmStringUtils.safeMessage(exception.getMessage, "对象被锁定或事务冲突")}"
  }

  /** 不支持的数据库操作 */
  case class UnsupportedOperation(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "不支持的操作"
    override val message: String = s"达梦数据库不支持该操作: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 权限错误 */
  case class PermissionError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "权限不足"
    override val message: String = s"达梦数据库权限不足: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 内部错误 */
  case class InternalError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "内部错误"
    override val message: String = s"达梦数据库内部错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }

  /** 未知异常 */
  case class UnknownError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "未知错误"
    override val message: String = s"达梦数据库未知错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }
}
