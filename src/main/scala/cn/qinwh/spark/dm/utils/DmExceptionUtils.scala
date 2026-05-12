package cn.qinwh.spark.dm.utils

import java.sql.{SQLException, SQLTimeoutException}

import cn.qinwh.spark.dm.utils.DmConstants._

/**
 * 达梦数据库异常分类工具
 *
 * 根据达梦 JDBC 驱动抛出的 SQLException 中的 SQLState 和 errorCode
 * 进行分类，将其映射为 Spark 友好的异常类型。
 *
 * @author qinwh
 */
object DmExceptionUtils {

  /**
   * 根据达梦异常信息返回异常分类标签
   *
   * @param ex 达梦 JDBC 驱动抛出的 SQLException
   * @return 异常分类标签，用于上游进一步包装为 Spark 异常
   */
  def classify(ex: SQLException): DmExceptionCategory = {
    val sqlState = Option(ex.getSQLState).getOrElse("")
    val errorCode = ex.getErrorCode

    sqlState match {
      // 连接异常 (08xxx)
      case s if s.startsWith(SQL_STATE_CONNECTION_PREFIX) =>
        DmExceptionCategory.ConnectionError(ex)

      // 语法错误或访问规则违反 (42xxx)
      case s if s.startsWith(SQL_STATE_SYNTAX_ERROR_PREFIX) =>
        DmExceptionCategory.SyntaxError(ex)

      // 数据异常 (22xxx)
      case s if s.startsWith(SQL_STATE_DATA_EXCEPTION_PREFIX) =>
        classifyDataException(ex, errorCode)

      // 完整性约束违反 (23xxx)
      case s if s.startsWith(SQL_STATE_INTEGRITY_PREFIX) =>
        DmExceptionCategory.ConstraintViolation(ex)

      // 超时
      case _ if ex.isInstanceOf[SQLTimeoutException] ||
        Option(ex.getMessage).exists(_.contains("超时")) =>
        DmExceptionCategory.Timeout(ex)

      // 未识别的异常
      case _ =>
        DmExceptionCategory.UnknownError(ex)
    }
  }

  /**
   * 细分数据异常类型
   */
  private def classifyDataException(ex: SQLException, errorCode: Int): DmExceptionCategory = {
    ex.getSQLState match {
      case "22001" => DmExceptionCategory.DataTruncation(ex)      // 数据截断
      case "22003" => DmExceptionCategory.NumericOverflow(ex)     // 数值溢出
      case "22005" => DmExceptionCategory.InvalidValue(ex)        // 赋值错误
      case "22012" => DmExceptionCategory.DivideByZero(ex)        // 除零错误
      case "22018" => DmExceptionCategory.InvalidCharacter(ex)    // 无效字符值
      case "22019" => DmExceptionCategory.InvalidEscapeChar(ex)   // 无效转义字符
      case _       => DmExceptionCategory.DataError(ex)           // 通用数据异常
    }
  }
}

/**
 * 达梦异常分类的密封特质
 */
sealed trait DmExceptionCategory {
  val exception: SQLException
  def categoryName: String
  def message: String
}

object DmExceptionCategory {

  /** 连接异常 */
  case class ConnectionError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "连接异常"
    override val message: String = s"达梦数据库连接异常: ${DmStringUtils.safeMessage(exception.getMessage, "无法连接到数据库")}"
  }

  /** 语法错误 */
  case class SyntaxError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "语法错误"
    override val message: String = s"达梦数据库 SQL 语法错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
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

  /** 无效转义字符 */
  case class InvalidEscapeChar(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "无效转义字符"
    override val message: String = s"达梦数据库无效转义字符: ${DmStringUtils.safeMessage(exception.getMessage)}"
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

  /** 未知异常 */
  case class UnknownError(exception: SQLException) extends DmExceptionCategory {
    override val categoryName: String = "未知错误"
    override val message: String = s"达梦数据库未知错误: ${DmStringUtils.safeMessage(exception.getMessage)}"
  }
}
