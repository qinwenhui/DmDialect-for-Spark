package cn.qinwh.spark.dm.utils

import org.slf4j.{Logger, LoggerFactory}

/**
 * 达梦方言日志工具
 *
 * 基于 SLF4J，通过 `spark.dmdialect.logging.enabled` 控制 DEBUG 日志输出，
 * 通过 `spark.dmdialect.logging.logQueries` 控制是否打印生成的 SQL。
 *
 * @author qinwh
 */
class DmLogger(debugEnabled: Boolean, logQueries: Boolean) extends Serializable {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(classOf[DmLogger])

  /** 记录调试信息（仅在 debugEnabled 为 true 时输出） */
  def debug(msg: => String): Unit = {
    if (debugEnabled) {
      logger.debug(msg)
    }
  }

  /** 记录 SQL 查询语句（仅在 logQueries 为 true 时输出） */
  def logQuery(msg: => String): Unit = {
    if (logQueries) {
      logger.info(s"[DM-QUERY] $msg")
    }
  }

  /** 记录普通信息 */
  def info(msg: => String): Unit = {
    logger.info(msg)
  }

  /** 记录警告信息 */
  def warn(msg: => String): Unit = {
    logger.warn(msg)
  }

  /** 记录警告信息（含异常） */
  def warn(msg: => String, throwable: Throwable): Unit = {
    logger.warn(msg, throwable)
  }

  /** 记录错误信息 */
  def error(msg: => String): Unit = {
    logger.error(msg)
  }

  /** 记录错误信息（含异常） */
  def error(msg: => String, throwable: Throwable): Unit = {
    logger.error(msg, throwable)
  }
}

object DmLogger {

  /**
   * 创建 DmLogger 实例
   *
   * @param debugEnabled 是否启用 DEBUG 日志
   * @param logQueries   是否打印生成的 SQL 查询
   */
  def apply(debugEnabled: Boolean = false, logQueries: Boolean = false): DmLogger = {
    new DmLogger(debugEnabled, logQueries)
  }
}
