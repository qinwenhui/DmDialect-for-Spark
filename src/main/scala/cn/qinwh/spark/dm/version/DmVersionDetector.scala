package cn.qinwh.spark.dm.version

import java.sql.{Connection, ResultSet, Statement}

import cn.qinwh.spark.dm.utils.DmConstants._
import cn.qinwh.spark.dm.utils.DmLogger

/**
 * 达梦数据库版本检测器
 *
 * 通过执行 `SELECT * FROM V$VERSION` 系统视图查询，
 * 解析版本号（如 `8.1.2.192`），返回 DmVersion 对象。
 *
 * @param logger 日志记录器
 *
 * @author qinwh
 */
class DmVersionDetector(logger: DmLogger) {

  /**
   * 检测当前连接的达梦数据库版本
   *
   * @param connection JDBC 连接
   * @return 检测到的版本信息
   */
  def detect(connection: Connection): DmVersion = {
    var versionFromQuery: Option[DmVersion] = None

    // 方式一：通过 V$VERSION 查询
    versionFromQuery = queryVersion(connection, VERSION_QUERY)

    // 方式二：如果方式一失败，尝试通过 V$INSTANCE 查询
    if (versionFromQuery.isEmpty) {
      versionFromQuery = queryVersion(connection, INSTANCE_QUERY)
    }

    // 方式三：通过 JDBC DatabaseMetaData 获取
    val versionFromMeta = if (versionFromQuery.isEmpty) {
      detectFromMetadata(connection)
    } else {
      None
    }

    versionFromQuery
      .orElse(versionFromMeta)
      .getOrElse {
        logger.warn("无法检测到达梦数据库版本，使用默认值 DM8")
        DmVersion.UNKNOWN
      }
  }

  /**
   * 执行 SQL 查询并解析版本号
   */
  private def queryVersion(connection: Connection, query: String): Option[DmVersion] = {
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      stmt = connection.createStatement()
      stmt.setQueryTimeout(10)  // 版本查询不应超时过久
      rs = stmt.executeQuery(query)
      if (rs.next()) {
        val versionText = rs.getString(1)
        DmVersion.parse(versionText)
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logger.warn(s"通过查询 [$query] 检测版本失败: ${e.getMessage}")
        None
    } finally {
      safeClose(rs)
      safeClose(stmt)
    }
  }

  /**
   * 通过 JDBC DatabaseMetaData 获取版本信息
   */
  private def detectFromMetadata(connection: Connection): Option[DmVersion] = {
    try {
      val meta = connection.getMetaData
      val productVersion = meta.getDatabaseProductVersion
      val productName = meta.getDatabaseProductName

      logger.debug(s"数据库产品: $productName, 版本字符串: $productVersion")
      DmVersion.parse(productVersion)
    } catch {
      case e: Exception =>
        logger.warn(s"通过 DatabaseMetaData 检测版本失败: ${e.getMessage}")
        None
    }
  }

  /** 安全关闭 ResultSet */
  private def safeClose(rs: ResultSet): Unit = {
    if (rs != null) {
      try { rs.close() } catch { case _: Exception => }
    }
  }

  /** 安全关闭 Statement */
  private def safeClose(stmt: Statement): Unit = {
    if (stmt != null) {
      try { stmt.close() } catch { case _: Exception => }
    }
  }
}

object DmVersionDetector {
  def apply(logger: DmLogger): DmVersionDetector = new DmVersionDetector(logger)
}
