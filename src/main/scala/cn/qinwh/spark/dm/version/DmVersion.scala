package cn.qinwh.spark.dm.version

/**
 * 达梦数据库版本信息
 *
 * @param major 主版本号（如 7 或 8）
 * @param minor 次版本号
 * @param patch 修订版本号
 * @param build 构建版本号
 *
 * @author qinwh
 */
case class DmVersion(
  major: Int,
  minor: Int,
  patch: Int,
  build: Int
) extends Ordered[DmVersion] {

  /** 是否为 DM7 系列 */
  def isDM7: Boolean = major == 7

  /** 是否为 DM8 系列 */
  def isDM8: Boolean = major >= 8

  /** 版本号字符串（如 "8.1.2.192"） */
  override def toString: String = s"$major.$minor.$patch.$build"

  /** 版本比较 */
  override def compare(that: DmVersion): Int = {
    val cmpMajor = this.major - that.major
    if (cmpMajor != 0) return cmpMajor
    val cmpMinor = this.minor - that.minor
    if (cmpMinor != 0) return cmpMinor
    val cmpPatch = this.patch - that.patch
    if (cmpPatch != 0) return cmpPatch
    this.build - that.build
  }
}

object DmVersion {

  /** 未知版本（检测失败时的默认值，假设为 DM8 最新版） */
  val UNKNOWN: DmVersion = DmVersion(8, 0, 0, 0)

  /**
   * 从版本字符串解析 DmVersion
   *
   * 支持的格式：
   *   - "DM Database Server 64 V8" 或包含版本号的描述行
   *   - "8.1.2.192" 纯版本号格式
   *
   * @param versionStr 版本字符串
   * @return 解析后的 DmVersion
   */
  def parse(versionStr: String): Option[DmVersion] = {
    if (versionStr == null || versionStr.isEmpty) return None

    // 匹配 x.y.z.w 格式的版本号
    val versionPattern = """(\d+)\.(\d+)\.(\d+)\.(\d+)""".r

    versionPattern.findFirstMatchIn(versionStr).map { m =>
      DmVersion(
        major = m.group(1).toInt,
        minor = m.group(2).toInt,
        patch = m.group(3).toInt,
        build = m.group(4).toInt
      )
    }
  }
}
