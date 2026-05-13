# DmDialect-for-Spark

[![Scala](https://img.shields.io/badge/Scala-2.13.18-red)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-4.1.1-orange)](https://spark.apache.org/)
[![JDK](https://img.shields.io/badge/JDK-17+-blue)](https://openjdk.org/)

Apache Spark JDBC 方言实现，支持达梦数据库（DM7/DM8）的无缝读写集成。

## 背景

Spark 通过 JDBC 访问数据库时，依靠 `JdbcDialect` 进行 **类型映射**、**SQL 语法适配**、**错误分类** 等操作。达梦数据库在这些方面有自身特色——社区版 Spark 未提供官方支持。本方言填补这一空白，**通过 SPI 机制零侵入集成**，引入 JAR 即可使用标准的 `spark.read.jdbc()` / `df.write.jdbc()` API。

## 特性

- **零侵入**：Java SPI 自动注册，无需代码改动
- **完整类型映射**：Spark `DataType` 与达梦 JDBC 类型的双向转换，支持 `TimestampNTZ` 开关和 CLOB 大字符串自动选择
- **SQL 语法适配**：双引号标识符引用、`LIMIT/OFFSET` 分页、`TABLESAMPLE` 采样、`COMMENT ON` 注释、达梦存储子句
- **多版本支持**：DM7/DM8 双版本，通过 `versionHint` 配置切换
- **达梦原生错误码**：基于 50+ 达梦官方错误码进行异常分类，修复 Spark 默认 `isObjectNotFoundException` 对达梦的误判
- **函数映射**：80+ Spark SQL 函数映射
- **可配置**：9 个配置项，三级优先级（Options > SparkConf > 默认值）

## 环境要求

| 组件 | 版本 |
|------|------|
| JDK | 17+ |
| Scala | 2.13.18 |
| Apache Spark | 4.1.1 |
| 达梦数据库 | DM7 / DM8 |
| 达梦 JDBC 驱动 | DmJdbcDriver18 |

## 快速开始

### 构建

```bash
git clone <仓库地址>
cd DmDialect-for-spark
mvn clean package
```

产物：`target/dm-dialect-for-spark-1.0.0-SNAPSHOT.jar`

### 在 Spark 项目中使用

**方式一：spark-submit / spark-shell**

```bash
spark-shell \
  --jars dm-dialect-for-spark-1.0.0-SNAPSHOT.jar,DmJdbcDriver18.jar
```

**方式二：Maven 依赖**

```xml
<dependency>
    <groupId>cn.qinwh.spark</groupId>
    <artifactId>dm-dialect-for-spark</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**方式三：将 JAR 放入 Spark 的 `jars/` 目录**

方言将通过 SPI 自动注册，无需额外代码配置。

### 读写达梦数据库

```scala
// 读取
val df = spark.read
  .format("jdbc")
  .option("url", "jdbc:dm://host:5236/DMSERVER")
  .option("dbtable", "SCHEMA.TABLE_NAME")
  .option("user", "username")
  .option("password", "password")
  .load()

// 写入
df.write
  .format("jdbc")
  .option("url", "jdbc:dm://host:5236/DMSERVER")
  .option("dbtable", "SCHEMA.NEW_TABLE")
  .option("user", "username")
  .option("password", "password")
  .save()

// 子查询作为数据源
val query = "(SELECT id, name FROM T_USER WHERE status = 1) as tmp"
val df = spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", query)
  .option("user", user)
  .option("password", password)
  .load()
```

## 配置项

所有配置键前缀为 `spark.dmdialect.*`，可通过 `.option()` 或 `spark.conf.set()` 设置。

| 配置键 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `spark.dmdialect.versionHint` | String | `8` | 达梦主版本提示：`7` 或 `8` |
| `spark.dmdialect.caseSensitive` | Boolean | `false` | DM 是否区分标识符大小写（默认不区分，自动转大写） |
| `spark.dmdialect.preferTimestampNTZ` | Boolean | `false` | TIMESTAMP 是否优先映射为 TimestampNTZType |
| `spark.dmdialect.legacyTimestampBehavior` | Boolean | `false` | 是否启用旧的 Timestamp 处理方式 |
| `spark.dmdialect.performance.batchSize` | Int | `1000` | JDBC 批量写入大小 |
| `spark.dmdialect.performance.fetchSize` | Int | `1000` | JDBC 读取 fetchSize |
| `spark.dmdialect.conn.useUnicode` | Boolean | `true` | JDBC 连接是否使用 Unicode |
| `spark.dmdialect.conn.characterEncoding` | String | `UTF-8` | JDBC 连接字符编码 |
| `spark.dmdialect.logging.enabled` | Boolean | `false` | 是否开启方言内部 DEBUG 日志 |
| `spark.dmdialect.logging.logQueries` | Boolean | `false` | 是否打印生成的 SQL 语句 |

## 架构

```
cn.qinwh.spark.dm
├── core          # 核心层：DmDialect 基类、DmDialectProvider SPI 入口、DM7/DM8 特化
├── config        # 配置管理：配置项定义 + 三级优先级解析
├── types         # 类型映射：Spark DataType ↔ 达梦 JDBC 类型
├── sql           # SQL 构建：DDL/DML/分页/采样/引号/注释
├── functions     # 函数映射：80+ Spark SQL → 达梦 函数对照
├── version       # 版本管理：DM 版本模型、检测、特性矩阵
└── utils         # 工具层：常量（含 50+ DM 错误码）、日志、字符串、异常分类
```

## 类型映射

| Spark 类型 | 达梦类型 |
|-----------|---------|
| BooleanType | BIT / BOOLEAN (DM8) |
| ByteType | TINYINT |
| ShortType | SMALLINT |
| IntegerType | INT |
| LongType | BIGINT |
| FloatType | FLOAT |
| DoubleType | DOUBLE PRECISION |
| DecimalType(p,s) | DECIMAL(p,s) / NUMBER |
| StringType (< 8188 bytes) | VARCHAR2(n) |
| StringType (≥ 8188 bytes) | CLOB |
| BinaryType | BLOB |
| DateType | DATE |
| TimestampType / TimestampNTZType | TIMESTAMP |
| YearMonthIntervalType | INTERVAL YEAR TO MONTH |
| DayTimeIntervalType | INTERVAL DAY TO SECOND |

## 许可证

Apache License 2.0
