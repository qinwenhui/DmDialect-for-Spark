# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**DmDialect-for-Spark** — 一个 Apache Spark (4.1.1) 的 JDBC 方言实现，使 Spark 能够通过 JDBC 无缝读写达梦数据库 (DM7/DM8)。通过 Java SPI 机制实现零侵入式自动注册，用户在 Spark 项目中引入本 JAR 即可自动生效。

- **包命名空间**: `cn.qinwh.spark.dm`
- **技术栈**: Scala 2.13.12, Maven 构建
- **Spark 依赖**: `spark-sql` (scope: `provided`)
- **达梦驱动依赖**: `DmJdbcDriver` (scope: `provided`)

## 架构设计

采用 **"抽象核心 + 版本特化 + 服务发现 (SPI)"** 三层架构:

1. **抽象核心** — `DmDialect` 抽象基类继承 `JdbcDialect`，封装所有达梦通用的类型映射、SQL 语法规则、连接管理。不依赖具体 DM 版本。
2. **版本特化** — `Dm7Dialect` 和 `Dm8Dialect` 各自处理版本独有特性（如 DM8 的 `JSON` 类型、`TIMESTAMP WITH TIME ZONE`）。运行时通过 `DmVersionDetector` 执行 `SELECT * FROM V$VERSION` 等系统查询自动判定版本。
3. **SPI 服务发现** — 在 `META-INF/services/org.apache.spark.sql.jdbc.JdbcDialect` 中注册 `DmDialectProvider` 工厂类。Spark 通过 `ServiceLoader` 自动发现，匹配 `jdbc:dm://` 开头的 URL。

## 项目状态

项目处于**启动阶段**。详细设计文档已就绪（`Spark达梦数据库方言项目开发设计文档.txt`），所有源代码、`pom.xml`、目录结构均待创建。

## 模块划分 (7个包)

| 包路径 | 职责 |
|--------|------|
| `cn.qinwh.spark.dm.core` | 核心层 — `DmDialect` 抽象基类、`DmDialectProvider` 工厂、连接管理 |
| `cn.qinwh.spark.dm.version` | 版本管理 — `DmVersionDetector` 运行时检测、版本特征矩阵 |
| `cn.qinwh.spark.dm.types` | 类型映射 — Spark `DataType` 与达梦 JDBC 类型的双向转换 |
| `cn.qinwh.spark.dm.sql` | SQL 构建 — 生成达梦兼容的 DDL/DML (建表/删表/CRUD) |
| `cn.qinwh.spark.dm.functions` | 函数映射 — Spark SQL 内置函数到达梦函数的映射 |
| `cn.qinwh.spark.dm.config` | 配置管理 — 从 `JDBCOptions` 解析配置项 |
| `cn.qinwh.spark.dm.utils` | 工具层 — 日志、常量、字符串、异常处理 |

## 关键设计要点

- **SPI 注册文件路径**: `src/main/resources/META-INF/services/org.apache.spark.sql.jdbc.JdbcDialect`
- **方言匹配 URL**: `jdbc:dm://` 前缀
- **标识符引用**: 双引号 `"` 规则
- **分页**: 支持 `LIMIT/OFFSET`（`supportsLimit`/`supportsOffset` 返回 `true`）
- **表采样**: 支持 `TABLESAMPLE` 语法
- **配置前缀**: `spark.dmdialect.*`，三级优先级: DataFrame Options > Spark Conf > 默认值
- **时间戳映射**: 默认 `TimestampType`，可通过 `preferTimestampNTZ=true` 切换为 `TimestampNTZType`
- **大字符串**: 超过 8188 字节自动映射为 `CLOB`
- **注释和文档**: 使用中文

## 开发约束

- 本机暂无 Scala 2.13 环境，先编写代码，后续提供编译器
- 暂无达梦数据库和 Spark 环境可连接测试，只需完成代码开发
- 只在本目录进行开发
- 依赖和大文件不要放到 C 盘
- Git 进行版本管理
- 作者: qinwh

## 常用命令 (待 pom.xml 创建后生效)

```bash
# 编译
mvn clean compile

# 打包
mvn clean package

# 生成源码包和文档包
mvn clean package -P release

# 运行测试 (待配置)
mvn test
```
