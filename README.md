# Metabase Impala Driver

这是一个为Metabase设计的Apache Impala数据库驱动程序，基于ClickHouse驱动的实现模式开发。

## 安装说明

### 1. 下载驱动JAR包

从releases页面下载 `metabase-impala-driver.jar` 文件。

### 2. 下载Impala JDBC驱动

**重要**: 此驱动包不包含Impala JDBC驱动依赖，您需要单独下载：

#### 选项1: 从Cloudera官方仓库下载Impala JDBC驱动
- 从Cloudera官方仓库下载Impala JDBC驱动
- 下载地址: https://repository.cloudera.com/repository/cloudera-repos/Impala/ImpalaJDBC42/2.6.33.1062/ImpalaJDBC42-2.6.33.1062.jar
#### 选项2: 使用Maven下载Impala JDBC驱动
- 在项目的`pom.xml`中添加以下依赖：
```xml
<dependency>
    <groupId>Impala</groupId>
    <artifactId>ImpalaJDBC42</artifactId>
    <version>2.6.33.1602</version>
</dependency>
```
- 然后使用Maven构建项目，下载依赖。

### 3. 安装到Metabase

1. 将 `metabase-impala-driver.jar` 复制到Metabase的plugins目录
2. 将Impala JDBC驱动JAR文件也复制到同一个plugins目录
3. 重启Metabase

## 4. 连接配置

#  基本连接属性

- **主机**: Impala服务器地址
- **端口**: Impala JDBC端口(默认21050)
- **数据库**: 需要连接的Impala数据库名称（默认为default，可以指定多个，多个数据库之间用空格分隔）
- **用户名**: 数据库用户名(默认为空，表示不需要用户名)
- **密码**: 数据库密码（默认为空，表示不需要密码）

# 高级连接属性

- **SSL**: 是否启用SSL（默认关闭）
- **Scan all databases**：是否扫描所有的数据库（默认为否，表示只扫描当前指定的数据库，如果设置为是，则会自动扫描所有的数据库）

## 功能特性

- 支持基本的SQL查询
- 支持数据库内省（表、列、类型等）
- 支持Impala特有的数据类型
- 支持SSL连接
- 支持SSH隧道
- 支持Kerberos认证

## 故障排除

### 常见问题

1. **连接失败**: 确保Impala服务正在运行，端口21050可访问
2. **驱动未找到**: 确保JDBC驱动JAR文件在plugins目录中
3. **认证失败**: 检查用户名、密码或Kerberos配置

### 日志调试

在Metabase日志中查看详细错误信息：
```
tail -f /path/to/metabase/logs/metabase.log
```

## 依赖

- Apache Impala JDBC驱动
- Clojure相关依赖

## 开发

### 构建驱动

```bash
clojure -T:build jar
```

### 测试

```bash
clojure -M:test
```

## 许可证

本项目采用Apache 2.0许可证。

## 贡献

欢迎提交Issue和Pull Request！

## 版本历史

- v1.0.0: 初始版本，支持基本的Impala连接和查询功能