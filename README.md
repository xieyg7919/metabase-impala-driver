# Metabase Impala Driver

这是一个为Metabase设计的Apache Impala数据库驱动程序，基于ClickHouse驱动的实现模式开发。

## 安装说明

### 1. 下载驱动JAR包

从releases页面下载 `metabase-impala-driver.jar` 文件。

### 2. 下载Impala JDBC驱动

**重要**: 此驱动包不包含Impala JDBC驱动依赖，您需要单独下载：

#### 选项1: 使用Hive JDBC驱动（推荐）
Impala支持Hive JDBC接口，您可以下载Hive JDBC驱动：
- 下载地址: https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
- 推荐版本: 3.1.3

#### 选项2: 使用Cloudera Impala JDBC驱动
- 从Cloudera官网下载Impala JDBC驱动
- 下载地址: https://www.cloudera.com/downloads/connectors/impala/jdbc.html

### 3. 安装到Metabase

1. 将 `metabase-impala-driver.jar` 复制到Metabase的plugins目录
2. 将Impala JDBC驱动JAR文件也复制到同一个plugins目录
3. 重启Metabase

## 连接配置

### 使用Hive JDBC驱动连接Impala

- **主机**: Impala服务器地址
- **端口**: 21050 (默认Impala JDBC端口)
- **数据库**: default (或指定的数据库名)
- **用户名**: 您的用户名
- **密码**: 您的密码

### 连接字符串示例

```
jdbc:hive2://your-impala-host:21050/default;auth=noSasl
```

对于需要认证的环境：
```
jdbc:hive2://your-impala-host:21050/default;user=username;password=password
```

对于Kerberos认证：
```
jdbc:hive2://your-impala-host:21050/default;principal=impala/hostname@REALM
```

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

- Apache Hive JDBC驱动 (org.apache.hive/hive-jdbc) - 包含Impala JDBC驱动类
- Clojure相关依赖

**注意**: 虽然使用Hive JDBC依赖包，但实际使用的是其中包含的 `com.cloudera.impala.jdbc.Driver` 驱动类。

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