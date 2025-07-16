# Metabase Impala Driver 部署指南

## 部署步骤

### 1. 构建驱动

```bash
clojure -T:build jar
```

这将在 `target/metabase-impala-driver.jar` 生成驱动文件。

### 2. 部署到Metabase

1. **找到Metabase的plugins目录**：
   - 如果使用JAR包运行：在Metabase JAR文件同级目录创建 `plugins` 文件夹
   - 如果使用Docker：挂载 `/plugins` 目录
   - 如果使用源码：在Metabase根目录下的 `plugins` 文件夹

2. **复制驱动文件**：
   ```bash
   cp target/metabase-impala-driver.jar /path/to/metabase/plugins/
   ```

3. **重启Metabase**：
   - 完全停止Metabase服务
   - 重新启动Metabase

### 3. 验证安装

1. 启动Metabase后，检查日志中是否有驱动加载信息
2. 在管理界面 → 数据库 → 添加数据库，查看是否出现 "Impala" 选项

## 故障排除

### 问题1：找不到Impala数据库选项

**可能原因**：
- JAR文件没有正确放置在plugins目录
- Metabase没有完全重启
- 驱动依赖缺失

**解决方案**：
1. 确认plugins目录路径正确
2. 检查Metabase启动日志，查找错误信息
3. 确保JAR文件权限正确（可读）
4. 尝试删除Metabase缓存目录后重启

### 问题2：连接失败

**检查项目**：
- Impala服务器地址和端口是否正确
- 网络连接是否正常
- 认证信息是否正确
- 防火墙设置

### 问题3：驱动加载错误

**检查Metabase日志**：
```bash
tail -f /path/to/metabase/logs/metabase.log
```

查找包含 "impala" 或 "hive" 的错误信息。

#### 常见错误类型：

**ClassNotFoundException错误**：
如果看到类似以下的错误：
```
ClassNotFoundException: com.cloudera.impala.jdbc.Driver
```

**解决方案：**
1. 确认JAR文件包含了所有必要的依赖
2. 检查Metabase日志中的详细错误信息
3. 验证JAR文件没有损坏：`jar tf metabase-impala-driver.jar`

**多方法调度错误**：
如果看到类似以下的错误：
```
No method in multimethod 'connection-details->spec' for dispatch value: :impala
```

**解决方案：**
1. 确保使用最新版本的驱动JAR文件
2. 重启Metabase以重新加载插件
3. 检查JAR文件是否包含所有必要的命名空间：
   - `metabase/driver/impala.clj`
   - `metabase/driver/impala_introspection.clj` 
   - `metabase/driver/impala_qp.clj`
4. 验证`metabase-plugin.yaml`包含正确的初始化步骤

### SSL连接属性错误

如果看到类似以下的错误：
```
Connection property ssl has invalid value of false. Valid values are: 0 , 1.
```

**解决方案：**
1. 确保使用最新版本的驱动JAR文件（已修复SSL值转换问题）
2. 重启Metabase以重新加载插件
3. 在连接配置中，SSL选项会自动转换为正确的数字格式（0或1）

### 连接超时错误

#### 问题1: 10秒连接超时错误（Metabase全局超时覆盖）

**错误信息:**
```
ERROR driver.util :: Failed to connect to Database 
java.util.concurrent.TimeoutException: Timed out after 10.0 s
ERROR driver.impala :: An exception during Impala connectivity check 
java.sql.SQLException: [Cloudera][ImpalaJDBCDriver](500593) Communication link failure. 
Failed to connect to server. Reason: java.net.SocketTimeoutException: Read timed out. OpenSession.
```

**问题分析:**
- Metabase在全局层面设置了10秒的连接超时
- 这个超时设置覆盖了驱动的超时配置
- 需要在驱动层面强制覆盖Metabase的全局设置

**解决方案:**

1. **使用最新的驱动JAR文件**（包含强制超时覆盖机制）
   ```bash
   # 重新构建驱动
   clojure -T:build jar
   
   # 复制到Metabase插件目录
   cp target/metabase-impala-driver.jar /path/to/metabase/plugins/
   
   # 重启Metabase
   ```

2. **新版本驱动的强制超时机制**:
   - 在连接时强制设置系统属性覆盖Metabase全局超时
   - 在JDBC连接、语句和查询级别都设置10分钟超时
   - 使用多层超时保护：JDBC URL + 连接选项 + 系统属性 + 语句级别

3. **验证插件配置**
   - 新版本驱动会自动覆盖Metabase的10秒超时
   - 在Metabase数据库配置界面，超时字段的默认值应显示为600

4. **手动设置更长超时时间（如果需要）**
   ```
   连接超时: 1200 (20分钟)
   套接字超时: 1200 (20分钟)
   ```

5. **检查网络和服务器状态**
   - 确认Impala服务器正在运行
   - 检查网络连接和防火墙设置
   - 确保端口21050可访问

6. **JVM参数优化（可选）**
   ```bash
   # 在Metabase启动时添加以下JVM参数
   -Djava.net.preferIPv4Stack=true
   -Dsun.net.useExclusiveBind=false
   -Dmetabase.db.connection.timeout=600000
   -Dmetabase.db.query.timeout=600000
   ```

#### 问题2: JDBC层面的套接字超时

如果看到类似以下的错误：
```
Communication link failure. Failed to connect to server. Reason: java.net.SocketTimeoutException: Read timed out
```

**问题分析：**
- 这种错误通常是JDBC层面的套接字超时
- 可能的原因包括：
  1. Metabase全局连接超时设置覆盖了驱动配置
  2. 网络延迟或Impala服务器响应慢
  3. 防火墙或网络配置问题
  4. Impala服务器负载过高

**解决方案：**
1. **确保使用最新版本的驱动JAR文件**（已包含多层超时配置）：
   - JDBC URL级别超时：ConnTimeout=600000&SocketTimeout=600000
   - 连接池级别超时：loginTimeout、connectTimeout、socketTimeout
   - 连接选项级别超时：connection-timeout、socket-timeout
   - 网络级别超时：setNetworkTimeout

2. **重启Metabase**以重新加载插件和配置

3. **新版本驱动的多层超时配置**：
   - **默认连接超时**：600秒（10分钟）
   - **默认套接字超时**：600秒（10分钟）
   - **登录超时**：600秒
   - **网络超时**：600000毫秒

4. **如果仍然出现10秒超时**，可能需要在Metabase启动时设置JVM参数：
   ```
   -Dmetabase.db.connection.timeout=600000
   -Dmetabase.db.socket.timeout=600000
   ```

5. **手动设置更长的超时时间**，在"附加选项"中添加：
   ```
   ConnTimeout=1200000&SocketTimeout=1200000&loginTimeout=1200
   ```
   （以上示例为20分钟超时）

6. **诊断步骤**：
   - 查看Metabase完整日志以获取更多错误信息
   - 检查Impala服务器日志
   - 使用网络工具测试连接（如telnet、nc等）
   - 尝试使用其他JDBC客户端连接Impala

### 连接线程中断错误

如果看到类似以下的错误：
```
ERROR driver.impala :: An exception during Impala connectivity check 
 java.sql.SQLException: [Cloudera][ImpalaJDBCDriver](700114) Error occurred while trying to start a new connection thread, the exception type is java.lang.InterruptedException: None.
```

**问题分析：**
这个错误表明连接线程被中断，通常是由于：
1. 网络超时设置导致的线程中断
2. 连接池管理器强制中断连接线程
3. JVM或应用程序级别的线程管理冲突
4. Impala JDBC驱动与某些超时设置不兼容

**解决方案：**
1. **确保使用最新版本的驱动JAR文件**（已移除可能导致线程中断的配置）
2. **重启Metabase**以重新加载插件
3. **新版本驱动的优化**：
   - 移除了可能导致线程中断的网络超时设置
   - 简化了连接池级别的超时配置
   - 只保留JDBC URL级别的超时设置
4. **检查系统资源**：
   - 确保有足够的内存和CPU资源
   - 检查是否有其他进程占用过多资源
5. **如果问题持续**，可以尝试在Metabase启动时添加JVM参数：
   ```
   -Djava.net.useSystemProxies=false
   -Dcom.cloudera.impala.jdbc.disable_connection_pooling=true
   ```
6. **网络配置检查**：
   - 确保网络连接稳定
   - 检查防火墙和代理设置
   - 验证Impala服务器的连接限制配置

### Thrift传输层超时错误

如果遇到以下错误：
```
Caused by: com.cloudera.impala.support.exceptions.ErrorException: [Cloudera][ImpalaJDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: java.net.SocketTimeoutException: Read timed out. OpenSession.
Caused by: com.cloudera.impala.jdbc42.internal.apache.thrift.transport.TTransportException: java.net.SocketTimeoutException: Read timed out
```

**问题分析：**
- Thrift传输层在OpenSession阶段发生套接字读取超时
- 网络延迟或Impala服务器响应慢
- Thrift层面的超时配置不足
- 服务器负载过高导致会话建立缓慢

**解决方案：**

1. **使用最新的驱动JAR文件**（包含Thrift层面超时优化）
   ```bash
   # 重新构建并部署
   clojure -T:build jar
   cp target/metabase-impala-driver.jar /path/to/metabase/plugins/
   # 重启Metabase
   ```

2. **新版本驱动的Thrift优化**：
   - 添加了`ThriftTransportTimeout`和`ThriftSocketTimeout`配置
   - 设置了`OpenSessionTimeout`专门处理会话建立超时
   - 增加了重试机制（`MaxRetryCount=3`, `RetryIntervalInSeconds=5`）
   - 所有Thrift超时默认为10分钟

3. **手动设置更长的超时时间**（如果需要）：
   在"附加选项"中添加：
   ```
   ThriftTransportTimeout=1200000&ThriftSocketTimeout=1200000&OpenSessionTimeout=1200000
   ```
   （以上示例为20分钟超时）

4. **检查Impala服务器状态**：
   - 验证Impala服务器负载
   - 检查服务器日志是否有异常
   - 确认服务器有足够资源处理新连接

5. **网络诊断**：
   - 测试网络延迟：`ping impala-server`
   - 检查端口连通性：`telnet impala-server 21050`
   - 验证防火墙和网络配置

6. **JVM参数优化**：
   ```bash
   -Djava.net.preferIPv4Stack=true
   -Dsun.net.useExclusiveBind=false
   -Dcom.sun.management.jmxremote.ssl=false
   ```

### Log4j类加载冲突错误

如果看到类似以下的错误：
```
StatusLogger Unable to create custom ContextSelector. Falling back to default.
java.lang.ClassCastException: Cannot cast org.apache.logging.log4j.core.selector.BasicContextSelector
```

**解决方案：**
1. 确保使用最新版本的驱动JAR文件（已排除冲突的Log4j依赖）
2. 重启Metabase以重新加载插件
3. 这个错误通常不会影响驱动功能，但如果持续出现问题：
   - 检查Metabase日志中是否有其他相关错误
   - 确认JAR文件没有包含冲突的Log4j版本
4. 如果问题仍然存在，可以在Metabase启动时添加JVM参数：
   ```
   -Dlog4j2.contextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector
   ```

## 连接配置说明

### 基本配置
- **Host**: Impala服务器IP或域名
- **Port**: 端口号（默认21050）
- **Database**: 数据库名称（默认"default"）
- **Username**: 用户名
- **Password**: 密码

### 高级配置
- **SSL**: 启用SSL连接
- **SSH Tunnel**: SSH隧道配置
- **Kerberos**: Kerberos认证（如果启用）
- **Connection Timeout**: 连接超时时间
- **Socket Timeout**: Socket超时时间

### 连接URL示例
```
jdbc:impala://impala-server:21050/default;SSL=0;AuthMech=3
```

## 支持的功能

✅ **支持的功能**：
- 基本SQL查询
- 表和列内省
- 数据类型映射
- 聚合函数
- 日期时间函数
- 窗口函数
- 嵌套查询
- 左连接

❌ **不支持的功能**：
- 外键约束
- 时区设置
- 主键（Impala特性限制）
- 某些窗口函数偏移

## 调试功能

### 日志输出

新版本驱动包含详细的调试日志输出功能，可以帮助诊断连接问题：

#### 1. JDBC连接字符串调试
在连接建立时，驱动会输出以下调试信息：
```
=== Impala JDBC Connection Debug Info ===
JDBC URL: jdbc:impala://host:port/database;SSL=1;AuthMech=3;ConnTimeout=600000;...
Host: your-host Port: 21050 Database: your-db
SSL: 1
Connection Timeout (ms): 600000
Socket Timeout (ms): 600000
Auth Parameters: [SSL=1, AuthMech=3, ConnTimeout=600000, ...]
Auth String: SSL=1;AuthMech=3;ConnTimeout=600000;...
=========================================
```

#### 2. 连接选项调试
在实际连接时，驱动会输出：
```
=== Impala Connection Options Debug ===
Original options: {...}
Effective options: {:connection-timeout 600, :socket-timeout 600, ...}
System properties:
  metabase.db.connection.timeout: 600000
  metabase.db.query.timeout: 600000
=======================================
```

#### 3. 连接测试调试
在连接测试时，驱动会输出：
```
=== Impala Connection Test Debug ===
Testing connection with details: {...}
Forced options: {:connection-timeout 600000, ...}
System properties:
  metabase.db.connection.timeout: 600000
  metabase.db.query.timeout: 600000
  metabase.driver.connection.timeout: 600000
====================================
```

### 多方法注册问题

**问题描述**：
```
ERROR api.database :: Cannot connect to Database
clojure.lang.ExceptionInfo: No method in multimethod 'connection-details->spec' for dispatch value: :impala
```

**原因分析**：
这个错误表明Metabase无法找到`:impala`驱动的`connection-details->spec`多方法实现，通常由以下原因导致：

1. **命名空间加载顺序问题**：相关命名空间未在驱动注册前正确加载
2. **JAR文件损坏**：驱动JAR文件不完整或损坏
3. **Metabase版本兼容性**：驱动与当前Metabase版本不兼容
4. **类路径问题**：JAR文件未正确添加到Metabase的类路径

**解决方案**：
新版本驱动采用强制注册机制，通过直接调用多方法的`addMethod`来确保`:impala`方法被正确注册。驱动会在启动时输出详细的注册信息：

```
[impala-driver] Successfully registered :impala method for connection-details->spec
[impala-driver] Registration success: true
[impala-driver] Available methods: [...:impala...]
[impala-driver] :impala method registered: true
[impala-driver] Method test result: SUCCESS
```

**重要说明**：此版本使用了强制注册机制，即使在复杂的类加载环境中也能确保方法正确注册。

**手动解决步骤**：

1. **检查JAR文件**：确保使用最新构建的JAR文件
2. **重启Metabase**：完全重启Metabase服务
3. **检查日志**：查看Metabase启动日志中的`[impala-driver]`消息
4. **验证插件目录**：确保JAR文件在正确的plugins目录
5. **清理缓存**：删除Metabase的缓存文件和临时文件
6. **验证强制注册**：在日志中查找以下信息：
   ```
   [impala-driver] Successfully registered :impala method for connection-details->spec
   [impala-driver] Registration success: true
   [impala-driver] Available methods: [...:impala...]
   [impala-driver] :impala method registered: true
   [impala-driver] Method test result: SUCCESS
   ```
7. **如果方法注册失败**：
   - 检查是否有类路径冲突
   - 确保Metabase版本兼容（推荐v0.46+）
   - 尝试重新构建驱动JAR文件
   - 检查是否有其他Impala驱动插件冲突
   - 如果看到"Failed to register method"错误，可能存在更深层的类加载问题

### Log4j冲突解决方案

**问题描述**：
```
ERROR StatusLogger Unable to create custom ContextSelector. Falling back to default.
java.lang.ClassCastException: Cannot cast org.apache.logging.log4j.core.selector.BasicContextSelector to com.cloudera.impala.jdbc42.internal.apache.logging.log4j.core.selector.ContextSelector
```

**解决方案**：
新版本驱动已内置Log4j冲突解决机制：

1. **自动Log4j配置**：驱动会自动设置Log4j系统属性以避免冲突
2. **安全日志函数**：使用自定义的`safe-log`函数，在Log4j冲突时自动回退到System.out
3. **系统属性设置**：自动禁用Log4j2的JMX和自动配置功能

**手动解决方案**（如果仍有问题）：
在启动Metabase时添加以下JVM参数：
```bash
-Dlog4j2.disable.jmx=true
-Dlog4j.skipJansi=true
-Dlog4j2.contextSelector=org.apache.logging.log4j.core.selector.BasicContextSelector
-Dlog4j.configuration=
```

### 如何查看日志

1. **Metabase日志文件**：检查Metabase的日志文件（通常在logs目录下）
2. **控制台输出**：如果从命令行启动Metabase，调试信息会显示在控制台
3. **系统输出**：如果Log4j冲突，调试信息会输出到System.out（控制台）
4. **日志级别**：确保日志级别设置为INFO或DEBUG以查看调试信息

## 日志调试

如果遇到问题，可以启用详细日志：

1. 在Metabase配置中添加：
   ```
   MB_LOG_LEVEL=DEBUG
   ```

2. 查看特定的驱动日志：
   ```bash
   grep -i "impala" /path/to/metabase/logs/metabase.log
   ```

## 部署建议

1. **部署新JAR文件**：将重新构建的JAR文件部署到Metabase插件目录
2. **重启Metabase**：重启Metabase服务以加载新的驱动配置
3. **启用调试日志**：设置适当的日志级别以查看调试信息
4. **手动设置更长超时**：如果仍有问题，可在连接配置中手动设置20分钟超时
5. **监控Impala服务器**：确保Impala服务器运行正常，负载不过高
6. **网络诊断**：检查网络连接质量和延迟
7. **JVM参数优化**：添加以下JVM参数以进一步优化超时设置：
   ```bash
   -Dmetabase.db.connection.timeout=1200000
   -Dmetabase.db.query.timeout=1200000
   -Dcom.cloudera.impala.jdbc.timeout=1200000
   -Djava.net.timeout=1200000
   -Dsun.net.client.defaultConnectTimeout=1200000
   -Dsun.net.client.defaultReadTimeout=1200000
   ```

## 版本兼容性

- **Metabase**: 0.46.0+
- **Impala**: 2.x, 3.x, 4.x
- **Impala JDBC**: 2.6.17
- **Java**: 8+

## 预期的日志输出

成功部署后，在 Metabase 启动日志中应该看到：

```
[impala-driver] Loading Impala driver
[impala-driver] Impala driver loaded successfully
[impala-driver] connection-details->spec method registered for :impala
```

在创建或测试 Impala 连接时，应该看到详细的连接信息：

```
[impala-driver] Building connection spec
[impala-driver] JDBC URL: jdbc:impala://your-host:21050/default
[impala-driver] Connection properties: {:classname "com.cloudera.impala.jdbc.Driver", :subprotocol "impala", ...}
[impala-driver] Connecting to your-host : 21050 / default
[impala-driver] Testing connection to your-host : 21050
[impala-driver] Connection timeout: 30000 ms
[impala-driver] Query timeout: 600000 ms
[impala-driver] Connection test result: SUCCESS
```

## 获取帮助

如果遇到问题，请提供以下信息：
1. Metabase版本
2. Impala版本
3. 错误日志
4. 连接配置（隐藏敏感信息）
5. 网络环境描述