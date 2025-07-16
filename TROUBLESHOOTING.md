# Impala Driver 故障排除指南

## 常见错误及解决方案

### 1. NullPointerException: "Cannot invoke 'java.lang.CharSequence.length()' because 's' is null"

**错误原因：**
- 连接详情中的必需字段（如 host、port、dbname、user）为 null 或未提供
- Metabase UI 中的表单字段可能没有正确填写

**解决方案：**
1. **检查连接详情**：确保所有必需字段都已填写
   - Host: Impala 服务器地址（不能为空）
   - Port: 端口号（默认 21050）
   - Database: 数据库名称（默认 "default"）
   - Username: 用户名（可以为空）
   - Password: 密码（可以为空）

2. **查看详细日志**：
   ```
   [impala-driver] Raw connection details: {...}
   [impala-driver] Cleaned connection details: {...}
   [impala-driver] Cleaned values - host: ... port: ... dbname: ... user: ...
   ```

3. **验证输入**：
   - Host 不能为 null 或空字符串
   - Port 必须是有效的数字
   - Database 名称不能包含特殊字符

### 2. 连接超时错误

**错误症状：**
- 连接测试长时间无响应
- 出现 "Connection timeout" 错误

**解决方案：**
1. **检查网络连接**：确保 Metabase 服务器可以访问 Impala 集群
2. **验证端口**：确认 Impala 服务运行在指定端口（通常是 21050）
3. **检查防火墙**：确保防火墙允许连接到 Impala 端口
4. **查看超时设置**：
   ```
   [impala-driver] Connection timeout: 600000 ms
   [impala-driver] Query timeout: 600000 ms
   ```

### 3. 认证失败

**错误症状：**
- "Authentication failed" 错误
- "Invalid username or password" 错误

**解决方案：**
1. **验证凭据**：确认用户名和密码正确
2. **检查认证方式**：Impala 驱动使用 LDAP 认证（AuthMech=3）
3. **查看 SSL 设置**：确认 SSL 配置与 Impala 集群一致

### 4. JDBC 驱动问题

**错误症状：**
- "No suitable driver found for jdbc:impala://..." 错误
- "No suitable driver found for jdbc:hive2://..." 错误

**解决方案：**
1. **检查驱动加载**：查看启动日志中的驱动加载信息：
   ```
   [impala-driver] Hive JDBC driver loaded successfully (supports Impala)
   [impala-driver] Hive driver registered with DriverManager
   ```

2. **验证 JDBC URL 格式**：
   - **正确格式**：`jdbc:hive2://host:port/database`
   - **错误格式**：`jdbc:impala://host:port/database`
   - 注意：我们使用 Hive JDBC 驱动连接 Impala，所以 URL 协议是 `hive2`

3. **检查依赖**：确认 `org.apache.hive/hive-jdbc` 依赖已正确包含

4. **重启 Metabase**：确保插件被正确加载

### 5. 驱动注册失败

**错误症状：**
- 多方法注册失败
- 驱动类找不到

**解决方案：**
1. **检查 JAR 文件**：确认 `metabase-impala-driver.jar` 在 `plugins` 目录中
2. **验证驱动注册**：查看启动日志中的驱动加载信息：
   ```
   [impala-driver] Loading Impala driver
   [impala-driver] Impala driver loaded successfully
   [impala-driver] connection-details->spec method registered for :impala
   ```
3. **检查类路径**：确保 Hive JDBC 驱动在类路径中

## 调试步骤

### 1. 启用详细日志
在 Metabase 启动时添加以下环境变量：
```bash
MB_LOG_LEVEL=DEBUG
```

### 2. 检查连接详情
查看日志中的连接详情输出：
```
[impala-driver] Raw connection details: {:host "...", :port ..., :dbname "...", :user "..."}
[impala-driver] Cleaned connection details: {:host "...", :port ..., :dbname "...", :user "..."}
[impala-driver] JDBC URL: jdbc:impala://host:port/database
```

### 3. 验证连接测试
查看连接测试的详细输出：
```
[impala-driver] Testing connection to host : port
[impala-driver] Connection timeout: 600000 ms
[impala-driver] Query timeout: 600000 ms
[impala-driver] Connection test result: SUCCESS
```

### 4. 检查错误堆栈
如果出现异常，查看完整的错误堆栈：
```
[impala-driver] An exception during Impala connectivity check
java.sql.SQLException: ...
```

## 常见配置问题

### 1. 端口配置
- **默认端口**：21050（Impala Daemon）
- **备用端口**：21000（Impala State Store）
- **JDBC 端口**：通常是 21050

### 2. SSL 配置
- **启用 SSL**：设置 SSL=1
- **禁用 SSL**：设置 SSL=0
- **证书验证**：确保证书配置正确

### 3. 超时配置
- **连接超时**：600 秒（10 分钟）
- **查询超时**：600 秒（10 分钟）
- **Socket 超时**：600 秒（10 分钟）

## 联系支持

如果问题仍然存在，请提供以下信息：

1. **完整的错误消息**
2. **Metabase 版本**
3. **Impala 版本**
4. **连接详情**（隐藏敏感信息）
5. **相关的日志输出**

这些信息将帮助快速诊断和解决问题。