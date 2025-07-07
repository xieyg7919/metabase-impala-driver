# Metabase Impala 驱动

一个全面的 Apache Impala 数据库 Metabase 驱动程序，提供与 Metabase 分析平台的完整集成。

## 功能特性

### 核心功能
- **数据库连接管理**: 强大的连接处理，支持连接池
- **查询执行**: 优化的 SQL 查询执行，具有 Impala 特定优化
- **类型映射**: Impala 和 Metabase 数据类型之间的全面映射
- **错误处理**: 增强的错误消息和调试支持
- **配置管理**: 通过 EDN 文件进行灵活配置

### 支持的 Impala 功能
- ✅ 基本聚合函数 (COUNT, SUM, AVG, MIN, MAX)
- ✅ 标准差聚合函数
- ✅ 表达式和计算字段
- ✅ 嵌套查询和子查询
- ✅ 数据分箱
- ✅ 区分大小写的字符串过滤
- ✅ JOIN 操作 (LEFT, RIGHT, INNER)
- ✅ 正则表达式
- ✅ 日期/时间函数
- ✅ 字符串操作函数
- ❌ 原生参数 (Impala 不支持)
- ❌ FULL JOIN (Impala 支持有限)
- ❌ 百分位聚合 (Impala 支持有限)

### 支持的数据类型
- **数值型**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **字符串型**: STRING, VARCHAR, CHAR, TEXT
- **日期/时间型**: TIMESTAMP, DATE, TIME
- **复杂型**: ARRAY, MAP, STRUCT (基本支持)
- **二进制型**: BINARY

## 快速开始

### 前置要求

- **Java 21** 或更高版本
- **Maven 3.6+** 用于构建
- **Clojure CLI** (可选，用于基于 Clojure 的构建)
- **Metabase 0.54.6**

### 安装

1. **克隆仓库**:
   ```bash
   git clone <repository-url>
   cd impala-metabase
   ```

2. **安装 Impala JDBC 驱动**:
   ```bash
   # 运行自动安装脚本
   install-impala-driver.bat
   ```
   
   或手动安装:
   ```bash
   # 从 Cloudera 下载 ImpalaJDBC42.jar，然后:
   mvn install:install-file -Dfile=ImpalaJDBC42.jar -DgroupId=Impala -DartifactId=ImpalaJDBC42 -Dversion=2.6.26.1031 -Dpackaging=jar
   ```

3. **构建驱动**:
   ```bash
   # 使用构建脚本 (推荐)
   build.bat
   
   # 或使用 Clojure CLI 手动构建
   clj -T:build uber
   
   # 或使用 Maven
   mvn clean package
   ```

4. **在 Metabase 中安装**:
   - 将 `target/metabase-impala-driver.jar` 复制到您的 Metabase `plugins/` 目录
   - 重启 Metabase
   - Impala 驱动将出现在数据库连接选项中

## 配置

### 连接参数

| 参数 | 描述 | 默认值 | 必需 |
|------|------|--------|------|
| **主机** | Impala 服务器主机名或 IP | localhost | 是 |
| **端口** | Impala 服务器端口 | 21050 | 否 |
| **数据库** | 要连接的数据库名称 | default | 否 |
| **用户名** | 数据库用户名 | impala | 否 |
| **密码** | 数据库密码 | (空) | 否 |
| **使用 SSL** | 启用 SSL 连接 | false | 否 |

### 连接示例

```
主机: impala-cluster.example.com
端口: 21050
数据库: analytics
用户名: analyst
密码: ********
使用 SSL: true
```

## 开发

### 项目结构

```
impala-metabase/
├── src/main/clojure/metabase/driver/
│   └── impala.clj              # 主要驱动实现
├── resources/
│   └── metabase-plugin.yaml    # 插件配置
├── deps.edn                    # Clojure 依赖
├── pom.xml                     # Maven 配置
├── build.clj                   # 构建脚本
├── build.bat                   # Windows 构建脚本
└── install-impala-driver.bat   # 驱动安装脚本
```

### 从源码构建

1. **安装依赖**:
   ```bash
   # 安装 Impala JDBC 驱动
   install-impala-driver.bat
   ```

2. **开发构建**:
   ```bash
   # 使用 Clojure CLI
   clj -T:build clean
   clj -T:build uber
   
   # 使用 Maven
   mvn clean compile
   mvn package
   ```

3. **测试**:
   ```bash
   # 运行测试 (如果可用)
   clj -M:test
   # 或
   mvn test
   ```

### 支持的功能

✅ **已支持**:
- 基本聚合函数 (COUNT, SUM, AVG, MIN, MAX)
- 标准差聚合函数
- 数学表达式
- 字符串操作和正则表达式
- 日期/时间函数
- 嵌套查询和子查询
- 百分位聚合
- 字符串过滤器中的大小写敏感
- 分箱和分组

❌ **不支持**:
- 外键关系
- 时区转换
- 连接模拟
- 数据上传

## 故障排除

### 常见问题

1. **"找不到驱动" 错误**:
   - 确保 ImpalaJDBC42 驱动已安装到本地 Maven 仓库
   - 运行 `install-impala-driver.bat` 自动安装

2. **连接超时**:
   - 验证 Impala 服务器正在运行且可访问
   - 检查端口 21050 (或您的自定义端口) 的防火墙设置
   - 确保主机名/IP 正确

3. **身份验证失败**:
   - 验证用户名和密码
   - 检查用户是否有权访问指定的数据库
   - 某些 Impala 集群可能需要 Kerberos 身份验证 (目前不支持)

4. **SSL 连接问题**:
   - 确保您的 Impala 集群支持 SSL
   - 验证 SSL 证书配置正确

### 获取帮助

- 查看 [Metabase 文档](https://www.metabase.com/docs/)
- 查阅 [Impala JDBC 文档](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_jdbc.html)
- 在此仓库中提交 issue 以解决驱动特定问题

## 技术详情

### 依赖项

- **Metabase Core**: 0.54.6
- **Clojure**: 1.11.1
- **ImpalaJDBC42**: 2.6.26.1031
- **Java**: 21+

### 驱动实现

此驱动扩展了 Metabase 的 SQL-JDBC 驱动框架并实现了:

- 连接管理和验证
- SQL 查询生成和优化
- Impala 和 Metabase 之间的数据类型映射
- 日期/时间处理和格式化
- 模式和表内省

### 性能考虑

- 使用连接池以获得更好的性能
- 考虑对大表进行分区以加快查询速度
- Impala 在列式文件格式 (Parquet, ORC) 下工作最佳
- 使用适当的数据类型来优化存储和查询性能

## 许可证

Apache License 2.0

## 贡献

欢迎贡献！请:

1. Fork 此仓库
2. 创建功能分支
3. 进行更改
4. 如适用，添加测试
5. 提交 pull request

---

**注意**: 此驱动需要 Cloudera Impala JDBC 驱动，由于许可限制，必须从 Cloudera 单独下载。