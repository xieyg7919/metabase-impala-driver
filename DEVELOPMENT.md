# Metabase Impala Driver 开发指南

本文档提供了Metabase Impala驱动的详细开发指南。

## 开发环境设置

### 前置要求

- Java 11 或更高版本
- Clojure CLI tools
- Git
- 文本编辑器或IDE（推荐IntelliJ IDEA + Cursive插件）

### 环境验证

```bash
# 检查Java版本
java -version

# 检查Clojure版本
clojure -version

# 检查Git版本
git --version
```

## 项目结构

```
impala-metabase/
├── deps.edn                    # 项目依赖和配置
├── build.clj                   # 构建脚本
├── README.md                   # 项目说明
├── DEVELOPMENT.md              # 开发指南（本文档）
├── deploy.bat                  # 部署脚本
├── dev.bat                     # 开发工具脚本
├── resources/
│   └── metabase-plugin.yaml    # Metabase插件配置
├── src/main/clojure/
│   └── metabase/driver/
│       └── impala.clj          # 驱动核心实现
├── test/
│   └── metabase/driver/
│       └── impala_test.clj     # 单元测试
└── target/                     # 构建输出目录
    └── metabase-impala-driver.jar
```

## 开发工作流

### 1. 快速开始

```bash
# 克隆项目
git clone <repository-url>
cd impala-metabase

# 构建项目
clojure -T:build uber

# 运行测试
clojure -M:test
```

### 2. 使用开发脚本

项目提供了便捷的开发脚本：

```bash
# Windows
dev.bat

# 选择相应的操作：
# 1. 构建项目
# 2. 运行测试
# 3. 清理构建
# 4. 启动REPL
# 5. 部署到本地Metabase
```

### 3. REPL开发

```bash
# 启动REPL
clojure -M:dev

# 在REPL中加载驱动
(require 'metabase.driver.impala)

# 重新加载代码（修改后）
(require 'metabase.driver.impala :reload)

# 测试驱动功能
(metabase.driver/available-drivers)
(metabase.driver/display-name :impala)
```

## 核心组件说明

### 1. 驱动注册 (`impala.clj`)

```clojure
;; 注册驱动
(driver/register! :impala :parent :sql-jdbc)
```

### 2. 连接规范

```clojure
;; 将连接详情转换为JDBC规范
(defmethod driver/connection-details->spec :impala
  [_ {:keys [host port db user password ssl] :as details}]
  ;; 实现连接字符串构建逻辑
  )
```

### 3. 功能支持声明

```clojure
;; 声明驱动支持的功能
(defmethod driver/database-supports? [:impala :basic-aggregations] [_ _ _] true)
(defmethod driver/database-supports? [:impala :full-join] [_ _ _] false)
```

### 4. 错误处理

```clojure
;; 详细的连接测试和错误诊断
(defn detailed-connection-test [details]
  (try
    ;; 连接测试逻辑
    (catch SQLException e
      ;; SQL异常处理
      )
    (catch Exception e
      ;; 通用异常处理
      )))
```

## 测试策略

### 1. 单元测试

位置：`test/metabase/driver/impala_test.clj`

```bash
# 运行所有测试
clojure -M:test

# 运行特定测试
clojure -M:test -n metabase.driver.impala-test
```

### 2. 集成测试

需要实际的Impala实例：

```clojure
;; 在测试文件中标记为集成测试
(deftest ^:integration connection-test
  ;; 测试实际连接
  )
```

运行集成测试：

```bash
# 需要配置实际的Impala连接信息
clojure -M:test -i integration
```

### 3. 手动测试

1. 构建驱动JAR
2. 部署到Metabase
3. 在Metabase UI中测试连接
4. 验证查询功能

## 调试技巧

### 1. 日志调试

```clojure
;; 在代码中添加日志
(require '[clojure.tools.logging :as log])

(log/info "Connection details:" details)
(log/debug "JDBC spec:" spec)
(log/error e "Connection failed")
```

### 2. REPL调试

```clojure
;; 在REPL中测试函数
(def test-details {:host "localhost" :port 21050})
(driver/connection-details->spec :impala test-details)

;; 测试连接
(driver/can-connect? :impala test-details)
```

### 3. Metabase日志

```bash
# 查看Metabase日志
tail -f /var/log/metabase.log | grep -i impala

# Windows
type "C:\metabase\logs\metabase.log" | findstr /i impala
```

## 常见开发任务

### 1. 添加新功能支持

```clojure
;; 在impala.clj中添加新的功能支持
(defmethod driver/database-supports? [:impala :new-feature] [_ _ _]
  true) ; 或 false
```

### 2. 修改连接逻辑

```clojure
;; 修改connection-details->spec方法
(defmethod driver/connection-details->spec :impala
  [_ details]
  ;; 新的连接逻辑
  )
```

### 3. 改进错误处理

```clojure
;; 在detailed-connection-test中添加新的错误处理
(catch SpecificException e
  {:status :error
   :message "Specific error message"
   :details (str e)})
```

### 4. 添加测试用例

```clojure
;; 在impala_test.clj中添加新测试
(deftest new-feature-test
  (testing "New feature functionality"
    (is (= expected-result (actual-function args)))))
```

## 性能优化

### 1. 连接池配置

```clojure
;; 在连接属性中设置连接池参数
{:MaxPoolSize "10"
 :MinPoolSize "1"
 :IdleTimeout "300"}
```

### 2. 查询优化

- 使用适当的索引
- 避免全表扫描
- 合理使用分区

### 3. 内存管理

- 及时关闭连接
- 避免内存泄漏
- 合理设置超时时间

## 发布流程

### 1. 版本准备

```bash
# 更新版本号
# 编辑 resources/metabase-plugin.yaml
version: "1.0.1"

# 更新 README.md 中的版本信息
```

### 2. 测试验证

```bash
# 运行完整测试套件
clojure -M:test

# 手动测试关键功能
# - 连接测试
# - 基本查询
# - 聚合查询
# - JOIN操作
```

### 3. 构建发布

```bash
# 清理并构建
clojure -T:build clean
clojure -T:build uber

# 验证JAR文件
jar -tf target/metabase-impala-driver.jar | head -20
```

### 4. 文档更新

- 更新 README.md
- 更新 CHANGELOG.md
- 更新安装指南

## 贡献指南

### 1. 代码风格

- 遵循Clojure社区标准
- 使用有意义的函数和变量名
- 添加适当的文档字符串
- 保持函数简洁

### 2. 提交规范

```bash
# 提交消息格式
type(scope): description

# 例如：
feat(driver): add SSL connection support
fix(connection): handle timeout errors properly
docs(readme): update installation instructions
```

### 3. Pull Request流程

1. Fork项目
2. 创建功能分支
3. 实现功能并添加测试
4. 确保所有测试通过
5. 提交Pull Request
6. 代码审查
7. 合并到主分支

## 故障排除

### 1. 构建问题

```bash
# 清理依赖缓存
rm -rf ~/.m2/repository/
clojure -P  # 重新下载依赖
```

### 2. 测试失败

```bash
# 详细测试输出
clojure -M:test --reporter documentation

# 运行单个测试
clojure -M:test -v metabase.driver.impala-test/specific-test
```

### 3. REPL问题

```bash
# 重启REPL
# 检查classpath
(System/getProperty "java.class.path")

# 检查已加载的命名空间
(all-ns)
```

## 资源链接

- [Metabase驱动开发文档](https://www.metabase.com/docs/latest/developers-guide/drivers/start)
- [Clojure官方文档](https://clojure.org/)
- [Apache Impala文档](https://impala.apache.org/docs/build/html/)
- [Cloudera JDBC驱动文档](https://docs.cloudera.com/)

## 联系方式

如有问题或建议，请：

1. 查看本文档
2. 搜索已有的Issues
3. 创建新的Issue
4. 联系维护者