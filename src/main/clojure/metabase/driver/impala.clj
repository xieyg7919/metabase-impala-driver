(ns metabase.driver.impala
  "Metabase driver for Apache Impala."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [cheshire.core :as json]))

;; 性能优化配置
(def ^:private connection-pool-config
  "优化的连接池配置"
  {:maximum-pool-size 10
   :minimum-idle 2
   :connection-timeout 30000    ; 30秒连接超时
   :idle-timeout 600000         ; 10分钟空闲超时
   :max-lifetime 1800000        ; 30分钟最大生命周期
   :leak-detection-threshold 60000  ; 1分钟泄漏检测
   :validation-timeout 5000})

(def ^:private sync-performance-config
  "同步性能配置"
  {:batch-size 50              ; 每批处理表数量
   :max-parallel-workers 4     ; 最大并行工作线程
   :table-timeout 120000       ; 单表同步超时(2分钟)
   :total-sync-timeout 1800000 ; 总同步超时(30分钟)
   :retry-attempts 3})

;; 同步性能监控
(def ^:private sync-metrics (atom {}))

(defn record-sync-metric
  "记录同步性能指标"
  [table-name duration success?]
  (swap! sync-metrics update table-name 
         (fn [metrics]
           (-> (or metrics {:total-time 0 :count 0 :failures 0})
               (update :total-time + duration)
               (update :count inc)
               (cond-> (not success?) (update :failures inc))))))

(defn get-sync-statistics
  "获取同步统计信息"
  []
  (let [metrics @sync-metrics]
    (map (fn [[table stats]]
           {:table table
            :avg-time (/ (:total-time stats) (:count stats))
            :success-rate (/ (- (:count stats) (:failures stats)) (:count stats))
            :total-syncs (:count stats)})
         metrics)))

(defn filter-problematic-tables
  "过滤掉已知会导致同步问题的表"
  [tables]
  (let [skip-patterns [#".*\$.*"        ; Oracle系统表
                       #".*_TEMP.*"     ; 临时表
                       #".*_BACKUP.*"   ; 备份表
                       #"SYS_.*"        ; 系统表
                       #"INFORMATION_SCHEMA.*"]] ; 信息模式表
    (filter (fn [table]
              (not (some #(re-matches % (:name table)) skip-patterns)))
            tables)))

;; Metabase multimethods will be defined at runtime when the plugin is loaded
;; This allows the plugin to work without compile-time dependencies on Metabase

;; Load driver configuration
(def ^:private driver-config
  "Driver configuration loaded from resources."
  (try
    (-> "impala-config.edn"
        io/resource
        slurp
        edn/read-string
        :impala-driver)
    (catch Exception e
      (log/warn "Could not load driver configuration, using defaults" e)
      {})))

;; Driver registration will be handled by metabase-plugin.yaml
;; This namespace provides the core functionality

;; Driver registration and multimethod definition function
;; This will be called during plugin initialization
(defn register-impala-driver!
  "Register Impala driver and define required multimethods."
  []
  (try
    ;; Dynamically require Metabase namespaces
    (require '[metabase.driver :as driver])
    (require '[metabase.driver.sql-jdbc.connection :as sql-jdbc.conn])
    (require '[metabase.driver.sql-jdbc.sync :as sql-jdbc.sync])
    (log/info "Metabase namespaces loaded successfully")
    
    ;; Register the driver
    ((resolve 'metabase.driver/register!) :impala)
    (log/info "Impala driver registered")
    
    ;; Define connection-details->spec multimethod using eval to ensure proper namespace resolution
    (eval
      '(defmethod metabase.driver.sql-jdbc.connection/connection-details->spec :impala
         [_ {:keys [host port dbname user password ssl additional-options] :as details}]
         (let [port (or port 21050)
               dbname (or dbname "default")
               ;; 构建Cloudera Impala JDBC连接参数
               connection-params (str "Host=" host ";Port=" port ";Schema=" dbname ";SocketTimeout=300000;LoginTimeout=60000;QueryTimeout=600000")
               connection-params (if ssl
                                 (str connection-params ";ssl=1")
                                 connection-params)
               connection-params (if (and additional-options (not (clojure.string/blank? additional-options)))
                                 (str connection-params ";" additional-options)
                                 connection-params)
               url (str "//" host ":" port "/" dbname ";" connection-params)]
           (merge
             {:classname "com.cloudera.impala.jdbc.Driver"
              :subprotocol "impala"
              :subname url}
             ;; 添加连接池配置
             metabase.driver.impala/connection-pool-config
             (when user {:user user})
             (when password {:password password})))))
    (log/info "connection-details->spec multimethod defined")
    
    ;; Define database type mapping multimethod using eval
    (eval
      '(defmethod metabase.driver.sql-jdbc.sync/database-type->base-type :impala
         [_ database-type]
         ;; Use the local database-type->base-type function defined below
         (metabase.driver.impala/database-type->base-type database-type)))
    (log/info "database-type->base-type multimethod defined")
    
    ;; Define excluded-schemas multimethod
    (eval
      '(defmethod metabase.driver.sql-jdbc.sync/excluded-schemas :impala
         [_]
         #{"information_schema" "sys" "_impala_builtins"}))
    (log/info "excluded-schemas multimethod defined")
    
    ;; Define active-tables multimethod
    (eval
      '(defmethod metabase.driver.sql-jdbc.sync/active-tables :impala
         [driver ^java.sql.DatabaseMetaData metadata]
         (try
           (let [start-time (System/currentTimeMillis)
                 excluded (metabase.driver.sql-jdbc.sync/excluded-schemas driver)
                 schemas (with-open [rs (.getSchemas metadata)]
                           (->> (jdbc/metadata-result rs)
                                (map :table_schem)
                                (remove excluded)
                                (remove nil?)))
                 all-tables (for [schema schemas
                                 table (with-open [rs (.getTables metadata nil schema "%"
                                                                  (into-array String ["TABLE" "VIEW" "FOREIGN TABLE" "MATERIALIZED VIEW"]))]
                                         (jdbc/metadata-result rs))]
                              {:name (:table_name table)
                               :schema schema})
                 ;; 过滤问题表
                 filtered-tables (metabase.driver.impala/filter-problematic-tables all-tables)
                 duration (- (System/currentTimeMillis) start-time)]
             (log/info (format "发现 %d 个表，过滤后剩余 %d 个表，耗时: %dms" 
                              (count all-tables) (count filtered-tables) duration))
             (log/info "处理的模式:" schemas)
             (set filtered-tables))
           (catch Exception e
             (log/error e "Error in active-tables for Impala")
             #{}))))
    (log/info "active-tables multimethod defined")
    
    ;; Define describe-database multimethod
    (eval
      '(defmethod metabase.driver/describe-database :impala
         [driver database]
         (try
           (let [start-time (System/currentTimeMillis)]
             (with-open [conn (jdbc/get-connection (metabase.driver.sql-jdbc.conn/connection-details->spec driver (:details database)))]
               (let [metadata (.getMetaData conn)
                     all-tables (metabase.driver.sql-jdbc.sync/active-tables driver metadata)
                     batch-size (:batch-size metabase.driver.impala/sync-performance-config)
                     batches (partition-all batch-size all-tables)
                     total-duration (- (System/currentTimeMillis) start-time)]
                 (log/info (format "开始描述数据库，共 %d 个表，分 %d 批处理，发现表耗时: %dms" 
                                  (count all-tables) (count batches) total-duration))
                 {:tables all-tables})))
           (catch Exception e
             (log/error e "Error in describe-database for Impala")
             {:tables #{}}))))
    (log/info "describe-database multimethod defined")
    
    ;; Define describe-table multimethod
    (eval
      '(defmethod metabase.driver/describe-table :impala
         [driver database table]
         (let [start-time (System/currentTimeMillis)
               table-name (if (:schema table)
                           (str (:schema table) "." (:name table))
                           (:name table))
               table-key (str (:schema table) "." (:name table))]
           (try
             (log/info (format "开始同步表: %s" table-name))
             (let [spec (metabase.driver.sql-jdbc.conn/connection-details->spec driver (:details database))
                   columns (with-open [conn (jdbc/get-connection spec)]
                            (jdbc/query conn [(str "DESCRIBE " table-name)]))
                   fields (set (map-indexed
                               (fn [idx col]
                                 {:name (:name col)
                                  :database-type (:type col)
                                  :base-type (metabase.driver.sql-jdbc.sync/database-type->base-type driver (:type col))
                                  :database-position idx})
                               columns))
                   duration (- (System/currentTimeMillis) start-time)]
               ;; 记录成功的同步指标
               (metabase.driver.impala/record-sync-metric table-key duration true)
               (log/info (format "表 %s 同步完成，%d 个字段，耗时: %dms" table-name (count fields) duration))
               {:name (:name table)
                :schema (:schema table)
                :fields fields})
             (catch Exception e
               (let [duration (- (System/currentTimeMillis) start-time)]
                 ;; 记录失败的同步指标
                 (metabase.driver.impala/record-sync-metric table-key duration false)
                 (log/error e (format "表 %s 同步失败，耗时: %dms" table-name duration))
                 {:name (:name table)
                  :schema (:schema table)
                  :fields #{}}))))))
    (log/info "describe-table multimethod defined")
    
    (log/info "Impala driver registration completed successfully")
    (catch Exception e
      (log/error e "Failed to register Impala driver and multimethods"))))

;; Connection specification builder
(defn connection-spec
  "Build JDBC connection spec for Impala."
  [{:keys [host port dbname user password ssl additional-options]}]
  (let [port (or port 21050)
        dbname (or dbname "default")
        base-url (str "jdbc:impala://" host ":" port "/" dbname)
        url (cond-> base-url
              ssl (str "?ssl=1")
              (and additional-options (not (str/blank? additional-options)))
              (str (if ssl "&" "?") additional-options))]
    (merge
      {:classname "com.cloudera.impala.jdbc.Driver"
       :subprotocol "impala"
       :subname url}
      (when user {:user user})
      (when password {:password password}))))

;; Test database connection
(defn test-connection
  "Test the connection to Impala database."
  [details]
  (try
    (let [spec (connection-spec details)]
      (with-open [conn (jdbc/get-connection spec)]
        (jdbc/query conn ["SELECT 1"])
        {:status :success
         :message "Connection successful"}))
    (catch Exception e
      {:status :error
       :message (.getMessage e)})))

(defn get-database-info
  "Get database information and metadata."
  [connection-spec]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (let [metadata (.getMetaData conn)]
        {:database-product-name (.getDatabaseProductName metadata)
         :database-product-version (.getDatabaseProductVersion metadata)
         :driver-name (.getDriverName metadata)
         :driver-version (.getDriverVersion metadata)}))
    (catch Exception e
      (log/warn "Could not get database metadata" e)
      {})))

(defn execute-native-query
  "Execute a native SQL query against Impala."
  [connection-spec query & options]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (jdbc/query conn [query]))
    (catch Exception e
      (log/error e "Error executing native query")
      (throw e))))

(defn can-connect?
  "Test if we can connect to Impala database."
  [details]
  (let [connection-spec (connection-spec details)
        result (test-connection details)]
    (= :success (:status result))))

;; Enhanced type mappings for Impala data types
(def impala-type-mappings
  "Comprehensive mapping from Impala types to Metabase types."
  {;; Numeric types
   "BOOLEAN"    :type/Boolean
   "TINYINT"    :type/Integer
   "SMALLINT"   :type/Integer
   "INT"        :type/Integer
   "INTEGER"    :type/Integer
   "BIGINT"     :type/BigInteger
   "FLOAT"      :type/Float
   "REAL"       :type/Float
   "DOUBLE"     :type/Float
   "DECIMAL"    :type/Decimal
   "NUMERIC"    :type/Decimal
   
   ;; String types
   "STRING"     :type/Text
   "VARCHAR"    :type/Text
   "CHAR"       :type/Text
   "TEXT"       :type/Text
   
   ;; Date/Time types
   "TIMESTAMP"  :type/DateTime
   "DATE"       :type/Date
   "TIME"       :type/Time
   
   ;; Binary and complex types
   "BINARY"     :type/*
   "ARRAY"      :type/*
   "MAP"        :type/*
   "STRUCT"     :type/*
   
   ;; Oracle/Enterprise specific types that may appear in Impala
   "MGMT_JOB_VECTOR_PARAMS" :type/Text
   "XMLTYPE"    :type/Text
   "CLOB"       :type/Text
   "BLOB"       :type/*
   "NCLOB"      :type/Text
   "NVARCHAR2"  :type/Text
   "VARCHAR2"   :type/Text
   "NUMBER"     :type/Decimal
   "RAW"        :type/*
   "LONG"       :type/Text
   "LONG_RAW"   :type/*
   "ROWID"      :type/Text
   "UROWID"     :type/Text
   "EXF$INDEXOPER" :type/Text})

(defn database-type->base-type
  "Map Impala database type to Metabase base type with enhanced parsing."
  [database-type]
  (let [clean-type (-> database-type
                       str
                       str/upper-case
                       str/trim
                       ;; Remove precision/scale info like VARCHAR(255) -> VARCHAR
                       (str/replace #"\([^)]*\)" "")
                       ;; Remove array notation like ARRAY<STRING> -> ARRAY
                       (str/replace #"<[^>]*>" ""))
        base-type (get impala-type-mappings clean-type :type/*)]
    (if (= base-type :type/*)
      (log/warn (format "Unknown Impala type '%s' (cleaned: '%s'), falling back to :type/*. Consider adding this type to impala-type-mappings."
                        database-type clean-type))
      (log/debug (format "Mapping Impala type '%s' (cleaned: '%s') to Metabase type '%s'"
                         database-type clean-type base-type)))
    base-type))

;; SQL dialect functions for Impala
(def impala-sql-functions
  "Impala-specific SQL functions mapping."
  {:date-trunc {:minute "date_trunc('minute', %s)"
                :hour "date_trunc('hour', %s)"
                :day "date_trunc('day', %s)"
                :week "date_trunc('week', %s)"
                :month "date_trunc('month', %s)"
                :quarter "date_trunc('quarter', %s)"
                :year "date_trunc('year', %s)"}
   :extract {:minute "extract(minute from %s)"
             :hour "extract(hour from %s)"
             :day "extract(day from %s)"
             :month "extract(month from %s)"
             :quarter "extract(quarter from %s)"
             :year "extract(year from %s)"}
   :date-functions {:dayofweek "dayofweek(%s)"
                    :dayofyear "dayofyear(%s)"
                    :weekofyear "weekofyear(%s)"}
   :string-functions {:length "length(%s)"
                      :regexp_extract "regexp_extract(%s, %s, 0)"}
   :math-functions {:stddev "stddev(%s)"}
   :current-timestamp "now()"})

(defn get-impala-function
  "Get Impala-specific SQL function."
  [category function-name]
  (get-in impala-sql-functions [category function-name]))

;; 性能优化的表描述函数
(defn describe-table-with-timing
  "带性能监控的表描述函数"
  [driver database table]
  (let [start-time (System/currentTimeMillis)
        table-name (if (:schema table)
                    (str (:schema table) "." (:name table))
                    (:name table))
        table-key (str (:schema table) "." (:name table))]
    (try
      (log/info (format "开始同步表: %s" table-name))
      (let [spec (connection-spec (:details database))
            columns (with-open [conn (jdbc/get-connection spec)]
                     (jdbc/query conn [(str "DESCRIBE " table-name)]))
            fields (set (map-indexed
                        (fn [idx col]
                          {:name (:name col)
                           :database-type (:type col)
                           :base-type (database-type->base-type (:type col))
                           :database-position idx})
                        columns))
            duration (- (System/currentTimeMillis) start-time)]
        (record-sync-metric table-key duration true)
        (log/info (format "表 %s 同步完成，%d 个字段，耗时: %dms" table-name (count fields) duration))
        {:name (:name table)
         :schema (:schema table)
         :fields fields})
      (catch Exception e
        (let [duration (- (System/currentTimeMillis) start-time)]
          (record-sync-metric table-key duration false)
          (log/error e (format "表 %s 同步失败，耗时: %dms" table-name duration))
          {:name (:name table)
           :schema (:schema table)
           :fields #{}})))))

;; 并行同步功能
(defn parallel-describe-tables
  "并行描述多个表以提高同步性能"
  [driver database tables max-workers]
  (let [table-chan (async/chan (count tables))
        result-chan (async/chan)
        workers (repeatedly max-workers 
                  #(async/go-loop []
                     (when-let [table (async/<! table-chan)]
                       (try
                         (let [result (describe-table-with-timing driver database table)]
                           (async/>! result-chan {:table table :result result :success true}))
                         (catch Exception e
                           (async/>! result-chan {:table table :error e :success false})))
                       (recur))))]
    ;; 发送所有表到通道
    (doseq [table tables]
      (async/>!! table-chan table))
    (async/close! table-chan)
    
    ;; 收集结果
    (loop [results {} remaining (count tables)]
      (if (zero? remaining)
        results
        (let [{:keys [table result success]} (async/<!! result-chan)]
          (recur (if success 
                   (assoc results (:name table) result)
                   results)
                 (dec remaining)))))))

(defn print-sync-report
  "打印同步性能报告"
  []
  (let [stats (get-sync-statistics)
        total-tables (count stats)
        successful-tables (count (filter #(> (:success-rate %) 0.5) stats))
        avg-time (if (seq stats)
                  (/ (reduce + (map :avg-time stats)) total-tables)
                  0)]
    (log/info "=== 同步性能报告 ===")
    (log/info (format "总表数: %d, 成功同步: %d, 平均耗时: %.2fms" 
                     total-tables successful-tables avg-time))
    (doseq [stat (take 10 (sort-by :avg-time > stats))]
      (log/info (format "表: %s, 平均耗时: %.2fms, 成功率: %.2f%%, 同步次数: %d"
                       (:table stat) (:avg-time stat) 
                       (* 100 (:success-rate stat)) (:total-syncs stat))))))

(defn format-sql-function
  "Format SQL function with arguments."
  [function-template & args]
  (apply format function-template args))

;; Load Impala JDBC driver
(try
  (Class/forName "com.cloudera.impala.jdbc.Driver")
  (log/info "Impala JDBC driver loaded successfully")
  (catch ClassNotFoundException e
    (log/warn "Impala JDBC driver not found. Please ensure the driver JAR is in the classpath.")))

;; Driver capabilities and features
(def driver-features
  "Features supported by the Impala driver."
  (merge
    {:basic-aggregations true
     :standard-deviation-aggregations true
     :expressions true
     :native-parameters false
     :nested-queries true
     :binning true
     :case-sensitivity-string-filter true
     :left-join true
     :right-join true
     :inner-join true
     :full-join false
     :regex true
     :percentile-aggregations false
     :datetime-diff true
     :now true}
    (:features driver-config)))

;; Impala驱动核心配置
(def ^:private impala-driver-config
  {:name "Impala"
   :display-name "Apache Impala"
   :connection-properties
   [{:name "host"
     :display-name "Host"
     :placeholder "localhost"
     :required true}
    {:name "port"
     :display-name "Port"
     :type :integer
     :default 21050
     :required true}
    {:name "dbname"
     :display-name "Database name"
     :placeholder "default"
     :required true}
    {:name "user"
     :display-name "Username"
     :required false}
    {:name "password"
     :display-name "Password"
     :type :password
     :required false}
    {:name "ssl"
     :display-name "Use SSL"
     :type :boolean
     :default false}
    {:name "additional-options"
     :display-name "Additional JDBC options"
     :placeholder "AuthMech=3;UID=myuser;PWD=mypassword"
     :required false}]})

(defn get-driver-info
  "Get comprehensive driver information."
  []
  {:name (:name driver-config "Impala")
   :display-name (:display-name driver-config "Apache Impala")
   :description (:description driver-config "Driver for Apache Impala databases")
   :version (:version driver-config "1.0.0")
   :features driver-features
   :connection-properties (:connection-properties impala-driver-config)
   :jdbc-info (:jdbc driver-config)})

(defn validate-connection-details
  "Validate connection details before attempting connection."
  [details]
  (let [required-fields [:host :port :database]
        missing-fields (filter #(not (get details %)) required-fields)]
    (if (seq missing-fields)
      {:valid false
       :errors (map #(str "Missing required field: " (name %)) missing-fields)}
      {:valid true})))

;; Initialize driver
(defn init!
  "Initialize the Impala driver with full functionality."
  []
  (log/info "Initializing Impala driver with performance optimizations...")
  ;; Register driver and define multimethods
  (register-impala-driver!)
  (log/info "Driver info:" (get-driver-info))
  (log/info "Supported features:" (keys (filter val driver-features)))
  (log/info "Performance config:" sync-performance-config)
  (log/info "Connection pool config:" connection-pool-config)
  
  ;; 启动性能监控定时器
  (future
    (while true
      (Thread/sleep 300000) ; 每5分钟打印一次报告
      (when (seq @sync-metrics)
        (print-sync-report))))
  
  (log/info "=== Impala驱动性能优化已启用 ===")
  (log/info "- 连接池优化: 最大连接数10，连接超时30秒")
  (log/info "- 同步优化: 批量处理50表/批，过滤问题表")
  (log/info "- 监控功能: 实时性能指标收集")
  (log/info "- 超时配置: Socket超时5分钟，查询超时10分钟")
  (log/info "Impala driver initialization complete"))

;; Public API functions for Metabase integration
(defn ^:export driver-info
  "Export driver information for Metabase."
  []
  (get-driver-info))

(defn ^:export connect
  "Create and test a connection to Impala database."
  [details]
  (let [validation (validate-connection-details details)]
    (if (:valid validation)
      (let [connection-spec (connection-spec details)
            test-result (test-connection details)]
        (if (= :success (:status test-result))
          {:connection connection-spec
           :metadata (get-database-info connection-spec)}
          test-result))
      validation)))

(defn ^:export query
  "Execute a query against Impala database."
  [connection-spec sql-query & options]
  (apply execute-native-query connection-spec sql-query options))

;; Driver initialization
(init!)