(ns metabase.driver.impala
  "Driver for Apache Impala databases"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.core.memoize :as memoize]
            [clojure.string :as str]
            [metabase.config :as config]
            [metabase.driver :as driver]
            ;[metabase.driver.clickhouse-introspection]
            ;[metabase.driver.clickhouse-nippy]
            ;[metabase.driver.clickhouse-qp]
            ;[metabase.driver.clickhouse-version :as clickhouse-version]
            [metabase.driver.impala-introspection]
            [metabase.driver.impala-qp]
            [metabase.driver.impala]
            [metabase.driver.ddl.interface :as ddl.i]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql-jdbc :as sql-jdbc]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql.util :as sql.u]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.query-processor.store :as qp.store]
            [metabase.upload :as upload]
            [metabase.util :as u]
            [java-time.api :as t]
            [metabase.util.log :as log]
            ))
;(System/setProperty "log4j2.contextSelector" "org.apache.logging.log4j.core.selector.BasicContextSelector")
(doseq [[k v] {"log4j2.contextSelector" "org.apache.logging.log4j.core.selector.BasicContextSelector"
               "log4j2.disableJmx" "true"}]
(System/setProperty k v))
(set! *warn-on-reflection* true)
(driver/register! :impala, :parent #{:sql-jdbc} )
(println "Impala Driver loading......")
(log/info "Impala Driver loading......")
(log/debug "Impala Driver loading......")

(defmethod driver/display-name :impala [_] "Apache Impala")
(def ^:private product-name "metabase/1.54.6")

(defmethod driver/prettify-native-form :clickhouse
  [_ native-form]
  (sql.u/format-sql-and-fix-params :mysql native-form))

(doseq [[feature supported?] {:standard-deviation-aggregations true
                              :now                             true
                              :set-timezone                    true
                              :convert-timezone                true
                              :test/jvm-timezone-setting       false
                              :test/date-time-type             true
                              :test/time-type                  true
                              :schemas                         true
                              :datetime-diff                   true
                              :upload-with-auto-pk             false
                              :window-functions/offset         false
                              :window-functions/cumulative     (not config/is-test?)
                              :left-join                       true
                              :describe-fks                    false
                              :actions                         false
                              :metadata/key-constraints        (not config/is-test?)}]
  (defmethod driver/database-supports? [:impala feature] [_driver _feature _db] supported?))

(def ^:private default-connection-details
  {:user "" :password "" :dbname "default" :host "localhost" :port "21050"})

;(defmethod sql-jdbc.execute/read-column-thunk [:impala java.sql.Types/TIMESTAMP]
;  ;[driver _ rs _ i]
;  [driver column-type ^java.sql.ResultSet rs meta ^Integer i]
;  (fn []
;    (try
;      (.getTimestamp rs i)
;      (catch java.sql.SQLException _
;        (try
;          (when-let [s (.getString rs i)]
;            (java.sql.Timestamp/valueOf s))
;          (catch Exception e
;            (log/warnf "读取Timestamp列失败(列%d): %s" i (.getMessage e))
;            nil))))))

(defmethod driver/database-supports? [:impala :set-timezone]
  [_driver _feature _db]
  (log/debug "检查时区支持情况")
  true) ; 确保返回true支持时区设置

(defmethod sql.qp/->honeysql [:impala java.time.LocalDateTime]
  [_ ^java.time.LocalDateTime t]
  [:raw (format "TIMESTAMP '%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSS" t))])

(defmethod sql.qp/->honeysql [:impala java.sql.Timestamp]
  [_ ^java.sql.Timestamp t]
  [:raw (format "TIMESTAMP '%s'" (.toString t))])

(defn- connection-details->spec* [details]
  (let [;; ensure defaults merge on top of nils
        details (reduce-kv (fn [m k v] (assoc m k (or v (k default-connection-details))))
                           default-connection-details
                           details)
        {:keys [user password dbname host port ssl impala-settings max-open-connections]} details
        ssl-val (if (boolean ssl) 1 0)
        dbname (first (str/split (str/trim dbname) #" "))
        host   (cond ; JDBCv1 used to accept schema in the `host` configuration option
                 (str/starts-with? host "http://")  (subs host 7)
                 (str/starts-with? host "https://") (subs host 8)
                 :else host)]
    (->
      {:classname                      "com.cloudera.impala.jdbc.Driver"
       ;:use_jvm_charset_converters      true      ;; 使用JVM字符转换
       ;:use_jvm_timestamp_with_timezone true ;; 使用JVM时区处理
       ;:use_server_time_zone_for_dates true
       :subprotocol                    "impala"
       :subname                        (str "//" host ":" port "/" dbname)
       :password                       (or password "")
       :user                           user
       :ssl                            ssl-val
       ;:timezone                       "UTC"
       ;:useTimezone                    false
       ;:useLegacyDatetimeCode          false
       ;:product_name                   product-name
       ;:remember_last_set_roles        true
       ;:http_connection_provider       "HTTP_URL_CONNECTION"
       ;:jdbc_ignore_unsupported_values "true"
       ;:jdbc_schema_term               "schema"
       ;:max_open_connections           (or max-open-connections 100)
       ; see also: https://github.com/xieyg7919/metabase-impala-driver
       ;:custom_http_params             (or impala-settings "")
       }
      (sql-jdbc.common/handle-additional-options details :separator-style :url))))


(defmethod sql-jdbc.conn/connection-details->spec :impala
  [_ details]
  (connection-details->spec* details))

(defmethod driver/can-connect? :impala
  [driver details]
  (if config/is-test?
    (try
      ;; Default SELECT 1 is not enough for Metabase test suite,
      ;; as it works slightly differently than expected there
      (let [spec  (sql-jdbc.conn/connection-details->spec driver details)
            db    (ddl.i/format-name driver (or (:dbname details) (:db details) "default"))]
        (sql-jdbc.execute/do-with-connection-with-options
          driver spec nil
          (fn [^java.sql.Connection conn]
            (let [stmt (.prepareStatement conn "select 1")
                  _    (.setString stmt 1 db)
                  rset (.executeQuery stmt)]
              (when (.next rset)
                (.getBoolean rset 1))))))
      (catch Throwable e
        (log/error e "An exception during ClickHouse connectivity check")
        false))
    ;; During normal usage, fall back to the default implementation
    (sql-jdbc.conn/can-connect? driver details)))

(defmethod driver/db-default-timezone :impala
  [driver database]
  (log/debug "获取数据库默认时区......")
  (sql-jdbc.execute/do-with-connection-with-options
    driver database nil
    (fn [^java.sql.Connection conn]
      (try
        ;(with-open [stmt (.createStatement conn)
        ;            rset (.executeQuery stmt "SELECT CONCAT('GMT', IF(offset_hours >= 0, '+', '-'), LPAD(CAST(ABS(offset_hours) AS STRING), 2, '0'), ':', LPAD(CAST(ABS(offset_minutes) AS STRING), 2, '0')) FROM (SELECT FLOOR((unix_timestamp() - unix_timestamp(utc_timestamp())) / 3600) AS offset_hours, MOD(FLOOR((unix_timestamp() - unix_timestamp(utc_timestamp())) / 60), 60) AS offset_minutes) t")]  ;; 使用Impala内置函数
        ;  (when (.next rset)
        ;    (let [tz-str (.getString rset 1)]
        ;      ;; 转换为标准时区格式
        ;      (cond
        ;        (re-find #"[+-]\d{2}:\d{2}" tz-str) tz-str
        ;        :else "GMT+00:00"))))  ;; 默认返回UTC
        "Asia/Shanghai"
        (catch Exception e
          (log/error "获取时区信息失败，将使用UTC作为默认时区。错误原因:" (.getMessage e))
          "Asia/Shanghai")))))  ;; 异常时返回UTC

(defmethod driver/db-start-of-week :impala [_] :monday)

(defmethod ddl.i/format-name :impala
  [_ table-or-field-name]
  (when table-or-field-name
    (str/replace table-or-field-name #"-" "_")))

(defn- quote-name [s]
  (let [parts (str/split (name s) #"\.")]
    (str/join "." (map #(str "`" % "`") parts))))

(println "Impala driver loaded")