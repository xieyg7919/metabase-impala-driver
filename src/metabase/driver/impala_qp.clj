(ns metabase.driver.impala-qp
  "Apache Impala driver: QueryProcessor-related definition"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [java-time.api :as t]
            ;[metabase.driver.clickhouse-nippy]
            ;[metabase.driver.clickhouse-version :as clickhouse-version]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql.query-processor :as sql.qp :refer [add-interval-honeysql-form]]
            [metabase.driver.sql.util :as sql.u]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.legacy-mbql.util :as mbql.u]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.util :as u]
            [metabase.util.log :as log]
            [metabase.util.date-2 :as u.date]
            [metabase.util.honey-sql-2 :as h2x])
  (:import [java.sql ResultSet ResultSetMetaData Types]
           [java.time
            LocalDate
            LocalDateTime
            LocalTime
            OffsetDateTime
            OffsetTime
            ZonedDateTime]
           java.util.Arrays))

(defmethod driver/connection-properties :impala
  [_]
  (merge
    (sql-jdbc.conn/connection-properties (sql-jdbc.execute/the-driver))
    {:connect-timeout {:value 30  ;; 将连接超时从默认10秒增加到30秒
                       :default 10
                       :type :integer
                       :display-name "Connection timeout (seconds)"}}))

(def gmt-timezone-mapping
  {
   "GMT-12:00" "Etc/GMT+12"    ; 国际日期变更线以西
   "GMT-11:00" "Pacific/Midway" ; 萨摩亚时区
   "GMT-10:00" "Pacific/Honolulu" ; 夏威夷
   "GMT-09:00" "America/Anchorage" ; 阿拉斯加
   "GMT-08:00" "America/Los_Angeles" ; 太平洋时间(美加)
   "GMT-07:00" "America/Denver" ; 山地时间(美加)
   "GMT-06:00" "America/Chicago" ; 中部时间(美加)
   "GMT-05:00" "America/New_York" ; 东部时间(美加)
   "GMT-04:00" "America/Halifax" ; 大西洋时间(加拿大)
   "GMT-03:30" "America/St_Johns" ; 纽芬兰
   "GMT-03:00" "America/Argentina/Buenos_Aires" ; 布宜诺斯艾利斯
   "GMT-02:00" "America/Noronha" ; 费尔南多·迪诺罗尼亚群岛
   "GMT-01:00" "Atlantic/Azores" ; 亚速尔群岛
   "GMT+00:00" "UTC"            ; 格林尼治标准时间
   "GMT+01:00" "Europe/London"  ; 英国夏令时
   "GMT+02:00" "Europe/Paris"   ; 欧洲中部时间(夏令时)
   "GMT+03:00" "Europe/Moscow"  ; 莫斯科时间
   "GMT+04:00" "Asia/Dubai"     ; 阿联酋
   "GMT+05:00" "Asia/Karachi"   ; 巴基斯坦
   "GMT+05:30" "Asia/Kolkata"   ; 印度标准时间
   "GMT+06:00" "Asia/Dhaka"     ; 孟加拉
   "GMT+07:00" "Asia/Bangkok"   ; 泰国
   "GMT+08:00" "Asia/Shanghai"  ; 中国标准时间
   "GMT+09:00" "Asia/Tokyo"     ; 日本标准时间
   "GMT+10:00" "Australia/Sydney" ; 澳大利亚东部
   "GMT+11:00" "Pacific/Guadalcanal" ; 所罗门群岛
   "GMT+12:00" "Pacific/Auckland" ; 新西兰
   "GMT+13:00" "Pacific/Apia"   ; 萨摩亚
   "GMT+14:00" "Pacific/Kiritimati" ; 莱恩群岛
   })
;; 引用样式
(defmethod sql.qp/quote-style :impala[_] :mysql)

;; 获取报告时区ID
;(defn- get-report-timezone-id-safely
;  []
;  (try
;    (qp.timezone/report-timezone-id-if-supported)
;    (catch Throwable _e nil)))

(defn- get-report-timezone-id-safely
  []
  (try
    (let [tz (qp.timezone/report-timezone-id-if-supported)]
      ;; 确保返回的时区格式是Impala支持的
      (cond
        (re-find #"^UTC[+-]\d+" tz) tz
        (= "GMT-12:00" tz) "UTC-12"
        (= "GMT-11:00" tz) "UTC-11"
        (= "GMT-10:00" tz) "UTC-10"
        (= "GMT-09:00" tz) "UTC-9"
        (= "GMT-08:00" tz) "UTC-8"
        (= "GMT-07:00" tz) "UTC-7"
        (= "GMT-06:00" tz) "UTC-6"
        (= "GMT-05:00" tz) "UTC-5"
        (= "GMT-04:00" tz) "UTC-4"
        (= "GMT-03:00" tz) "UTC-3"
        (= "GMT-02:00" tz) "UTC-2"
        (= "GMT-01:00" tz) "UTC-1"
        (= "GMT+00:00" tz) "UTC"
        (= "GMT+01:00" tz) "UTC+1"
        (= "GMT+02:00" tz) "UTC+2"
        (= "GMT+03:00" tz) "UTC+3"
        (= "GMT+04:00" tz) "UTC+4"
        (= "GMT+05:00" tz) "UTC+5"
        (= "GMT+06:00" tz) "UTC+6"
        (= "GMT+07:00" tz) "UTC+7"
        (= "GMT+08:00" tz) "UTC+8"
        (= "GMT+09:00" tz) "UTC+9"
        (= "GMT+10:00" tz) "UTC+10"
        (= "GMT+11:00" tz) "UTC+11"
        (= "GMT+12:00" tz) "UTC+12"
        :else (get gmt-timezone-mapping tz tz)))
    (catch Throwable e
      (log/warn "Failed to get report timezone:" (.getMessage e))
      nil)))

;;; ------------------------------------------------------------------------------------
;;; 日期提取函数
;;; ------------------------------------------------------------------------------------

;(defn- date-extract
;  [impala-fn expr db-type]
;  (-> [impala-fn expr]
;      (h2x/with-database-type-info db-type)))
(defn- impala-date-extract
  [unit expr]
  (let [impala-unit (case unit
                      :day-of-week :DAYOFWEEK
                      :day-of-month :DAY
                      :day-of-year :DAYOFYEAR
                      :month-of-year :MONTH
                      :quarter-of-year :QUARTER
                      :year-of-era :YEAR
                      :hour-of-day :HOUR
                      :minute-of-hour :MINUTE
                      :second-of-minute :SECOND
                      :week-of-year-iso :WEEKOFYEAR
                      unit)]
    [:'EXTRACT [:'timestamp expr] impala-unit]))

(defmethod sql.qp/date [:impala :day-of-week]
  [_ _ expr]
  (-> (impala-date-extract :DAYOFWEEK expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :day-of-month]
  [_ _ expr]
  (-> (impala-date-extract :DAY expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :day-of-year]
  [_ _ expr]
  (-> (impala-date-extract :DAYOFYEAR expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :month-of-year]
  [_ _ expr]
  (-> (impala-date-extract :MONTH expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :minute-of-hour]
  [_ _ expr]
  (-> (impala-date-extract :MINUTE expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :hour-of-day]
  [_ _ expr]
  (-> (impala-date-extract :HOUR expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :week-of-year-iso]
  [_ _ expr]
  (-> (impala-date-extract :WEEKOFYEAR expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :quarter-of-year]
  [_ _ expr]
  (-> (impala-date-extract :QUARTER expr)
      (h2x/with-database-type-info "int")))

(defmethod sql.qp/date [:impala :year-of-era]
  [_ _ expr]
  (-> (impala-date-extract :YEAR expr)
      (h2x/with-database-type-info "int")))

;; 检查字段的`:display-as`设置
(defmethod sql.qp/date [:impala :default]
  [driver _ expr]
  (let [report-tz (get-report-timezone-id-safely)]
    (if report-tz
      ;; 确保返回完整的timestamp类型
      (h2x/with-database-type-info
        [:'from_utc_timestamp expr report-tz]
        "timestamp")  ;; 明确指定返回类型
      expr)))
;;; ------------------------------------------------------------------------------------
;;; 日期截断函数（统一使用impala-date-trunc实现）
;;; ------------------------------------------------------------------------------------

(defn- impala-date-trunc
  [unit expr]
  (case unit
    :minute  [:'TRUNC [:'timestamp expr] "MINUTE"]
    :hour    [:'TRUNC [:'timestamp expr] "HOUR"]
    :day     [:'TO_DATE expr]  ;; Impala中TO_DATE比TRUNC更高效
    :week    [:'DATE_TRUNC "WEEK" [:'timestamp expr]]
    :month   [:'TRUNC [:'timestamp expr] "MONTH"]
    :quarter [:'TRUNC [:'timestamp expr] "QUARTER"]
    :year    [:'TRUNC [:'timestamp expr] "YEAR"]
    ;; 默认情况
    [:'TRUNC [:'timestamp expr] (str/upper-case (name unit))]))

;; 统一调用impala-date-trunc
(defmethod sql.qp/date [:impala :minute]
  [_ _ expr]
  (impala-date-trunc :minute expr))

(defmethod sql.qp/date [:impala :hour]
  [_ _ expr]
  (impala-date-trunc :hour expr))

(defmethod sql.qp/date [:impala :day]
  [_ _ expr]
  (impala-date-trunc :day expr))

(defmethod sql.qp/date [:impala :week]
  [driver _ expr]
  (impala-date-trunc :week expr))

(defmethod sql.qp/date [:impala :month]
  [_ _ expr]
  (impala-date-trunc :month expr))

(defmethod sql.qp/date [:impala :quarter]
  [_ _ expr]
  (impala-date-trunc :quarter expr))

(defmethod sql.qp/date [:impala :year]
  [_ _ expr]
  (impala-date-trunc :year expr))

;;; ------------------------------------------------------------------------------------
;;; Unix时间戳转换函数
;;; ------------------------------------------------------------------------------------

(defmethod sql.qp/unix-timestamp->honeysql [:impala :seconds]
  [_ _ expr]
  [:'from_unixtime expr])

(defmethod sql.qp/unix-timestamp->honeysql [:impala :milliseconds]
  [_ _ expr]
  [:'from_unixtime (h2x// expr 1000)])

(defmethod sql.qp/unix-timestamp->honeysql [:impala :microseconds]
  [_ _ expr]
  [:'from_unixtime (h2x// expr 1000000)])

;(defmethod sql-jdbc.execute/read-column-thunk [:impala java.sql.Types/TIMESTAMP]
;  [& args]
;  (let [rs (first (filter #(instance? java.sql.ResultSet %) args))
;        i (some #(when (integer? %) %) args)]
;    (fn []
;      (try
;        (if-let [s (.getString rs i)]  ; 直接获取字符串形式
;          (java.sql.Timestamp/valueOf s)
;          nil)
;        (catch Exception e nil)))))
;; 确保读取时为timestamp类型
(defmethod sql-jdbc.execute/read-column-thunk [:impala java.sql.Types/TIMESTAMP]
  [& args]
  (let [rs (first (filter #(instance? java.sql.ResultSet %) args))
        i (some #(when (integer? %) %) args)]
    (fn []
      (try
        (if-let [s (.getString rs i)]
          ;; 明确创建带时间的Timestamp
          (java.sql.Timestamp/valueOf
            (if (re-find #"^\d{4}-\d{2}-\d{2}$" s)
              (str s " 00:00:00")  ;; 补全时间部分
              s))
          nil)
        (catch Exception e nil)))))
;;; ------------------------------------------------------------------------------------
;;; 时区转换函数（全面支持所有时区）
;;; ------------------------------------------------------------------------------------
(defmethod sql.qp/->honeysql [:impala :convert-timezone]
  [driver [_ arg target-timezone source-timezone]]
  (let [expr (sql.qp/->honeysql driver (cond-> arg (string? arg) u.date/parse))
        ;; 标准化时区名称
        normalize-tz (fn [tz]
                       (cond
                         ;; 处理GMT±HH:MM格式
                         (re-find #"^GMT[+-]\d{1,2}:?\d{0,2}" tz)
                         (let [[_ sign hours minutes] (re-find #"^GMT([+-])(\d{1,2}):?(\d{0,2})?" tz)
                               hours (Integer/parseInt hours)
                               minutes (if (empty? minutes) 0 (Integer/parseInt minutes))
                               offset-hours (+ hours (/ minutes 60.0))]
                           (get gmt-timezone-mapping tz
                                (format "UTC%s%d" sign (int offset-hours))))

                         ;; 处理IANA时区（如Asia/Shanghai）
                         (re-find #"^[A-Za-z]+/[A-Za-z_]+" tz) tz

                         ;; 处理UTC±HH格式
                         (re-find #"^UTC[+-]\d{1,2}" tz) tz

                         :else
                         (or (get gmt-timezone-mapping tz)
                             (throw (ex-info (format "Unsupported timezone format: %s" tz)
                                             {:timezone tz :valid-formats ["GMT±HH:MM" "UTC±HH" "Region/City"]})))))]

    [:'FROM_UTC_TIMESTAMP
     [:'TO_UTC_TIMESTAMP expr (normalize-tz source-timezone)]
     (normalize-tz target-timezone)]))
;;; ------------------------------------------------------------------------------------
;;; 日期时间加操作
;;; ------------------------------------------------------------------------------------
(defmethod sql.qp/add-interval-honeysql-form :impala
  [_ hsql-form amount unit]
  (case unit
    :second  [:'date_add hsql-form [:'interval amount :second]]
    :minute  [:'date_add hsql-form [:'interval amount :minute]]
    :hour    [:'date_add hsql-form [:'interval amount :hour]]
    :day     [:'date_add hsql-form [:'interval amount :day]]
    :week    [:'date_add hsql-form [:'interval (* amount 7) :day]]
    :month   [:'date_add hsql-form [:'interval amount :month]]
    :quarter [:'date_add hsql-form [:'interval (* amount 3) :month]]
    :year    [:'date_add hsql-form [:'interval amount :year]]))

;;; ------------------------------------------------------------------------------------
;;; 日期差计算（使用Impala原生函数）
;;; ------------------------------------------------------------------------------------

(defmethod sql.qp/datetime-diff [:impala :year]
  [_driver _unit x y]
  [:'FLOOR [:/ [:'DATEDIFF y x] 365.2425]])

(defmethod sql.qp/datetime-diff [:impala :quarter]
  [_driver _unit x y]
  [:'FLOOR [:/ [:'DATEDIFF y x] 91.3106]])

(defmethod sql.qp/datetime-diff [:impala :month]
  [_driver _unit x y]
  [:'MONTHS_BETWEEN y x])

(defmethod sql.qp/datetime-diff [:impala :week]
  [_driver _unit x y]
  [:'FLOOR [:/ [:'DATEDIFF y x] 7]])

(defmethod sql.qp/datetime-diff [:impala :day]
  [_driver _unit x y]
  [:'DATEDIFF y x])

(defmethod sql.qp/datetime-diff [:impala :hour]
  [_driver _unit x y]
  [:'FLOOR [:/ [:'TIMESTAMPDIFF :SECOND y x] 3600]])

(defmethod sql.qp/datetime-diff [:impala :minute]
  [_driver _unit x y]
  [:'FLOOR [:/ [:'TIMESTAMPDIFF :SECOND y x] 60]])

(defmethod sql.qp/datetime-diff [:impala :second]
  [_driver _unit x y]
  [:'TIMESTAMPDIFF :SECOND y x])

;;; ------------------------------------------------------------------------------------
;;; Unix时间戳转换（使用Impala的FROM_UNIXTIME）
;;; ------------------------------------------------------------------------------------

(defmethod sql.qp/unix-timestamp->honeysql [:impala :seconds]
  [_ _ expr]
  [:'FROM_UNIXTIME expr])

(defmethod sql.qp/unix-timestamp->honeysql [:impala :milliseconds]
  [_ _ expr]
  [:'FROM_UNIXTIME (h2x// expr 1000)])

(defmethod sql.qp/unix-timestamp->honeysql [:impala :microseconds]
  [_ _ expr]
  [:'FROM_UNIXTIME (h2x// expr 1000000)])
;;; ------------------------------------------------------------------------------------
;;; 当前时间（使用Impala的NOW()）
;;; ------------------------------------------------------------------------------------
(defmethod sql.qp/current-datetime-honeysql-form :impala
  [_]
  [:'NOW])  ;; 或者使用 [:'CURRENT_TIMESTAMP]
;;; ------------------------------------------------------------------------------------
;;; 聚合函数
;;; ------------------------------------------------------------------------------------
;; 标准差
(defmethod sql.qp/->honeysql [:impala :stddev]
  [driver [_ field]]
  [:'stddev (sql.qp/->honeysql driver field)])

;; 方差
(defmethod sql.qp/->honeysql [:impala :var]
  [driver [_ field]]
  [:'variance (sql.qp/->honeysql driver field)])

;;; ------------------------------------------------------------------------------------
;;; 窗口函数
;;; ------------------------------------------------------------------------------------

;; 累积和
(defmethod sql.qp/->honeysql [:impala :cum-sum]
  [driver [_ field]]
  [:'sum (sql.qp/->honeysql driver field) :over []])

;; 累积计数
(defmethod sql.qp/->honeysql [:impala :cum-count]
  [driver [_ field]]
  [:'count (sql.qp/->honeysql driver field) :over []])

;;; ------------------------------------------------------------------------------------
;;; 字符串函数
;;; ------------------------------------------------------------------------------------

;; 字符串连接
(defmethod sql.qp/->honeysql [:impala :concat]
  [driver [_ & args]]
  (into [:'concat] (map (partial sql.qp/->honeysql driver) args)))

;; 子字符串
(defmethod sql.qp/->honeysql [:impala :substring]
  [driver [_ arg start length]]
  (if length
    [:'substr (sql.qp/->honeysql driver arg) start length]
    [:'substr (sql.qp/->honeysql driver arg) start]))

;; 字符串替换
(defmethod sql.qp/->honeysql [:impala :replace]
  [driver [_ arg pattern replacement]]
  [:'replace (sql.qp/->honeysql driver arg) pattern replacement])

;; 正则表达式匹配
(defmethod sql.qp/->honeysql [:impala :regex-match-first]
  [driver [_ arg pattern]]
  [:'regexp_extract (sql.qp/->honeysql driver arg) pattern 0])

;;; ------------------------------------------------------------------------------------
;;; 数学函数
;;; ------------------------------------------------------------------------------------

;; 取整
(defmethod sql.qp/->honeysql [:impala :floor]
  [driver [_ arg]]
  [:'floor (sql.qp/->honeysql driver arg)])

;; 向上取整
(defmethod sql.qp/->honeysql [:impala :ceil]
  [driver [_ arg]]
  [:'ceil (sql.qp/->honeysql driver arg)])

;; 四舍五入
(defmethod sql.qp/->honeysql [:impala :round]
  [driver [_ arg precision]]
  (if precision
    [:'round (sql.qp/->honeysql driver arg) precision]
    [:'round (sql.qp/->honeysql driver arg)]))

;; 绝对值
(defmethod sql.qp/->honeysql [:impala :abs]
  [driver [_ arg]]
  [:'abs (sql.qp/->honeysql driver arg)])

;; 对数
(defmethod sql.qp/->honeysql [:impala :log]
  [driver [_ arg]]
  [:'ln (sql.qp/->honeysql driver arg)])

;; 指数
(defmethod sql.qp/->honeysql [:impala :exp]
  [driver [_ arg]]
  [:'exp (sql.qp/->honeysql driver arg)])

;; 平方根
(defmethod sql.qp/->honeysql [:impala :sqrt]
  [driver [_ arg]]
  [:'sqrt (sql.qp/->honeysql driver arg)])

;; 幂
(defmethod sql.qp/->honeysql [:impala :power]
  [driver [_ arg power]]
  [:'pow (sql.qp/->honeysql driver arg) power])