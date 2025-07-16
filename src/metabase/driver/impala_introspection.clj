(ns metabase.driver.impala-introspection
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [metabase.config :as config]
            [metabase.driver :as driver]
            [metabase.driver.ddl.interface :as ddl.i]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql-jdbc.sync.describe-table :as sql-jdbc.describe-table]
            [metabase.util :as u])
  )

(set! *warn-on-reflection* true)

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [[#"array"       :type/Array]
    [#"boolean"     :type/Boolean]
    [#"timestamp"   :type/DateTime]
    [#"decimal"     :type/Decimal]
    [#"float"       :type/Float]
    [#"double"      :type/Float]
    [#"map"         :type/Dictionary]
    [#"string"      :type/Text]
    [#"tuple"       :type/*]
    [#"tinyint"     :type/Integer]
    [#"smallint"    :type/Integer]
    [#"int"         :type/Integer]
    [#"bigint"      :type/BigInteger]
    [#"binary"      :type/Text]
    ]
   ))

;; 规范化数据库类型
(defn- normalize-db-type
  [db-type]
  (cond
    ;; 处理复杂类型
    (str/starts-with? db-type "array<")
    :type/Array

    (str/starts-with? db-type "map<")
    :type/Dictionary

    (str/starts-with? db-type "struct<")
    :type/*

    ;; 处理decimal类型
    (str/starts-with? db-type "decimal")
    :type/Decimal

    ;; 处理varchar类型
    (str/starts-with? db-type "varchar")
    :type/Text

    ;; 处理char类型
    (str/starts-with? db-type "char")
    :type/Text

    ;; 默认情况
    :else (or (database-type->base-type (keyword db-type)) :type/*)))

;; 数据库类型到基础类型的转换
(defmethod sql-jdbc.sync/database-type->base-type :impala
  [_ database-type]
  (let [db-type (if (keyword? database-type)
                  (subs (str database-type) 1)
                  database-type)]
    (normalize-db-type (u/lower-case-en db-type))))

(defmethod sql-jdbc.sync/excluded-schemas :impala[_]
  #{"information_schema" "sys"})

;; 允许的表类型
(def ^:private allowed-table-types
  (into-array String ["TABLE" "VIEW" "EXTERNAL_TABLE"]))

;; 表集合
(defn- tables-set
  [tables]
  (set
    (for [table tables]
      (let [remarks (:remarks table)]
        {:name (:table_name table)
         :schema (:table_schem table)
         :description (when-not (str/blank? remarks) remarks)}))))
;; 从元数据获取表
(defn- get-tables-from-metadata
  [^DatabaseMetaData metadata schema-pattern]
  (.getTables metadata
              nil            ; catalog - unused in the source code there
              schema-pattern
              "%"            ; tablePattern "%" = match all tables
              allowed-table-types))

;; 转换为规格
(defn- ->spec
  [db]
  (if (u/id db)
    (sql-jdbc.conn/db->pooled-connection-spec db) db))

;; 获取所有表
(defn- get-all-tables
  [db]
  (jdbc/with-db-metadata [metadata (->spec db)]
                         (->> (get-tables-from-metadata metadata "%")
                              (jdbc/metadata-result)
                              (vec)
                              (filter #(not (contains? (sql-jdbc.sync/excluded-schemas :impala) (:table_schem %))))
                              (tables-set))))

;; 获取数据库名称
(defn- get-db-name
  [db]
  (or (get-in db [:details :dbname])
      (get-in db [:details :db])))

;; 获取数据库中的表
(defn- get-tables-in-dbs [db-or-dbs]
  (->> (for [db (as-> (or (get-db-name db-or-dbs) "default") dbs
                      (str/split dbs #" ")
                      (remove empty? dbs)
                      (map str/trim dbs))]
         (jdbc/with-db-metadata [metadata (->spec db-or-dbs)]
                                (jdbc/metadata-result
                                  (get-tables-from-metadata metadata db))))
       (apply concat)
       (tables-set)))

;; 描述数据库
(defmethod driver/describe-database :impala
  [_ {{:keys [scan-all-databases]}
      :details :as db}]
  {:tables
   (if (boolean scan-all-databases)
     (get-all-tables db)
     (get-tables-in-dbs db))})

;; 检查字段是否必需
(defn- ^:private is-db-required?
  [field]
  ;; Impala中大多数字段都是可选的
  false)

(defmethod driver/describe-table :impala
  [_ database table]
  (try
    (let [table-metadata (sql-jdbc.sync/describe-table :impala database table)
          filtered-fields (for [field (:fields table-metadata)
                                :let [updated-field (update field :database-required
                                                            (fn [_] (is-db-required? field)))]]
                            updated-field)]
      (merge table-metadata {:fields (set filtered-fields)}))

    (catch java.sql.SQLException e
      (println "同步表 %s 元数据失败，已跳过。错误原因: %s" (:name table) (.getMessage e))
      nil)  ;; 返回nil表示跳过此表

    (catch Exception e
      (println e "同步表 %s 元数据时发生意外错误" (:name table))
      nil)))

;; 获取表主键
(defmethod sql-jdbc.describe-table/get-table-pks :impala
  [_driver ^java.sql.Connection conn db-name-or-nil table]
  ;; Impala通常不使用主键，返回空列表
  [])