(ns metabase.driver.impala.query
  "Query processing utilities for Impala driver."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]))

;; Query optimization settings
(def ^:private query-settings
  "Default query settings for Impala."
  {:query-timeout 300000  ; 5 minutes
   :fetch-size 1000
   :max-rows 10000
   :enable-result-cache true
   :enable-query-cache true})

(defn build-connection-url
  "Build JDBC connection URL for Impala."
  [{:keys [host port database ssl additional-options] :as connection-spec}]
  (let [base-url (format "jdbc:impala://%s:%s/%s" host port database)
        ssl-param (when ssl "SSL=1")
        additional (when (and additional-options (not (str/blank? additional-options)))
                    additional-options)
        params (filter some? [ssl-param additional])
        param-string (when (seq params) (str "?" (str/join ";" params)))]
    (str base-url param-string)))

(defn prepare-connection-properties
  "Prepare connection properties for Impala JDBC driver."
  [{:keys [user password] :as connection-spec}]
  (cond-> {}
    user (assoc "user" user)
    password (assoc "password" password)
    true (merge {"ConnTimeout" "30"
                 "SocketTimeout" "60"
                 "LogLevel" "2"
                 "UseNativeQuery" "1"})))

(defn optimize-query
  "Apply Impala-specific query optimizations."
  [sql-query]
  (-> sql-query
      ;; Add query hints for better performance
      (str/replace #"^SELECT" "SELECT /*+ STRAIGHT_JOIN */")
      ;; Ensure proper formatting
      str/trim))

(defn add-limit-clause
  "Add LIMIT clause to query if not present."
  [sql-query limit]
  (if (and limit (not (str/includes? (str/lower-case sql-query) "limit")))
    (str sql-query " LIMIT " limit)
    sql-query))

(defn validate-query
  "Validate SQL query for Impala compatibility."
  [sql-query]
  (let [query-lower (str/lower-case sql-query)
        issues []]
    (cond-> issues
      ;; Check for unsupported features
      (str/includes? query-lower "recursive")
      (conj "Recursive CTEs are not supported in Impala")
      
      (str/includes? query-lower "window")
      (conj "Window functions may have limited support")
      
      ;; Check for potential performance issues
      (and (str/includes? query-lower "select *")
           (not (str/includes? query-lower "limit")))
      (conj "SELECT * without LIMIT may cause performance issues"))))

(defn format-error-message
  "Format Impala error message for better readability."
  [error-message]
  (-> error-message
      (str/replace #"com\.cloudera\.impala\." "")
      (str/replace #"java\.sql\." "")
      (str/replace #"\[Cloudera\]\[ImpalaJDBCDriver\]" "")
      str/trim))

(defn extract-table-info
  "Extract table information from query."
  [sql-query]
  (let [query-lower (str/lower-case sql-query)
        from-match (re-find #"from\s+([\w\.]+)" query-lower)
        join-matches (re-seq #"join\s+([\w\.]+)" query-lower)]
    {:main-table (second from-match)
     :joined-tables (map second join-matches)}))

(defn get-query-settings
  "Get query settings with optional overrides."
  [& {:as overrides}]
  (merge query-settings overrides))

(defn log-query-execution
  "Log query execution details."
  [sql-query execution-time-ms row-count]
  (log/info (format "Impala query executed in %dms, returned %d rows: %s"
                    execution-time-ms
                    row-count
                    (if (> (count sql-query) 100)
                      (str (subs sql-query 0 100) "...")
                      sql-query))))