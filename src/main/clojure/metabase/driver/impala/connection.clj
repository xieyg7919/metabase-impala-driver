(ns metabase.driver.impala.connection
  "Connection management for Impala driver."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase.driver.impala.query :as impala.query])
  (:import [java.sql DriverManager SQLException]
           [java.util Properties]))

;; Load Impala JDBC driver
(try
  (Class/forName "com.cloudera.impala.jdbc.Driver")
  (log/info "Impala JDBC driver loaded successfully")
  (catch ClassNotFoundException e
    (log/warn "Impala JDBC driver not found. Please ensure the driver JAR is in the classpath.")))

(defn create-connection-spec
  "Create a JDBC connection specification for Impala."
  [{:keys [host port database user password ssl additional-options] :as details}]
  (let [jdbc-url (impala.query/build-connection-url details)
        properties (impala.query/prepare-connection-properties details)]
    {:connection-uri jdbc-url
     :classname "com.cloudera.impala.jdbc.Driver"
     :subprotocol "impala"
     :subname (str "//" host ":" port "/" database)
     :user user
     :password password
     :properties properties}))

(defn test-connection
  "Test connection to Impala database."
  [connection-spec]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (let [result (jdbc/query conn ["SELECT 1 as test_connection"])]
        (if (= 1 (-> result first :test_connection))
          {:status :success
           :message "Connection successful"}
          {:status :error
           :message "Connection test failed: unexpected result"})))
    (catch SQLException e
      (let [error-msg (impala.query/format-error-message (.getMessage e))]
        (log/error e "Impala connection test failed")
        {:status :error
         :message (str "Connection failed: " error-msg)
         :error-code (.getErrorCode e)
         :sql-state (.getSQLState e)}))
    (catch Exception e
      (log/error e "Unexpected error during Impala connection test")
      {:status :error
       :message (str "Unexpected error: " (.getMessage e))})))

(defn get-database-metadata
  "Get metadata information from Impala database."
  [connection-spec]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (let [metadata (.getMetaData conn)]
        {:database-product-name (.getDatabaseProductName metadata)
         :database-product-version (.getDatabaseProductVersion metadata)
         :driver-name (.getDriverName metadata)
         :driver-version (.getDriverVersion metadata)
         :jdbc-major-version (.getJDBCMajorVersion metadata)
         :jdbc-minor-version (.getJDBCMinorVersion metadata)
         :supports-transactions (.supportsTransactions metadata)
         :supports-batch-updates (.supportsBatchUpdates metadata)}))
    (catch Exception e
      (log/error e "Failed to get database metadata")
      {})))

(defn list-databases
  "List available databases in Impala."
  [connection-spec]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (let [result (jdbc/query conn ["SHOW DATABASES"])]
        (map :name result)))
    (catch Exception e
      (log/error e "Failed to list databases")
      [])))

(defn list-tables
  "List tables in the specified database."
  [connection-spec database]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (let [query (if database
                    (format "SHOW TABLES IN %s" database)
                    "SHOW TABLES")
            result (jdbc/query conn [query])]
        (map :name result)))
    (catch Exception e
      (log/error e "Failed to list tables")
      [])))

(defn describe-table
  "Get table schema information."
  [connection-spec table-name]
  (try
    (with-open [conn (jdbc/get-connection connection-spec)]
      (let [result (jdbc/query conn [(str "DESCRIBE " table-name)])]
        (map (fn [row]
               {:column-name (:name row)
                :data-type (:type row)
                :comment (:comment row)})
             result)))
    (catch Exception e
      (log/error e "Failed to describe table" table-name)
      [])))

(defn execute-query
  "Execute a query against Impala database."
  [connection-spec sql-query & {:keys [limit timeout] :as options}]
  (let [optimized-query (impala.query/optimize-query sql-query)
        final-query (if limit
                      (impala.query/add-limit-clause optimized-query limit)
                      optimized-query)
        validation-issues (impala.query/validate-query final-query)]
    
    ;; Log validation issues as warnings
    (doseq [issue validation-issues]
      (log/warn "Query validation issue:" issue))
    
    (try
      (let [start-time (System/currentTimeMillis)]
        (with-open [conn (jdbc/get-connection connection-spec)]
          (when timeout
            (.setQueryTimeout (.createStatement conn) (/ timeout 1000)))
          
          (let [result (jdbc/query conn [final-query])
                execution-time (- (System/currentTimeMillis) start-time)
                row-count (count result)]
            
            (impala.query/log-query-execution final-query execution-time row-count)
            
            {:status :success
             :data result
             :row-count row-count
             :execution-time-ms execution-time
             :validation-issues validation-issues})))
      (catch SQLException e
        (let [error-msg (impala.query/format-error-message (.getMessage e))]
          (log/error e "Query execution failed")
          {:status :error
           :message error-msg
           :error-code (.getErrorCode e)
           :sql-state (.getSQLState e)
           :query final-query}))
      (catch Exception e
        (log/error e "Unexpected error during query execution")
        {:status :error
         :message (.getMessage e)
         :query final-query}))))

(defn close-connection
  "Close database connection if open."
  [connection]
  (when connection
    (try
      (.close connection)
      (log/debug "Connection closed successfully")
      (catch Exception e
        (log/warn e "Error closing connection")))))

(defn create-connection-pool
  "Create a basic connection pool configuration."
  [connection-spec & {:keys [max-connections min-connections]
                      :or {max-connections 10 min-connections 2}}]
  {:connection-spec connection-spec
   :max-connections max-connections
   :min-connections min-connections
   :validation-query "SELECT 1"
   :test-on-borrow true
   :test-while-idle true})