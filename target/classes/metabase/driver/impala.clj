(ns metabase.driver.impala
  "Metabase driver for Apache Impala."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [metabase.driver.impala.connection :as impala.conn]
            [metabase.driver.impala.query :as impala.query]))

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
               base-url (str "//" host ":" port "/" dbname)
               url (cond-> base-url
                     ssl (str "?ssl=1")
                     (and additional-options (not (clojure.string/blank? additional-options)))
                     (str (if ssl "&" "?") additional-options))]
           (merge
             {:classname "com.cloudera.impala.jdbc.Driver"
              :subprotocol "impala"
              :subname url}
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
           (let [excluded (metabase.driver.sql-jdbc.sync/excluded-schemas driver)
                 schemas (with-open [rs (.getSchemas metadata)]
                           (->> (jdbc/metadata-result rs)
                                (map :table_schem)
                                (remove excluded)
                                (remove nil?)))
                 tables (for [schema schemas
                             table (with-open [rs (.getTables metadata nil schema "%"
                                                              (into-array String ["TABLE" "VIEW" "FOREIGN TABLE" "MATERIALIZED VIEW"]))]
                                     (jdbc/metadata-result rs))]
                          {:name (:table_name table)
                           :schema schema})]
             (log/info "Found" (count tables) "tables in schemas:" schemas)
             (set tables))
           (catch Exception e
             (log/error e "Error in active-tables for Impala")
             #{}))))
    (log/info "active-tables multimethod defined")
    
    ;; Define describe-database multimethod
    (eval
      '(defmethod metabase.driver/describe-database :impala
         [driver database]
         (try
           (with-open [conn (jdbc/get-connection (metabase.driver.sql-jdbc.conn/connection-details->spec driver (:details database)))]
             (let [metadata (.getMetaData conn)
                   tables (metabase.driver.sql-jdbc.sync/active-tables driver metadata)]
               (log/info "Describing database with" (count tables) "tables")
               {:tables tables}))
           (catch Exception e
             (log/error e "Error in describe-database for Impala")
             {:tables #{}}))))
    (log/info "describe-database multimethod defined")
    
    ;; Define describe-table multimethod
    (eval
      '(defmethod metabase.driver/describe-table :impala
         [driver database table]
         (try
           (let [spec (metabase.driver.sql-jdbc.conn/connection-details->spec driver (:details database))
                 table-name (if (:schema table)
                             (str (:schema table) "." (:name table))
                             (:name table))
                 columns (with-open [conn (jdbc/get-connection spec)]
                          (jdbc/query conn [(str "DESCRIBE " table-name)]))
                 fields (set (map-indexed
                             (fn [idx col]
                               {:name (:name col)
                                :database-type (:type col)
                                :base-type (metabase.driver.sql-jdbc.sync/database-type->base-type driver (:type col))
                                :database-position idx})
                             columns))]
             (log/info "Describing table" table-name "with" (count fields) "fields")
             {:name (:name table)
              :schema (:schema table)
              :fields fields})
           (catch Exception e
             (log/error e "Error in describe-table for Impala table:" (:name table))
             {:name (:name table)
              :schema (:schema table)
              :fields #{}}))))
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
(defn connection-details
  "Build connection details for Impala using the connection module."
  [details]
  (impala.conn/create-connection-spec details))

(defn test-connection
  "Test connection to Impala database using the connection module."
  [connection-spec]
  (impala.conn/test-connection connection-spec))

(defn get-database-info
  "Get database information and metadata."
  [connection-spec]
  (merge
    (impala.conn/get-database-metadata connection-spec)
    {:available-databases (impala.conn/list-databases connection-spec)}))

(defn execute-native-query
  "Execute a native SQL query against Impala."
  [connection-spec query & options]
  (apply impala.conn/execute-query connection-spec query options))

(defn can-connect?
  "Test if we can connect to Impala database."
  [details]
  (let [connection-spec (connection-details details)
        result (test-connection connection-spec)]
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
   "UROWID"     :type/Text})

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

(defn format-sql-function
  "Format SQL function with arguments."
  [function-template & args]
  (apply format function-template args))

(defn get-impala-function
  "Get Impala-specific SQL function."
  [category function-name]
  (get-in impala-sql-functions [category function-name]))

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

(defn get-driver-info
  "Get comprehensive driver information."
  []
  {:name (:name driver-config "Impala")
   :display-name (:display-name driver-config "Apache Impala")
   :description (:description driver-config "Driver for Apache Impala databases")
   :version (:version driver-config "1.0.0")
   :features driver-features
   :connection-properties (:connection-properties driver-config)
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
  (log/info "Initializing Impala driver...")
  ;; Register driver and define multimethods
  (register-impala-driver!)
  (log/info "Driver info:" (get-driver-info))
  (log/info "Supported features:" (keys (filter val driver-features)))
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
      (let [connection-spec (connection-details details)
            test-result (test-connection connection-spec)]
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