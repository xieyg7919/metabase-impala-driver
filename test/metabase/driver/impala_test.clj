(ns metabase.driver.impala-test
  "Tests for Impala Metabase driver."
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [metabase.driver.impala :as impala]
            [metabase.driver.impala.connection :as impala.conn]
            [metabase.driver.impala.query :as impala.query]))

;; Test connection details
(def test-connection-details
  {:host "localhost"
   :port 21050
   :database "default"
   :user "test-user"
   :password "test-password"})

(def invalid-connection-details
  {:host "localhost"
   ;; Missing port and database
   :user "test-user"})

(deftest test-driver-info
  (testing "Driver info should contain required fields"
    (let [info (impala/get-driver-info)]
      (is (contains? info :name))
      (is (contains? info :display-name))
      (is (contains? info :description))
      (is (contains? info :version))
      (is (contains? info :features))
      (is (string? (:name info)))
      (is (string? (:display-name info)))
      (is (map? (:features info))))))

(deftest test-connection-validation
  (testing "Valid connection details should pass validation"
    (let [result (impala/validate-connection-details test-connection-details)]
      (is (:valid result))
      (is (nil? (:errors result)))))
  
  (testing "Invalid connection details should fail validation"
    (let [result (impala/validate-connection-details invalid-connection-details)]
      (is (not (:valid result)))
      (is (seq (:errors result)))
      (is (some #(re-find #"port" %) (:errors result)))
      (is (some #(re-find #"database" %) (:errors result))))))

(deftest test-connection-spec-creation
  (testing "Connection spec should be created correctly"
    (let [spec (impala/connection-details test-connection-details)]
      (is (contains? spec :connection-uri))
      (is (contains? spec :classname))
      (is (= "com.cloudera.impala.jdbc.Driver" (:classname spec)))
      (is (= "test-user" (:user spec)))
      (is (= "test-password" (:password spec))))))

(deftest test-url-building
  (testing "JDBC URL should be built correctly"
    (let [url (impala.query/build-connection-url test-connection-details)]
      (is (string? url))
      (is (.startsWith url "jdbc:impala://"))
      (is (.contains url "localhost:21050"))
      (is (.contains url "/default"))))
  
  (testing "JDBC URL with SSL should include SSL parameter"
    (let [ssl-details (assoc test-connection-details :ssl true)
          url (impala.query/build-connection-url ssl-details)]
      (is (.contains url "SSL=1"))))
  
  (testing "JDBC URL with additional options should include them"
    (let [details-with-options (assoc test-connection-details :additional-options "option1=value1;option2=value2")
          url (impala.query/build-connection-url details-with-options)]
      (is (.contains url "option1=value1"))
      (is (.contains url "option2=value2")))))

(deftest test-type-mapping
  (testing "Basic type mappings should work correctly"
    (is (= :type/Boolean (impala/database-type->base-type "BOOLEAN")))
    (is (= :type/Integer (impala/database-type->base-type "INT")))
    (is (= :type/BigInteger (impala/database-type->base-type "BIGINT")))
    (is (= :type/Float (impala/database-type->base-type "DOUBLE")))
    (is (= :type/Text (impala/database-type->base-type "STRING")))
    (is (= :type/DateTime (impala/database-type->base-type "TIMESTAMP")))
    (is (= :type/Date (impala/database-type->base-type "DATE"))))
  
  (testing "Type mappings should handle precision/scale info"
    (is (= :type/Text (impala/database-type->base-type "VARCHAR(255)")))
    (is (= :type/Decimal (impala/database-type->base-type "DECIMAL(10,2)"))))
  
  (testing "Type mappings should handle complex types"
    (is (= :type/* (impala/database-type->base-type "ARRAY<STRING>")))
    (is (= :type/* (impala/database-type->base-type "MAP<STRING,INT>"))))
  
  (testing "Unknown types should default to :type/*"
    (is (= :type/* (impala/database-type->base-type "UNKNOWN_TYPE")))))

(deftest test-query-optimization
  (testing "Query optimization should add hints"
    (let [original-query "SELECT * FROM table1"
          optimized (impala.query/optimize-query original-query)]
      (is (.contains optimized "STRAIGHT_JOIN"))))
  
  (testing "Query limit should be added when missing"
    (let [query "SELECT * FROM table1"
          limited (impala.query/add-limit-clause query 1000)]
      (is (.contains limited "LIMIT 1000"))))
  
  (testing "Query limit should not be added when already present"
    (let [query "SELECT * FROM table1 LIMIT 500"
          result (impala.query/add-limit-clause query 1000)]
      (is (= query result)))))

(deftest test-query-validation
  (testing "Query validation should detect potential issues"
    (let [issues (impala.query/validate-query "SELECT * FROM large_table")]
      (is (seq issues))
      (is (some #(re-find #"SELECT \*" %) issues))))
  
  (testing "Query validation should detect unsupported features"
    (let [issues (impala.query/validate-query "WITH RECURSIVE cte AS (...) SELECT * FROM cte")]
      (is (seq issues))
      (is (some #(re-find #"Recursive" %) issues)))))

(deftest test-sql-functions
  (testing "SQL function mapping should work correctly"
    (is (= "date_trunc('day', %s)" (impala/get-impala-function :date-trunc :day)))
    (is (= "extract(hour from %s)" (impala/get-impala-function :extract :hour)))
    (is (= "length(%s)" (impala/get-impala-function :string-functions :length))))
  
  (testing "SQL function formatting should work"
    (let [formatted (impala/format-sql-function "length(%s)" "column_name")]
      (is (= "length(column_name)" formatted)))))

(deftest test-error-formatting
  (testing "Error message formatting should clean up verbose messages"
    (let [verbose-error "[Cloudera][ImpalaJDBCDriver][Impala] Error: some error message"
          cleaned (impala.query/format-error-message verbose-error)]
      (is (not (.contains cleaned "[Cloudera]")))
      (is (not (.contains cleaned "[ImpalaJDBCDriver]")))
      (is (.contains cleaned "some error message")))))

(deftest test-table-info-extraction
  (testing "Table info extraction should work for simple queries"
    (let [query "SELECT * FROM users u JOIN orders o ON u.id = o.user_id"
          info (impala.query/extract-table-info query)]
      (is (= "users" (:main-table info)))
      (is (= ["orders"] (:joined-tables info)))))
  
  (testing "Table info extraction should handle complex queries"
    (let [query "SELECT * FROM schema1.table1 t1 LEFT JOIN schema2.table2 t2 ON t1.id = t2.ref_id"
          info (impala.query/extract-table-info query)]
      (is (= "schema1.table1" (:main-table info)))
      (is (= ["schema2.table2"] (:joined-tables info))))))

;; Integration tests (these would require an actual Impala instance)
(deftest ^:integration test-connection-integration
  (testing "Connection test should work with valid credentials"
    ;; This test would require actual Impala instance
    ;; (let [result (impala/test-connection (impala/connection-details test-connection-details))]
    ;;   (is (= :success (:status result))))
    (is true "Integration test placeholder")))

(deftest ^:integration test-query-execution-integration
  (testing "Query execution should work with valid connection"
    ;; This test would require actual Impala instance
    ;; (let [connection-spec (impala/connection-details test-connection-details)
    ;;       result (impala/execute-native-query connection-spec "SELECT 1 as test")]
    ;;   (is (= :success (:status result)))
    ;;   (is (= 1 (-> result :data first :test))))
    (is true "Integration test placeholder")))

;; Test runner helper
(defn run-unit-tests
  "Run all unit tests except integration tests."
  []
  (clojure.test/run-tests #'test-driver-info
                          #'test-connection-validation
                          #'test-connection-spec-creation
                          #'test-url-building
                          #'test-type-mapping
                          #'test-query-optimization
                          #'test-query-validation
                          #'test-sql-functions
                          #'test-error-formatting
                          #'test-table-info-extraction))

(defn run-integration-tests
  "Run integration tests (requires Impala instance)."
  []
  (clojure.test/run-tests #'test-connection-integration
                          #'test-query-execution-integration))