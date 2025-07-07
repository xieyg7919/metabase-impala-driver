(ns metabase.driver.impala-test
  "Tests for the Impala driver."
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.impala :as impala]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.test :as mt]))

(deftest driver-registration-test
  (testing "Impala driver is properly registered"
    (is (contains? (set (driver/available-drivers)) :impala))))

(deftest display-name-test
  (testing "Driver display name"
    (is (= "Impala" (driver/display-name :impala)))))

(deftest connection-details->spec-test
  (testing "Connection details conversion"
    (let [details {:host "localhost"
                   :port 21050
                   :db "test_db"
                   :user "test_user"
                   :password "test_pass"
                   :ssl false}
          spec (sql-jdbc.conn/connection-details->spec :impala details)]
      (is (= "com.cloudera.impala.jdbc42.Driver" (:classname spec)))
      (is (= "impala" (:subprotocol spec)))
      (is (str/includes? (:subname spec) "localhost:21050/test_db"))
      (is (= "test_user" (:user spec)))
      (is (= "test_pass" (:password spec))))))

(deftest connection-details->spec-ssl-test
  (testing "Connection details with SSL"
    (let [details {:host "secure-host"
                   :port 21050
                   :db "secure_db"
                   :ssl true}
          spec (sql-jdbc.conn/connection-details->spec :impala details)]
      (is (str/includes? (:subname spec) "?SSL=1")))))

(deftest supported-features-test
  (testing "Driver feature support"
    ;; Supported features
    (is (driver/supports? :impala :basic-aggregations))
    (is (driver/supports? :impala :standard-deviation-aggregations))
    (is (driver/supports? :impala :expressions))
    (is (driver/supports? :impala :expression-aggregations))
    (is (driver/supports? :impala :native-parameters))
    (is (driver/supports? :impala :nested-queries))
    (is (driver/supports? :impala :binning))
    (is (driver/supports? :impala :case-sensitivity-string-filter))
    (is (driver/supports? :impala :regex))
    (is (driver/supports? :impala :percentile-aggregations))
    (is (driver/supports? :impala :advanced-math-expressions))
    (is (driver/supports? :impala :datetime-diff))
    (is (driver/supports? :impala :now))
    (is (driver/supports? :impala :schemas))
    
    ;; Unsupported features
    (is (not (driver/supports? :impala :foreign-keys)))
    (is (not (driver/supports? :impala :set-timezone)))
    (is (not (driver/supports? :impala :connection-impersonation)))
    (is (not (driver/supports? :impala :uploads)))
    (is (not (driver/supports? :impala :convert-timezone)))
    (is (not (driver/supports? :impala :test/jvm-timezone-setting)))))

(deftest database-type->base-type-test
  (testing "Database type mapping"
    (are [db-type expected] (= expected (#'impala/database-type->base-type :impala db-type))
      "boolean"    :type/Boolean
      "tinyint"    :type/Integer
      "smallint"   :type/Integer
      "int"        :type/Integer
      "bigint"     :type/BigInteger
      "float"      :type/Float
      "double"     :type/Float
      "decimal"    :type/Decimal
      "string"     :type/Text
      "varchar"    :type/Text
      "char"       :type/Text
      "timestamp"  :type/DateTime
      "date"       :type/Date
      "binary"     :type/*
      "array<int>" :type/Array
      "map<string,int>" :type/Dictionary
      "struct<name:string>" :type/Structured
      "unknown"    :type/*)))

(deftest excluded-schemas-test
  (testing "Excluded schemas"
    (let [excluded (#'impala/excluded-schemas :impala)]
      (is (contains? excluded "information_schema"))
      (is (contains? excluded "sys"))
      (is (contains? excluded "_impala_builtins")))))

(deftest db-start-of-week-test
  (testing "Database start of week"
    (is (= :sunday (driver/db-start-of-week :impala)))))

(deftest db-default-timezone-test
  (testing "Database default timezone"
    (is (= "UTC" (driver/db-default-timezone :impala nil)))))

(deftest connection-error-messages-test
  (testing "Connection error message humanization"
    (are [error-msg expected] (str/includes? 
                               (driver/humanize-connection-error-message :impala error-msg)
                               expected)
      "Communications link failure" "check-host-and-port"
      "Access denied" "username-or-password"
      "Unknown database" "database-name"
      "Some other error" "Some other error")))

;; Integration tests would go here if we had a test Impala instance
;; For now, we focus on unit tests that don't require a live connection

(comment
  ;; Example integration test structure (requires test database)
  (deftest ^:integration connection-test
    (testing "Can connect to test Impala instance"
      (mt/test-driver :impala
        (is (driver/can-connect? :impala (mt/db-connection-details)))))))