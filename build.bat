@echo off
REM Build script for Metabase Impala Driver
REM Supports both Clojure CLI and Maven builds

echo ========================================
echo Metabase Impala Driver Build Script
echo ========================================
echo.

REM Check if Maven is installed
mvn -version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Maven is not installed or not in PATH
    echo Please install Maven and add it to your PATH
    echo Download from: https://maven.apache.org/download.cgi
    pause
    exit /b 1
)

echo Maven found. Checking dependencies...
echo.

REM Check if Impala JDBC42 driver is installed
echo Checking for Impala JDBC42 driver...
mvn dependency:get -Dartifact=Impala:ImpalaJDBC42:2.6.26.1031 -q >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Impala JDBC42 driver not found in local Maven repository
    echo.
    echo Please run the installation script first:
    echo   install-impala-driver.bat
    echo.
    echo Or install manually:
    echo   1. Download ImpalaJDBC42.jar from Cloudera
    echo   2. Run: mvn install:install-file -Dfile=ImpalaJDBC42.jar -DgroupId=Impala -DartifactId=ImpalaJDBC42 -Dversion=2.6.26.1031 -Dpackaging=jar
    echo.
    pause
    exit /b 1
)

echo Impala JDBC42 driver found.
echo.

REM Determine build method
set BUILD_METHOD=%1
if "%BUILD_METHOD%"=="" set BUILD_METHOD=clj

if /i "%BUILD_METHOD%"=="maven" goto :maven_build
if /i "%BUILD_METHOD%"=="mvn" goto :maven_build
if /i "%BUILD_METHOD%"=="clj" goto :clj_build
if /i "%BUILD_METHOD%"=="clojure" goto :clj_build

echo Invalid build method: %BUILD_METHOD%
echo Usage: build.bat [clj|maven]
echo   clj    - Use Clojure CLI (default)
echo   maven  - Use Maven
pause
exit /b 1

:clj_build
echo Building with Clojure CLI...
echo.

REM Check if Clojure CLI is installed
clj -version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Clojure CLI is not installed or not in PATH
    echo Please install Clojure CLI
    echo Download from: https://clojure.org/guides/getting_started
    echo.
    echo Falling back to Maven build...
    goto :maven_build
)

echo Cleaning previous build...
clj -T:build clean

echo Building uberjar...
clj -T:build uber

if %errorlevel% equ 0 (
    echo.
    echo SUCCESS: Build completed successfully!
    echo.
    echo Output file: target\metabase-impala-driver-1.0.0-standalone.jar
    echo.
    echo To use this driver with Metabase:
    echo 1. Copy the JAR file to your Metabase plugins directory
    echo 2. Restart Metabase
    echo 3. The Impala driver will be available in the database connection options
    echo.
) else (
    echo.
    echo ERROR: Build failed
    echo Please check the error messages above
    echo.
)
goto :end

:maven_build
echo Building with Maven...
echo.

echo Cleaning previous build...
mvn clean

echo Compiling and packaging...
mvn package

if %errorlevel% equ 0 (
    echo.
    echo SUCCESS: Build completed successfully!
    echo.
    echo Output file: target\metabase-impala-driver-1.0.0.jar
    echo.
    echo To use this driver with Metabase:
    echo 1. Copy the JAR file to your Metabase plugins directory
    echo 2. Restart Metabase
    echo 3. The Impala driver will be available in the database connection options
    echo.
) else (
    echo.
    echo ERROR: Build failed
    echo Please check the error messages above
    echo.
)
goto :end

:end
echo Build process completed.
pause