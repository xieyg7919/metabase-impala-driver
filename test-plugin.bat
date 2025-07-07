@echo off
echo Testing Impala Driver Plugin...
echo.

echo 1. Checking JAR file exists...
if exist "target\impala-driver-1.0.0-standalone.jar" (
    echo ✓ JAR file found
) else (
    echo ✗ JAR file not found
    exit /b 1
)

echo.
echo 2. Checking plugin manifest...
jar tf target\impala-driver-1.0.0-standalone.jar | findstr "metabase-plugin.yaml" >nul
if %errorlevel% equ 0 (
    echo ✓ Plugin manifest found
) else (
    echo ✗ Plugin manifest not found
    exit /b 1
)

echo.
echo 3. Checking driver source files...
jar tf target\impala-driver-1.0.0-standalone.jar | findstr "metabase/driver/impala.clj" >nul
if %errorlevel% equ 0 (
    echo ✓ Driver source files found
) else (
    echo ✗ Driver source files not found
    exit /b 1
)

echo.
echo 4. Checking JDBC driver...
jar tf target\impala-driver-1.0.0-standalone.jar | findstr "com/cloudera/impala" >nul
if %errorlevel% equ 0 (
    echo ✓ Impala JDBC driver found
) else (
    echo ✗ Impala JDBC driver not found
    exit /b 1
)

echo.
echo 5. Extracting and validating plugin manifest...
jar xf target\impala-driver-1.0.0-standalone.jar metabase-plugin.yaml
if exist "metabase-plugin.yaml" (
    echo ✓ Plugin manifest extracted successfully
    echo.
    echo Plugin manifest content:
    type metabase-plugin.yaml
    del metabase-plugin.yaml
) else (
    echo ✗ Failed to extract plugin manifest
    exit /b 1
)

echo.
echo ✓ All tests passed! Plugin appears to be correctly packaged.
echo.
echo Next steps:
echo 1. Copy the JAR file to your Metabase plugins directory
echo 2. Restart Metabase
echo 3. Check Metabase logs for plugin loading messages
echo 4. Look for "Apache Impala" in the database creation dropdown