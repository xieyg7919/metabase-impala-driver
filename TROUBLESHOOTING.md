# Troubleshooting Guide

This guide helps you resolve common issues when building and using the Metabase Impala Driver.

## Build Issues

### 1. ImpalaJDBC42 Driver Not Found

**Error Message**:
```
Could not find artifact Impala:ImpalaJDBC42:jar:2.6.26.1031
```

**Solution**:
The Cloudera Impala JDBC driver is not available in public Maven repositories and must be installed manually.

**Automatic Installation**:
```bash
# Run the installation script
install-impala-driver.bat
```

**Manual Installation**:
1. Download the Impala JDBC Driver from [Cloudera Downloads](https://www.cloudera.com/downloads/connectors/impala/jdbc/)
2. Extract the downloaded archive
3. Find the `ImpalaJDBC42.jar` file
4. Install it to your local Maven repository:
   ```bash
   mvn install:install-file -Dfile=ImpalaJDBC42.jar -DgroupId=Impala -DartifactId=ImpalaJDBC42 -Dversion=2.6.26.1031 -Dpackaging=jar
   ```

### 2. Maven Not Found

**Error Message**:
```
'mvn' is not recognized as an internal or external command
```

**Solution**:
1. Install Maven from [Apache Maven](https://maven.apache.org/download.cgi)
2. Add Maven's `bin` directory to your system PATH
3. Verify installation: `mvn -version`

### 3. Java Version Issues

**Error Message**:
```
Unsupported class file major version
```

**Solution**:
This driver requires Java 21. Ensure you have the correct version:
```bash
java -version
# Should show version 21.x.x
```

If you have multiple Java versions:
1. Set `JAVA_HOME` to point to Java 21
2. Update your PATH to use Java 21's `bin` directory

### 4. Clojure CLI Issues

**Error Message**:
```
'clj' is not recognized as an internal or external command
```

**Solution**:
1. Install Clojure CLI from [Clojure.org](https://clojure.org/guides/getting_started)
2. Alternatively, use Maven for building:
   ```bash
   build.bat maven
   ```

## Connection Issues

### 1. Connection Timeout

**Error Message**:
```
Connection timed out
```

**Possible Causes & Solutions**:

1. **Incorrect Host/Port**:
   - Verify the Impala server hostname/IP
   - Default port is 21050 for Impala daemon
   - Check if using Impala coordinator port (25000) instead

2. **Firewall Issues**:
   - Ensure port 21050 is open
   - Check network connectivity: `telnet <host> 21050`

3. **Impala Service Down**:
   - Verify Impala daemon is running
   - Check Impala cluster health

### 2. Authentication Failed

**Error Message**:
```
Access denied for user
```

**Solutions**:

1. **Check Credentials**:
   - Verify username and password
   - Some Impala clusters allow anonymous access (leave username/password empty)

2. **Database Access**:
   - Ensure the user has access to the specified database
   - Try connecting without specifying a database first

3. **Kerberos Authentication**:
   - This driver doesn't currently support Kerberos
   - Use a non-Kerberos Impala setup for testing

### 3. SSL Connection Issues

**Error Message**:
```
SSL connection failed
```

**Solutions**:

1. **Verify SSL Support**:
   - Ensure your Impala cluster supports SSL
   - Check if SSL is properly configured on the server

2. **Certificate Issues**:
   - Verify SSL certificates are valid
   - For testing, try without SSL first

3. **Port Configuration**:
   - SSL-enabled Impala might use a different port
   - Check your cluster's SSL port configuration

### 4. Database Not Found

**Error Message**:
```
Unknown database 'database_name'
```

**Solutions**:

1. **Check Database Name**:
   - Verify the database exists: `SHOW DATABASES`
   - Database names are case-sensitive

2. **Use Default Database**:
   - Leave database field empty to connect to default
   - Switch databases after connecting

3. **Permissions**:
   - Ensure user has access to the database
   - Check with Impala administrator

## Runtime Issues

### 1. Driver Not Loaded in Metabase

**Symptoms**:
- Impala option not available in database connections
- Driver JAR in plugins directory but not recognized

**Solutions**:

1. **Check JAR Location**:
   - Ensure JAR is in Metabase's `plugins/` directory
   - Use the correct JAR: `metabase-impala-driver-1.0.0-standalone.jar`

2. **Restart Metabase**:
   - Completely restart Metabase after adding the driver
   - Check Metabase logs for driver loading errors

3. **Verify JAR Integrity**:
   - Ensure the JAR file is not corrupted
   - Rebuild if necessary

4. **Check Metabase Version**:
   - This driver is built for Metabase 0.54.6
   - Compatibility with other versions is not guaranteed

### 2. Query Performance Issues

**Symptoms**:
- Slow query execution
- Timeouts on large datasets

**Solutions**:

1. **Optimize Queries**:
   - Use appropriate WHERE clauses
   - Limit result sets for exploration
   - Use partitioned tables when possible

2. **Impala Configuration**:
   - Increase query timeout settings
   - Optimize Impala cluster resources
   - Use appropriate file formats (Parquet recommended)

3. **Connection Pooling**:
   - Configure appropriate connection pool settings
   - Monitor connection usage

### 3. Data Type Issues

**Symptoms**:
- Incorrect data type detection
- Formatting issues with dates/numbers

**Solutions**:

1. **Check Type Mapping**:
   - Review the driver's type mapping logic
   - Some complex types might not be fully supported

2. **Use Explicit Casting**:
   - Cast columns to appropriate types in Impala
   - Use SQL expressions for complex transformations

## Development Issues

### 1. Test Failures

**Error Message**:
```
Test failures in impala-test
```

**Solutions**:

1. **Missing Test Dependencies**:
   - Ensure all test dependencies are available
   - Run: `clj -M:test` or `mvn test`

2. **No Test Database**:
   - Unit tests should pass without a database
   - Integration tests require a test Impala instance

### 2. REPL Development

**Issues**:
- Cannot connect to REPL
- Driver not loading in development

**Solutions**:

1. **Start REPL with Dependencies**:
   ```bash
   clj -M:dev
   ```

2. **Load Driver Namespace**:
   ```clojure
   (require '[metabase.driver.impala :as impala])
   ```

## Getting Additional Help

### Log Analysis

1. **Metabase Logs**:
   - Check Metabase application logs
   - Look for driver loading and connection errors
   - Enable debug logging if needed

2. **Build Logs**:
   - Review Maven/Clojure build output
   - Check for dependency resolution issues

3. **Impala Logs**:
   - Check Impala daemon logs
   - Review query execution logs
   - Monitor cluster health

### Useful Commands

```bash
# Check Java version
java -version

# Check Maven version
mvn -version

# Check Clojure CLI
clj -version

# Test Impala connection
telnet <impala-host> 21050

# Check if driver is installed
mvn dependency:get -Dartifact=Impala:ImpalaJDBC42:2.6.26.1031

# Clean and rebuild
clj -T:build clean
clj -T:build uber
```

### Community Resources

- [Metabase Documentation](https://www.metabase.com/docs/)
- [Impala Documentation](https://impala.apache.org/docs/build/html/)
- [Cloudera JDBC Documentation](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_jdbc.html)

### Reporting Issues

When reporting issues, please include:

1. **Environment Information**:
   - Operating System
   - Java version
   - Maven version
   - Metabase version
   - Impala version

2. **Error Details**:
   - Complete error messages
   - Stack traces
   - Build logs

3. **Steps to Reproduce**:
   - Exact commands used
   - Configuration details
   - Sample data (if applicable)

4. **Expected vs Actual Behavior**:
   - What you expected to happen
   - What actually happened