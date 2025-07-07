# Impala Metabase Driver

A comprehensive Metabase driver for Apache Impala databases, providing full integration with Metabase's analytics platform.

## Features

### Core Functionality
- **Database Connection Management**: Robust connection handling with connection pooling support
- **Query Execution**: Optimized SQL query execution with Impala-specific optimizations
- **Type Mapping**: Comprehensive mapping between Impala and Metabase data types
- **Error Handling**: Enhanced error messages and debugging support
- **Configuration Management**: Flexible configuration through EDN files

### Supported Impala Features
- ✅ Basic aggregations (COUNT, SUM, AVG, MIN, MAX)
- ✅ Standard deviation aggregations
- ✅ Expressions and calculated fields
- ✅ Nested queries and subqueries
- ✅ Data binning
- ✅ Case-sensitive string filtering
- ✅ JOIN operations (LEFT, RIGHT, INNER)
- ✅ Regular expressions
- ✅ Date/time functions
- ✅ String manipulation functions
- ❌ Native parameters (not supported by Impala)
- ❌ FULL JOIN (limited Impala support)
- ❌ Percentile aggregations (limited Impala support)

### Data Types Supported
- **Numeric**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **String**: STRING, VARCHAR, CHAR, TEXT
- **Date/Time**: TIMESTAMP, DATE, TIME
- **Complex**: ARRAY, MAP, STRUCT (basic support)
- **Binary**: BINARY

## Quick Start

### Prerequisites

- **Java 21** or higher
- **Maven 3.6+** for building
- **Clojure CLI** (optional, for Clojure-based builds)
- **Metabase 0.54.6**

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd impala-metabase
   ```

2. **Install the Impala JDBC Driver**:
   ```bash
   # Run the automated installation script
   install-impala-driver.bat
   ```
   
   Or install manually:
   ```bash
   # Download ImpalaJDBC42.jar from Cloudera, then:
   mvn install:install-file -Dfile=ImpalaJDBC42.jar -DgroupId=Impala -DartifactId=ImpalaJDBC42 -Dversion=2.6.26.1031 -Dpackaging=jar
   ```

3. **Build the driver**:
   ```bash
   # Using the build script (recommended)
   build.bat
   
   # Or manually with Clojure CLI
   clj -T:build uber
   
   # Or with Maven
   mvn clean package
   ```

4. **Install in Metabase**:
   - Copy `target/metabase-impala-driver-1.0.0-standalone.jar` to your Metabase `plugins/` directory
   - Restart Metabase
   - The Impala driver will appear in the database connection options

## Configuration

### Connection Parameters

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| **Host** | Impala server hostname or IP | localhost | Yes |
| **Port** | Impala server port | 21050 | No |
| **Database** | Database name to connect to | default | No |
| **Username** | Database username | impala | No |
| **Password** | Database password | (empty) | No |
| **Use SSL** | Enable SSL connection | false | No |

### Example Connection

```
Host: impala-cluster.example.com
Port: 21050
Database: analytics
Username: analyst
Password: ********
Use SSL: true
```

## Development

### Project Structure

```
impala-metabase/
├── src/main/clojure/metabase/driver/
│   └── impala.clj              # Main driver implementation
├── resources/
│   └── metabase-plugin.yaml    # Plugin configuration
├── deps.edn                    # Clojure dependencies
├── pom.xml                     # Maven configuration
├── build.clj                   # Build script
├── build.bat                   # Windows build script
└── install-impala-driver.bat   # Driver installation script
```

### Building from Source

1. **Install dependencies**:
   ```bash
   # Install Impala JDBC driver
   install-impala-driver.bat
   ```

2. **Development build**:
   ```bash
   # With Clojure CLI
   clj -T:build clean
   clj -T:build uber
   
   # With Maven
   mvn clean compile
   mvn package
   ```

3. **Testing**:
   ```bash
   # Run tests (when available)
   clj -M:test
   # or
   mvn test
   ```

### Supported Features

✅ **Supported**:
- Basic aggregations (COUNT, SUM, AVG, MIN, MAX)
- Standard deviation aggregations
- Mathematical expressions
- String operations and regex
- Date/time functions
- Nested queries and subqueries
- Percentile aggregations
- Case sensitivity in string filters
- Binning and grouping

❌ **Not Supported**:
- Foreign key relationships
- Timezone conversion
- Connection impersonation
- Data uploads

## Troubleshooting

### Common Issues

1. **"Driver not found" error**:
   - Ensure the ImpalaJDBC42 driver is installed in your local Maven repository
   - Run `install-impala-driver.bat` to install it automatically

2. **Connection timeout**:
   - Verify the Impala server is running and accessible
   - Check firewall settings for port 21050 (or your custom port)
   - Ensure the hostname/IP is correct

3. **Authentication failed**:
   - Verify username and password
   - Check if the user has access to the specified database
   - Some Impala clusters may require Kerberos authentication (not currently supported)

4. **SSL connection issues**:
   - Ensure your Impala cluster supports SSL
   - Verify SSL certificates are properly configured

### Getting Help

- Check the [Metabase documentation](https://www.metabase.com/docs/)
- Review [Impala JDBC documentation](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_jdbc.html)
- Open an issue in this repository for driver-specific problems

## Technical Details

### Dependencies

- **Metabase Core**: 0.54.6
- **Clojure**: 1.11.1
- **ImpalaJDBC42**: 2.6.26.1031
- **Java**: 21+

### Driver Implementation

This driver extends Metabase's SQL-JDBC driver framework and implements:

- Connection management and validation
- SQL query generation and optimization
- Data type mapping between Impala and Metabase
- Date/time handling and formatting
- Schema and table introspection

### Performance Considerations

- Use connection pooling for better performance
- Consider partitioning large tables for faster queries
- Impala works best with columnar file formats (Parquet, ORC)
- Use appropriate data types to optimize storage and query performance

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

---

**Note**: This driver requires the Cloudera Impala JDBC driver, which must be downloaded separately from Cloudera due to licensing restrictions.