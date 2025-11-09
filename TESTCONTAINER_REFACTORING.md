# TestContainer Refactoring - Migration Guide

## Overview

The r2dbc-mysql TestContainer framework has been refactored to use an **abstract base class approach** instead of JUnit extensions. This provides:

- ✅ **Simpler API** - Direct access to methods via inheritance
- ✅ **Better discoverability** - IDE autocomplete shows all available methods
- ✅ **Flexible configurations** - Different base classes for different MySQL versions/auth methods
- ✅ **JDBC + R2DBC** - Built-in JDBC connection support (mysql-connector-j / mariadb-connector-j)
- ✅ **Property-based configuration** - Configure vendor and version via system properties
- ✅ **Backward compatible** - Existing tests continue to work

---

## Architecture

### New Class Hierarchy

```
AbstractMySqlContainerHolder (Base Class)
├── Manages MySQL/MariaDB container lifecycle
├── Provides both R2DBC and JDBC connections
├── Configured via system properties or .testrc file
└── Shared static container per base class

IntegrationTestSupport (extends AbstractMySqlContainerHolder)
├── Used by existing query integration tests
└── Provides all test utilities

Authentication Test Base Classes:
├── AbstractMySqlNativePasswordTest (MySQL 5.7)
├── AbstractCachingSha2PasswordTest (MySQL 8.0)
└── AbstractSha256PasswordTest (MySQL 8.0)
```

---

## Configuration

### System Properties

```properties
# Database vendor (default: mysql)
test.db.type=mysql|mariadb

# Database version (default: 5.7.44 for MySQL, 10.11 for MariaDB)
test.db.version=8.0.35

# Container mode (default: true)
test.db.testcontainer=true|false

# External database (when testcontainer=false)
test.db.host=localhost
test.db.port=3306
test.db.database=test
test.db.username=root
test.db.password=root
```

### .testrc File (for local development)

Create a `.testrc` file in the project root:

```properties
# Use testcontainer or external database
preference=testcontainer  # or 'external'

# Vendor and version
vendor=mysql
type=mysql
version=8.0.35

# External database settings (if preference=external)
host=localhost
port=3306
database=test
username=root
password=root
```

---

## Usage Examples

### Example 1: Basic Query Test

```java
class MyQueryIntegrationTest extends IntegrationTestSupport {

    @Test
    void testSimpleQuery() throws SQLException {
        // Use JDBC for setup
        executeJdbc("CREATE TABLE test (id INT, name VARCHAR(100))");
        executeJdbc("INSERT INTO test VALUES (1, 'Alice')");

        // Use R2DBC for testing
        complete(connection -> connection
            .createStatement("SELECT name FROM test WHERE id = ?")
            .bind(0, 1)
            .execute()
            .flatMap(result -> result.map((row, meta) ->
                row.get("name", String.class))));
    }
}
```

### Example 2: Authentication Test

```java
class MyAuthIntegrationTest extends AbstractCachingSha2PasswordTest {

    @BeforeEach
    void setUp() throws SQLException {
        dropUser("testuser");
    }

    @AfterEach
    void tearDown() throws SQLException {
        dropUser("testuser");
    }

    @Test
    void testUserAuthentication() throws SQLException {
        // Create user with JDBC
        createUser("testuser", "testpass");
        grantAllPrivileges("testuser");

        // Test R2DBC connection
        MySqlConnectionConfiguration config =
            MySqlConnectionConfiguration.builder()
                .host(getContainer().getHost())
                .port(getContainer().getFirstMappedPort())
                .user("testuser")
                .password("testpass")
                .database("test")
                .build();

        MySqlConnectionFactory factory = MySqlConnectionFactory.from(config);

        Mono.from(factory.create())
            .flatMap(conn -> conn.validate(ValidationDepth.LOCAL)
                .thenReturn(conn))
            .flatMap(conn -> conn.close().thenReturn(true))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }
}
```

---

## Available Base Classes

### 1. AbstractMySqlContainerHolder

**Use for:** Generic MySQL/MariaDB integration tests

**Container:** Configured via system properties (vendor + version)

**Methods:**
- `getJdbcConnection()` - Get JDBC connection
- `executeJdbc(String sql)` - Execute SQL via JDBC
- `create()` - Create R2DBC connection
- `complete(runner)` - Execute R2DBC operation expecting success
- `badGrammar(runner)` - Execute expecting syntax error
- `timeout(runner)` - Execute expecting timeout
- `isMariaDb()` - Check if using MariaDB
- `getServerVersion()` - Get server version

### 2. IntegrationTestSupport

**Use for:** Existing query integration tests (backward compatible)

**Container:** Same as AbstractMySqlContainerHolder

**Additional methods:** Same as parent + configuration helpers

### 3. AbstractMySqlNativePasswordTest

**Use for:** Testing mysql_native_password authentication (MySQL 5.7)

**Container:** MySQL 5.7.44 with mysql_native_password plugin

**Additional methods:**
- `createUser(username, password)` - Create user with mysql_native_password
- `dropUser(username)` - Drop user
- `grantAllPrivileges(username)` - Grant all privileges

### 4. AbstractCachingSha2PasswordTest

**Use for:** Testing caching_sha2_password authentication (MySQL 8.0 default)

**Container:** MySQL 8.0.35 with caching_sha2_password plugin

**Additional methods:**
- `createUser(username, password)` - Create user with caching_sha2_password
- `dropUser(username)` - Drop user
- `grantAllPrivileges(username)` - Grant all privileges
- `getServerRsaPublicKey()` - Get RSA public key for secure authentication

### 5. AbstractSha256PasswordTest

**Use for:** Testing sha256_password authentication (MySQL 8.0)

**Container:** MySQL 8.0.35 with sha256_password plugin

**Additional methods:** Same as AbstractCachingSha2PasswordTest

---

## Migration Guide

### Migrating Existing Tests

**Before (using JUnit extension):**

```java
@ExtendWith(TestContainerExtension.class)
class MyTest {
    @Test
    void test() {
        String host = TestServerUtil.getHost();
        int port = TestServerUtil.getPort();
        // ...
    }
}
```

**After (using abstract base class):**

```java
class MyTest extends IntegrationTestSupport {
    @Test
    void test() {
        Container container = getContainer();
        String host = container.getHost();
        int port = container.getPort();
        // ...
    }
}
```

### For Tests Not Extending IntegrationTestSupport

Tests that directly use `@ExtendWith(TestContainerExtension.class)` should migrate to either:

1. **Extend IntegrationTestSupport** (recommended for most tests)
2. **Extend AbstractMySqlContainerHolder** (if you need more control)
3. **Keep using TestContainerExtension** (deprecated, but still works for now)

---

## Running Tests

### Run with default MySQL version

```bash
mvn test
```

### Run with specific MySQL version

```bash
mvn test -Dtest.db.type=mysql -Dtest.db.version=8.0.35
```

### Run with MariaDB

```bash
mvn test -Dtest.db.type=mariadb -Dtest.db.version=10.11
```

### Run with external database

```bash
mvn test -Dtest.db.testcontainer=false \
  -Dtest.db.host=localhost \
  -Dtest.db.port=3306 \
  -Dtest.db.username=root \
  -Dtest.db.password=root
```

### Run specific authentication tests

```bash
mvn test -Dtest=MySqlNativePasswordIntegrationTest
mvn test -Dtest=CachingSha2PasswordIntegrationTest
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        vendor: [mysql, mariadb]
        version:
          - mysql: ['5.7.44', '8.0.35', '8.4.0', '9.1.0']
          - mariadb: ['10.6', '10.11', '11.0']

    steps:
      - uses: actions/checkout@v3

      - name: Set up JDK 17
        uses: actions/setup-java@v3
        with:
          java-version: '17'

      - name: Run Integration Tests
        run: |
          mvn verify \
            -Dmaven.surefire.skip=true \
            -Dtest.db.type=${{ matrix.vendor }} \
            -Dtest.db.version=${{ matrix.version }}
```

---

## Benefits

### 1. Simpler API

**Before:**
```java
@ExtendWith(TestContainerExtension.class)
class MyTest {
    void test() {
        String host = TestServerUtil.getHost();  // Indirect access
    }
}
```

**After:**
```java
class MyTest extends AbstractMySqlContainerHolder {
    void test() {
        String host = getContainer().getHost();  // Direct access
    }
}
```

### 2. Better Discoverability

IDE autocomplete shows all available methods when you type `this.` or `get...`

### 3. Flexible Configurations

Different base classes = different MySQL containers:
- `IntegrationTestSupport` → Generic MySQL (configurable via properties)
- `AbstractMySqlNativePasswordTest` → MySQL 5.7 with mysql_native_password
- `AbstractCachingSha2PasswordTest` → MySQL 8.0 with caching_sha2_password

### 4. JDBC + R2DBC

```java
class MyTest extends IntegrationTestSupport {
    @Test
    void test() throws SQLException {
        // Setup with JDBC
        executeJdbc("CREATE TABLE test (id INT)");

        // Test with R2DBC
        complete(conn -> conn.createStatement("SELECT * FROM test").execute());

        // Verify with JDBC
        try (Connection jdbc = getJdbcConnection();
             Statement stmt = jdbc.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }
}
```

---

## Authentication Testing

The refactoring adds comprehensive authentication testing support:

### Test Coverage

1. **mysql_native_password** (MySQL 5.7)
   - User creation and authentication
   - Wrong password handling
   - Non-existent user handling
   - Query execution after authentication

2. **caching_sha2_password** (MySQL 8.0 default)
   - User creation and authentication
   - RSA public key support
   - Wrong password handling
   - Query execution after authentication

3. **sha256_password** (MySQL 8.0)
   - User creation and authentication
   - RSA public key support
   - Wrong password handling

### Example Authentication Tests

See:
- `MySqlNativePasswordIntegrationTest.java`
- `CachingSha2PasswordIntegrationTest.java`

---

## Troubleshooting

### Test fails with "Test server is not configured"

Make sure your test class extends one of the base classes:
- `IntegrationTestSupport`
- `AbstractMySqlContainerHolder`
- `AbstractMySqlNativePasswordTest`
- `AbstractCachingSha2PasswordTest`
- `AbstractSha256PasswordTest`

### Container fails to start

Check Docker is running:
```bash
docker ps
```

### Tests are slow

Tests sharing the same base class share the same container instance. The container is started once and reused. If tests are slow, check:
1. Docker performance
2. Network connectivity
3. Resource availability

### Cannot connect to external database

When using `.testrc` with `preference=external`, ensure:
1. Database is running and accessible
2. Credentials are correct
3. Database name exists

---

## Future Improvements

1. **Deprecate and remove** `TestContainerExtension` and `TestServerUtil` after migrating all tests
2. **Add more authentication tests** for edge cases (locked accounts, expired passwords, etc.)
3. **Add MariaDB authentication tests** (different auth plugins)
4. **Add connection pool tests** using the new framework
5. **Add SSL/TLS authentication tests** with certificate chains

---

## Questions?

For questions or issues, please open an issue on GitHub or refer to the test examples in:
- `src/test/java/io/asyncer/r2dbc/mysql/authentication/`
- `src/test/java/io/asyncer/r2dbc/mysql/IntegrationTestSupport.java`
