/*
 * Copyright 2024 asyncer.io projects
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Test infrastructure for MySQL authentication integration tests using TestContainers.
 * <p>
 * This package provides abstract base classes for testing different MySQL authentication
 * methods with R2DBC. Each base class manages a shared Docker container configured for
 * a specific authentication plugin, providing both R2DBC and JDBC access for comprehensive
 * integration testing.
 *
 * <h2>Available Authentication Test Classes</h2>
 * <ul>
 *   <li>{@link io.asyncer.r2dbc.mysql.authentication.AbstractMySqlNativePasswordTest} -
 *       MySQL 5.7 with {@code mysql_native_password} (legacy authentication)</li>
 *   <li>{@link io.asyncer.r2dbc.mysql.authentication.AbstractCachingSha2PasswordTest} -
 *       MySQL 8.0 with {@code caching_sha2_password} (MySQL 8.0+ default)</li>
 *   <li>{@link io.asyncer.r2dbc.mysql.authentication.AbstractSha256PasswordTest} -
 *       MySQL 8.0 with {@code sha256_password} (RSA-based authentication)</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre class="code">
 * package com.example.myapp;
 *
 * import io.asyncer.r2dbc.mysql.authentication.AbstractCachingSha2PasswordTest;
 * import org.junit.jupiter.api.AfterEach;
 * import org.junit.jupiter.api.BeforeEach;
 * import org.junit.jupiter.api.Test;
 *
 * import java.sql.SQLException;
 *
 * class MyAuthenticationTest extends AbstractCachingSha2PasswordTest {
 *
 *     private static final String TEST_USER = "test_user";
 *     private static final String TEST_PASSWORD = "test_pass";
 *
 *     &#64;BeforeEach
 *     void setUp() throws SQLException {
 *         dropUser(TEST_USER);  // Clean up if exists
 *         createUser(TEST_USER, TEST_PASSWORD);
 *         grantAllPrivileges(TEST_USER);
 *     }
 *
 *     &#64;AfterEach
 *     void tearDown() throws SQLException {
 *         dropUser(TEST_USER);
 *     }
 *
 *     &#64;Test
 *     void testAuthentication() {
 *         // Test R2DBC connection
 *         complete(connection ->
 *             connection.createStatement("SELECT 1")
 *                 .execute()
 *                 .flatMap(result -> result.getRowsUpdated())
 *         );
 *     }
 * }
 * </pre>
 *
 * <h2>Container Lifecycle</h2>
 * <p>
 * Each authentication test base class manages a static MySQL container that is:
 * <ul>
 *   <li>Started once during class initialization</li>
 *   <li>Shared across all test classes extending the same base</li>
 *   <li>Stopped via JVM shutdown hook</li>
 * </ul>
 * This approach significantly improves test performance by avoiding the 15-30 second
 * container startup overhead for each test class.
 *
 * <h2>Configuration</h2>
 * <p>
 * Container versions can be customized via system properties:
 * <pre>
 * # MySQL 5.7 (native password)
 * -Dtest.mysql.native.version=5.7.44
 *
 * # MySQL 8.0 (caching_sha2_password)
 * -Dtest.mysql.sha2.version=8.0.35
 *
 * # MySQL 8.0 (sha256_password)
 * -Dtest.mysql.sha256.version=8.0.35
 *
 * # Enable container reuse (faster development)
 * -Dtest.container.reuse=true
 * </pre>
 *
 * <h2>Test Utilities</h2>
 * <p>
 * All authentication test classes inherit utilities from
 * {@link io.asyncer.r2dbc.mysql.authentication.AbstractAuthenticationTest}:
 *
 * <h3>User Management</h3>
 * <ul>
 *   <li>{@code createUser(username, password)} - Create MySQL user</li>
 *   <li>{@code dropUser(username)} - Remove MySQL user</li>
 *   <li>{@code grantAllPrivileges(username)} - Grant all privileges</li>
 * </ul>
 *
 * <h3>JDBC Utilities</h3>
 * <ul>
 *   <li>{@code executeJdbc(sql)} - Execute SQL via JDBC</li>
 *   <li>{@code getJdbcConnection()} - Get JDBC connection for custom operations</li>
 * </ul>
 *
 * <h3>R2DBC Utilities</h3>
 * <ul>
 *   <li>{@code complete(function)} - Execute R2DBC operations expecting success</li>
 *   <li>{@code badGrammar(function)} - Execute expecting SQL syntax errors</li>
 *   <li>{@code process(function)} - Execute with custom StepVerifier assertions</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 * <p>
 * The test infrastructure uses a layered architecture:
 * <pre>
 * AbstractAuthenticationTest (utilities, no container management)
 *   ├── Abstract methods: getContainer(), getJdbcDataSource()
 *   ├── User management helpers
 *   ├── JDBC utilities
 *   └── R2DBC test utilities
 *
 * AbstractMySqlNativePasswordTest extends AbstractAuthenticationTest
 *   ├── Static container (MySQL 5.7)
 *   ├── Static JDBC datasource
 *   └── Implements abstract methods
 *
 * AbstractCachingSha2PasswordTest extends AbstractAuthenticationTest
 *   ├── Static container (MySQL 8.0)
 *   ├── Static JDBC datasource
 *   └── Implements abstract methods
 *
 * AbstractSha256PasswordTest extends AbstractAuthenticationTest
 *   ├── Static container (MySQL 8.0)
 *   ├── Static JDBC datasource
 *   └── Implements abstract methods
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <p>
 * All classes in this package are designed for concurrent test execution:
 * <ul>
 *   <li>Static containers are initialized once per JVM</li>
 *   <li>JDBC datasources use HikariCP connection pooling</li>
 *   <li>R2DBC connections are created per-test via {@code connectionFactory.create()}</li>
 * </ul>
 *
 * <h2>Spring Boot Integration</h2>
 * <p>
 * These test classes can be integrated with Spring Boot using {@code @TestConfiguration}:
 * <pre class="code">
 * &#64;SpringBootTest
 * class MySpringAuthTest extends AbstractCachingSha2PasswordTest {
 *
 *     &#64;TestConfiguration
 *     static class TestConfig {
 *         &#64;Bean
 *         &#64;Primary
 *         ConnectionFactory testConnectionFactory(AbstractAuthenticationTest testBase) {
 *             return testBase.connectionFactory;
 *         }
 *     }
 * }
 * </pre>
 *
 * @since 1.0.0
 * @see io.asyncer.r2dbc.mysql.AbstractMySqlContainerHolder
 * @see org.testcontainers.containers.MySQLContainer
 */
@org.jspecify.annotations.NullMarked
package io.asyncer.r2dbc.mysql.authentication;
