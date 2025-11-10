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

package io.asyncer.r2dbc.mysql.authentication;

import com.zaxxer.hikari.HikariDataSource;
import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.netty.util.internal.SystemPropertyUtil;
import org.testcontainers.containers.MySQLContainer;

import java.sql.SQLException;
import java.util.function.Function;

/**
 * Base class for integration tests using MySQL 8.0 with {@code sha256_password} authentication.
 * <p>
 * This class provides a shared MySQL 8.0 container instance that is started once during class
 * initialization and reused across all test classes that extend this base. The container is
 * configured with:
 * <ul>
 *   <li>MySQL 8.0.35 (default, configurable via {@code test.mysql.sha256.version} system property)</li>
 *   <li>{@code sha256_password} as the default authentication plugin</li>
 *   <li>UTF-8mb4 character set with Unicode collation</li>
 *   <li>Default credentials: root/test, database: test</li>
 *   <li>RSA public key encryption support</li>
 * </ul>
 * <p>
 * <b>Usage Example:</b>
 * <pre class="code">
 * class MyAuthenticationTest extends AbstractSha256PasswordTest {
 *
 *     &#64;Test
 *     void testSha256Authentication() throws SQLException {
 *         // Create a test user with sha256_password authentication
 *         createUser("testuser", "testpass");
 *         grantAllPrivileges("testuser");
 *
 *         try {
 *             // Test R2DBC connection with the created user
 *             complete(connection -> connection.createStatement("SELECT 1")
 *                 .execute()
 *                 .flatMap(result -> result.getRowsUpdated()));
 *         } finally {
 *             // Clean up test user
 *             dropUser("testuser");
 *         }
 *     }
 * }
 * </pre>
 * <p>
 * <b>Container Lifecycle:</b>
 * The MySQL container is started during static initialization and stopped via a JVM shutdown hook.
 * All test classes extending this base share the same container instance, which significantly
 * improves test performance by avoiding the 15-30 second container startup overhead for each
 * test class.
 * <p>
 * <b>Thread Safety:</b>
 * This class uses static initialization with proper synchronization and is thread-safe for
 * concurrent test execution by multiple test classes. The underlying HikariCP datasource
 * provides connection pooling with thread-safe connection management.
 * <p>
 * <b>Test Utilities:</b>
 * This class inherits comprehensive test utilities from {@link AbstractAuthenticationTest}:
 * <ul>
 *   <li>{@link #createUser(String, String)} - Create MySQL user with sha256_password auth</li>
 *   <li>{@link #dropUser(String)} - Remove test user (use in cleanup)</li>
 *   <li>{@link #executeJdbc(String)} - Execute SQL statements via JDBC</li>
 *   <li>{@link #grantAllPrivileges(String)} - Grant privileges to test users</li>
 *   <li>{@link #getServerRsaPublicKey()} - Get RSA public key for authentication</li>
 *   <li>{@link #complete(Function)} - Execute R2DBC operations expecting success</li>
 *   <li>{@link #badGrammar(Function)} - Execute R2DBC operations expecting SQL errors</li>
 *   <li>{@link #process(Function)} - Execute R2DBC operations with custom assertions</li>
 * </ul>
 * <p>
 * <b>Authentication Details:</b>
 * {@code sha256_password} is an authentication plugin available in MySQL 5.6+. It provides:
 * <ul>
 *   <li>SHA-256 password hashing for improved security</li>
 *   <li>RSA public key encryption for password exchange</li>
 *   <li>Requires secure connection or RSA encryption for authentication</li>
 *   <li>Predecessor to {@code caching_sha2_password} (no caching mechanism)</li>
 * </ul>
 * <p>
 * <b>Configuration:</b>
 * The MySQL version can be customized via system property:
 * <pre>
 * -Dtest.mysql.sha256.version=8.0.36
 * </pre>
 * Container reuse for faster development iterations (requires Testcontainers 1.16+):
 * <pre>
 * -Dtest.container.reuse=true
 * </pre>
 * <p>
 * <b>Resource Management:</b>
 * The container and JDBC datasource are automatically cleaned up on JVM shutdown. Test methods
 * should clean up any test users or temporary data created during testing using
 * {@code @AfterEach} methods or try-finally blocks.
 *
 * @since 1.0.0
 * @see AbstractAuthenticationTest
 * @see AbstractMySqlNativePasswordTest
 * @see AbstractCachingSha2PasswordTest
 */
public abstract class AbstractSha256PasswordTest extends AbstractAuthenticationTest {

    /**
     * Default MySQL version for sha256_password authentication tests.
     * Can be overridden via {@code test.mysql.sha256.version} system property.
     */
    private static final String DEFAULT_MYSQL_VERSION = "8.0.35";

    /**
     * MySQL version used for the container (configurable via system property).
     */
    private static final String MYSQL_VERSION = SystemPropertyUtil.get(
        "test.mysql.sha256.version", DEFAULT_MYSQL_VERSION);

    /**
     * Authentication plugin used for this test class.
     */
    private static final String AUTH_PLUGIN = "sha256_password";

    /**
     * Default MySQL username.
     */
    private static final String DEFAULT_USERNAME = "root";

    /**
     * Default MySQL password.
     */
    private static final String DEFAULT_PASSWORD = "test";

    /**
     * Default MySQL database name.
     */
    private static final String DEFAULT_DATABASE = "test";

    /**
     * Whether to enable container reuse for faster development iterations.
     * Enabled via {@code test.container.reuse} system property.
     */
    private static final boolean REUSE_CONTAINER = SystemPropertyUtil.getBoolean(
        "test.container.reuse", false);

    /**
     * Shared static container - one instance for all test classes extending this base.
     * Initialized during class loading and stopped via shutdown hook.
     */
    private static final MySQLContainer<?> CONTAINER;

    /**
     * Shared JDBC datasource for test setup and verification.
     * Initialized alongside the container and closed via shutdown hook.
     */
    private static final HikariDataSource JDBC_DATASOURCE;

    /*
     * Static initialization block that creates and starts the MySQL container.
     * The shutdown hook is registered before starting the container to ensure
     * proper cleanup even if initialization fails partway through.
     */
    static {
        MySQLContainer<?> tempContainer = null;
        HikariDataSource tempDataSource = null;

        try {
            // Create container (not started yet)
            tempContainer = createMySqlContainer();

            // Register shutdown hook BEFORE starting container to ensure cleanup on failure
            final MySQLContainer<?> containerToCleanup = tempContainer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (tempDataSource != null && !tempDataSource.isClosed()) {
                        tempDataSource.close();
                    }
                } catch (Exception e) {
                    // Log to stderr as logging framework may not be available during shutdown
                    System.err.println("Failed to close JDBC datasource for MySQL sha256_password tests: " +
                                     e.getMessage());
                } finally {
                    try {
                        if (containerToCleanup != null && containerToCleanup.isRunning()) {
                            containerToCleanup.stop();
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to stop MySQL container for sha256_password tests: " +
                                         e.getMessage());
                    }
                }
            }, "mysql-sha256-test-cleanup"));

            // Start container (may take 15-30 seconds)
            tempContainer.start();

            // Create JDBC datasource
            tempDataSource = createJdbcDataSource(tempContainer);

        } catch (Exception e) {
            // Clean up on initialization failure
            if (tempDataSource != null) {
                try {
                    tempDataSource.close();
                } catch (Exception closeEx) {
                    // Suppress
                }
            }
            if (tempContainer != null) {
                try {
                    tempContainer.stop();
                } catch (Exception stopEx) {
                    // Suppress
                }
            }
            throw new IllegalStateException(
                "Failed to initialize MySQL container for sha256_password authentication tests. " +
                "This may be due to Docker not running, network issues, or insufficient resources.", e);
        }

        CONTAINER = tempContainer;
        JDBC_DATASOURCE = tempDataSource;
    }

    /**
     * Creates the MySQL container with sha256_password authentication configuration.
     * <p>
     * The container is configured with:
     * <ul>
     *   <li>Version specified by {@code MYSQL_VERSION}</li>
     *   <li>{@code sha256_password} as default auth plugin</li>
     *   <li>UTF-8mb4 character set and Unicode collation</li>
     *   <li>Container reuse if enabled via system property</li>
     * </ul>
     *
     * @return configured but not yet started MySQL container
     */
    @SuppressWarnings("resource")
    private static MySQLContainer<?> createMySqlContainer() {
        MySQLContainer<?> container = new MySQLContainer<>("mysql:" + MYSQL_VERSION)
            .withUsername(DEFAULT_USERNAME)
            .withPassword(DEFAULT_PASSWORD)
            .withDatabaseName(DEFAULT_DATABASE)
            .withCommand(
                "--default-authentication-plugin=" + AUTH_PLUGIN,
                "--character-set-server=utf8mb4",
                "--collation-server=utf8mb4_unicode_ci"
            );

        // Enable container reuse for faster development if configured
        if (REUSE_CONTAINER) {
            container.withReuse(true);
        }

        return container;
    }

    /**
     * Default constructor using default R2DBC configuration.
     * <p>
     * The connection will use the container's host, port, and credentials.
     */
    protected AbstractSha256PasswordTest() {
        super();
    }

    /**
     * Constructor with custom R2DBC configuration.
     * <p>
     * Allows customization of connection configuration such as SSL settings,
     * timeouts, or other R2DBC options while still using the shared container.
     *
     * @param customizer function to customize the connection configuration builder
     */
    protected AbstractSha256PasswordTest(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {
        super(customizer);
    }

    /**
     * Returns the shared MySQL container instance.
     * <p>
     * This container is shared across all test classes extending this base.
     *
     * @return the MySQL 8.0 container with sha256_password authentication
     */
    @Override
    protected MySQLContainer<?> getContainer() {
        return CONTAINER;
    }

    /**
     * Returns the shared JDBC datasource.
     * <p>
     * This datasource is backed by HikariCP connection pool and can be safely
     * used concurrently by multiple tests.
     *
     * @return the JDBC datasource connected to the MySQL container
     */
    @Override
    protected HikariDataSource getJdbcDataSource() {
        return JDBC_DATASOURCE;
    }

    /**
     * Creates a MySQL user with {@code sha256_password} authentication.
     * <p>
     * This is a convenience method that automatically uses the correct authentication
     * plugin for this test class. The created user will have no privileges by default;
     * use {@link #grantAllPrivileges(String)} to grant permissions.
     * <p>
     * <b>Important:</b> Always clean up created users in {@code @AfterEach} methods
     * or try-finally blocks to avoid test pollution.
     * <p>
     * <b>Note:</b> Authentication with sha256_password requires either a secure
     * connection or RSA key exchange. The R2DBC driver handles this automatically.
     *
     * @param username the username for the new user (must be valid MySQL identifier)
     * @param password the password for the new user
     * @throws SQLException if the user already exists, contains invalid characters,
     *                      or a database access error occurs
     * @throws IllegalArgumentException if username or password is null
     * @see #dropUser(String)
     * @see #grantAllPrivileges(String)
     * @see #getServerRsaPublicKey()
     */
    protected void createUser(String username, String password) throws SQLException {
        if (username == null || password == null) {
            throw new IllegalArgumentException("Username and password must not be null");
        }
        createUser(username, password, AUTH_PLUGIN);
    }
}
