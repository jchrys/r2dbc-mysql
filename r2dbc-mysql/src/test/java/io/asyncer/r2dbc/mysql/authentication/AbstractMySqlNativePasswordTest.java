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
import org.testcontainers.containers.MySQLContainer;

import java.sql.SQLException;
import java.util.function.Function;

/**
 * Base class for mysql_native_password authentication tests.
 * <p>
 * Uses MySQL 5.7 with mysql_native_password as the default authentication plugin.
 * All tests extending this class share the same MySQL 5.7 container instance
 * for optimal performance.
 */
public abstract class AbstractMySqlNativePasswordTest extends AbstractAuthenticationTest {

    private static final String MYSQL_VERSION = "5.7.44";
    private static final String AUTH_PLUGIN = "mysql_native_password";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "test";
    private static final String DEFAULT_DATABASE = "test";

    // Shared static container - one instance for all test classes
    private static final MySQLContainer<?> CONTAINER;
    private static final HikariDataSource JDBC_DATASOURCE;

    static {
        // Create and start container once
        CONTAINER = createMySqlContainer();
        CONTAINER.start();

        // Create JDBC datasource
        JDBC_DATASOURCE = createJdbcDataSource(CONTAINER);

        // Register shutdown hook once
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (JDBC_DATASOURCE != null && !JDBC_DATASOURCE.isClosed()) {
                JDBC_DATASOURCE.close();
            }
            if (CONTAINER != null && CONTAINER.isRunning()) {
                CONTAINER.stop();
            }
        }));
    }

    @SuppressWarnings("resource")
    private static MySQLContainer<?> createMySqlContainer() {
        return new MySQLContainer<>("mysql:" + MYSQL_VERSION)
            .withUsername(DEFAULT_USERNAME)
            .withPassword(DEFAULT_PASSWORD)
            .withDatabaseName(DEFAULT_DATABASE)
            .withCommand(
                "--default-authentication-plugin=" + AUTH_PLUGIN,
                "--character-set-server=utf8mb4",
                "--collation-server=utf8mb4_unicode_ci"
            );
    }

    protected AbstractMySqlNativePasswordTest() {
        super();
    }

    protected AbstractMySqlNativePasswordTest(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {
        super(customizer);
    }

    @Override
    protected MySQLContainer<?> getContainer() {
        return CONTAINER;
    }

    @Override
    protected HikariDataSource getJdbcDataSource() {
        return JDBC_DATASOURCE;
    }

    /**
     * Create a MySQL user with mysql_native_password authentication.
     * Convenience method that uses the correct auth plugin automatically.
     *
     * @param username the username
     * @param password the password
     * @throws SQLException if a database access error occurs
     */
    protected void createUser(String username, String password) throws SQLException {
        createUser(username, password, AUTH_PLUGIN);
    }
}
