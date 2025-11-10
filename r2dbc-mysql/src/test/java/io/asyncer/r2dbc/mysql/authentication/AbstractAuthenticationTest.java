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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import io.asyncer.r2dbc.mysql.ServerVersion;
import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.MySQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.function.Function;

/**
 * Base class providing common utilities for authentication integration tests.
 * <p>
 * This class provides JDBC and R2DBC utilities, but does NOT manage container lifecycle.
 * Container lifecycle management is delegated to concrete subclasses
 * (e.g., AbstractMySqlNativePasswordTest) which use static containers to ensure
 * all tests of the same authentication type share a single container instance.
 * <p>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #getContainer()} - provide access to the static container</li>
 *   <li>{@link #getJdbcDataSource()} - provide access to the JDBC datasource</li>
 * </ul>
 */
public abstract class AbstractAuthenticationTest {

    protected final MySqlConnectionFactory connectionFactory;

    /**
     * Default constructor using default configuration.
     */
    protected AbstractAuthenticationTest() {
        this(builder -> builder);
    }

    /**
     * Constructor with custom R2DBC configuration.
     *
     * @param customizer function to customize the connection configuration builder
     */
    protected AbstractAuthenticationTest(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {

        MySQLContainer<?> container = getContainer();

        MySqlConnectionConfiguration.Builder builder =
            MySqlConnectionConfiguration.builder()
                .host(container.getHost())
                .port(container.getFirstMappedPort())
                .user(container.getUsername())
                .password(container.getPassword())
                .database(container.getDatabaseName())
                .connectTimeout(Duration.ofSeconds(3));

        this.connectionFactory = MySqlConnectionFactory.from(
            customizer.apply(builder).build()
        );
    }

    // ========== Abstract Methods (implemented by subclasses) ==========

    /**
     * Get the MySQL container instance.
     * Subclasses provide their static container instance.
     *
     * @return the MySQL container
     */
    protected abstract MySQLContainer<?> getContainer();

    /**
     * Get the JDBC datasource for this container.
     * Subclasses provide their static datasource instance.
     *
     * @return the JDBC datasource
     */
    protected abstract HikariDataSource getJdbcDataSource();

    // ========== JDBC Utilities ==========

    /**
     * Create a HikariCP datasource for the given container.
     * Helper method for subclasses to use in their static initialization.
     *
     * @param container the MySQL container
     * @return configured HikariDataSource
     */
    protected static HikariDataSource createJdbcDataSource(MySQLContainer<?> container) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format(
            "jdbc:mysql://%s:%d/%s",
            container.getHost(),
            container.getFirstMappedPort(),
            container.getDatabaseName()
        ));
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        return new HikariDataSource(config);
    }

    /**
     * Get a JDBC connection for test setup and verification.
     *
     * @return a JDBC connection from the connection pool
     * @throws SQLException if a database access error occurs
     */
    protected Connection getJdbcConnection() throws SQLException {
        return getJdbcDataSource().getConnection();
    }

    /**
     * Execute SQL statement via JDBC.
     *
     * @param sql the SQL statement to execute
     * @throws SQLException if a database access error occurs
     */
    protected void executeJdbc(String sql) throws SQLException {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ========== User Management Helpers ==========

    /**
     * Create a MySQL user with a specific authentication plugin.
     *
     * @param username the username
     * @param password the password
     * @param authPlugin the authentication plugin name
     * @throws SQLException if a database access error occurs
     */
    protected void createUser(String username, String password, String authPlugin) throws SQLException {
        executeJdbc(String.format(
            "CREATE USER '%s'@'%%' IDENTIFIED WITH %s BY '%s'",
            username, authPlugin, password
        ));
    }

    /**
     * Drop a MySQL user if exists.
     *
     * @param username the username to drop
     * @throws SQLException if a database access error occurs
     */
    protected void dropUser(String username) throws SQLException {
        executeJdbc(String.format("DROP USER IF EXISTS '%s'@'%%'", username));
    }

    /**
     * Grant all privileges to a user.
     *
     * @param username the username
     * @throws SQLException if a database access error occurs
     */
    protected void grantAllPrivileges(String username) throws SQLException {
        executeJdbc(String.format(
            "GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'", username
        ));
        executeJdbc("FLUSH PRIVILEGES");
    }

    // ========== RSA Public Key Helpers (for SHA2-based auth) ==========

    /**
     * Get the server's RSA public key for authentication.
     * Only applicable for caching_sha2_password and sha256_password.
     *
     * @return the RSA public key string, or null if not available
     * @throws SQLException if a database access error occurs
     */
    protected String getServerRsaPublicKey() throws SQLException {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Rsa_public_key'")) {
            if (rs.next()) {
                return rs.getString(2);
            }
            return null;
        }
    }

    // ========== R2DBC Test Utilities ==========

    /**
     * Create a new R2DBC connection.
     *
     * @return a Mono that completes with a MySqlConnection
     */
    protected Mono<? extends MySqlConnection> create() {
        return connectionFactory.create();
    }

    /**
     * Execute R2DBC operation expecting successful completion.
     *
     * @param runner function that executes operations on the connection
     */
    protected void complete(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyComplete();
    }

    /**
     * Execute R2DBC operation expecting syntax error.
     *
     * @param runner function that executes operations on the connection
     */
    protected void badGrammar(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyError(R2dbcBadGrammarException.class);
    }

    /**
     * Process R2DBC operation and return StepVerifier for custom assertions.
     *
     * @param runner function that executes operations on the connection
     * @return StepVerifier.FirstStep for chaining assertions
     */
    protected StepVerifier.FirstStep<Void> process(
            Function<? super MySqlConnection, Publisher<?>> runner) {
        return create()
            .flatMap(connection -> Flux.from(runner.apply(connection))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
                .then())
            .as(StepVerifier::create);
    }

    /**
     * Extract rows updated from Result.
     *
     * @param result the result to extract from
     * @return a Mono with the number of updated rows
     */
    protected static Mono<Long> extractRowsUpdated(Result result) {
        return Mono.from(result.getRowsUpdated());
    }

    // ========== Server Info ==========

    /**
     * Get server version string.
     *
     * @return the database server version
     * @throws SQLException if a database access error occurs
     */
    protected String getServerVersionString() throws SQLException {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT VERSION()")) {
            if (rs.next()) {
                return rs.getString(1);
            }
            throw new SQLException("Cannot get server version");
        }
    }

    /**
     * Get server version parsed.
     *
     * @return the server version object
     */
    protected ServerVersion getServerVersion() {
        try {
            return ServerVersion.parse(getServerVersionString());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get server version", e);
        }
    }
}
