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
 * Base class for mysql_native_password authentication tests.
 * <p>
 * Uses MySQL 5.7 with mysql_native_password as the default authentication plugin.
 * All tests extending this class share the same MySQL 5.7 container instance.
 */
public abstract class AbstractMySqlNativePasswordTest {

    private static final MySQLContainer<?> CONTAINER;
    private static final HikariDataSource JDBC_DATASOURCE;

    protected final MySqlConnectionFactory connectionFactory;

    static {
        CONTAINER = new MySQLContainer<>("mysql:5.7.44")
            .withUsername("root")
            .withPassword("test")
            .withDatabaseName("test")
            .withCommand(
                "--default-authentication-plugin=mysql_native_password",
                "--character-set-server=utf8mb4",
                "--collation-server=utf8mb4_unicode_ci"
            );
        CONTAINER.start();

        JDBC_DATASOURCE = createJdbcDataSource();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (JDBC_DATASOURCE != null) {
                JDBC_DATASOURCE.close();
            }
            CONTAINER.stop();
        }));
    }

    /**
     * Default constructor using default configuration.
     */
    protected AbstractMySqlNativePasswordTest() {
        this(builder -> builder);
    }

    /**
     * Constructor with custom R2DBC configuration.
     *
     * @param customizer function to customize the connection configuration builder
     */
    protected AbstractMySqlNativePasswordTest(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {
        MySqlConnectionConfiguration.Builder builder =
            MySqlConnectionConfiguration.builder()
                .host(CONTAINER.getHost())
                .port(CONTAINER.getFirstMappedPort())
                .user(CONTAINER.getUsername())
                .password(CONTAINER.getPassword())
                .database(CONTAINER.getDatabaseName())
                .connectTimeout(Duration.ofSeconds(3));

        this.connectionFactory = MySqlConnectionFactory.from(
            customizer.apply(builder).build()
        );
    }

    // ========== JDBC Utilities ==========

    private static HikariDataSource createJdbcDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format(
            "jdbc:mysql://%s:%d/%s",
            CONTAINER.getHost(),
            CONTAINER.getFirstMappedPort(),
            CONTAINER.getDatabaseName()
        ));
        config.setUsername(CONTAINER.getUsername());
        config.setPassword(CONTAINER.getPassword());
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
    protected static Connection getJdbcConnection() throws SQLException {
        return JDBC_DATASOURCE.getConnection();
    }

    /**
     * Execute SQL statement via JDBC.
     *
     * @param sql the SQL statement to execute
     * @throws SQLException if a database access error occurs
     */
    protected static void executeJdbc(String sql) throws SQLException {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ========== User Management Helpers ==========

    /**
     * Create a MySQL user with mysql_native_password authentication.
     *
     * @param username the username
     * @param password the password
     * @throws SQLException if a database access error occurs
     */
    protected void createUser(String username, String password) throws SQLException {
        executeJdbc(String.format(
            "CREATE USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s'",
            username, password
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

    // ========== Container Access ==========

    /**
     * Get the underlying MySQL container.
     *
     * @return the MySQL container instance
     */
    protected static MySQLContainer<?> getContainer() {
        return CONTAINER;
    }

    /**
     * Get server version string.
     *
     * @return the database server version
     * @throws SQLException if a database access error occurs
     */
    protected static String getServerVersionString() throws SQLException {
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
    protected static ServerVersion getServerVersion() {
        try {
            return ServerVersion.parse(getServerVersionString());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get server version", e);
        }
    }
}
