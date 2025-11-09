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

package io.asyncer.r2dbc.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.netty.util.internal.SystemPropertyUtil;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

/**
 * Base class for all MySQL/MariaDB integration tests using TestContainers.
 * <p>
 * All tests extending this class share the same MySQL/MariaDB container instance.
 * The container is started once per JVM and stopped on shutdown.
 * <p>
 * Configuration via system properties or .testrc file:
 * <ul>
 *   <li>test.db.testcontainer - whether to use testcontainer (default: true)
 *   <li>test.db.type - database vendor: mysql or mariadb (default: mysql)
 *   <li>test.db.version - database version (default: 5.7.44 for MySQL, 10.11 for MariaDB)
 *   <li>test.db.host - external database host (default: 127.0.0.1)
 *   <li>test.db.port - external database port (default: 3306)
 *   <li>test.db.database - database name (default: test)
 *   <li>test.db.username - database username (default: root)
 *   <li>test.db.password - database password (default: root)
 * </ul>
 * <p>
 * Provides both R2DBC (via MySqlConnectionFactory) and JDBC (via mysql-connector-j or
 * mariadb-connector-j) connections for test setup and verification.
 */
public abstract class AbstractMySqlContainerHolder {

    private static final Container CONTAINER;
    private static final HikariDataSource JDBC_DATASOURCE;

    protected final MySqlConnectionFactory connectionFactory;

    static {
        Config config = loadConfig();
        CONTAINER = createContainer(config);
        CONTAINER.start();
        JDBC_DATASOURCE = createJdbcDataSource(CONTAINER, config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (JDBC_DATASOURCE != null) {
                JDBC_DATASOURCE.close();
            }
            CONTAINER.stop();
        }));
    }

    /**
     * Default constructor using default R2DBC configuration.
     */
    protected AbstractMySqlContainerHolder() {
        this(builder -> builder);
    }

    /**
     * Constructor with custom R2DBC configuration.
     *
     * @param customizer function to customize the connection configuration builder
     */
    protected AbstractMySqlContainerHolder(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {
        this.connectionFactory = MySqlConnectionFactory.from(
            configuration(customizer)
        );
    }

    // ========== Configuration Loading ==========

    private static Config loadConfig() {
        Config config = new Config();

        // Try .testrc file first
        File testrc = new File(".testrc");
        if (testrc.exists()) {
            try (FileReader reader = new FileReader(testrc)) {
                Properties props = new Properties();
                props.load(reader);
                config.loadFromProperties(props);
            } catch (IOException e) {
                // Ignore and fall back to system properties
            }
        }

        // System properties override .testrc
        config.useTestContainer = SystemPropertyUtil.getBoolean(
            "test.db.testcontainer", config.useTestContainer);
        config.vendor = SystemPropertyUtil.get("test.db.type", config.vendor);
        config.version = SystemPropertyUtil.get("test.db.version", config.version);
        config.host = SystemPropertyUtil.get("test.db.host", config.host);
        config.port = SystemPropertyUtil.getInt("test.db.port", config.port);
        config.database = SystemPropertyUtil.get("test.db.database", config.database);
        config.username = SystemPropertyUtil.get("test.db.username", config.username);
        config.password = SystemPropertyUtil.get("test.db.password", config.password);

        // Set default version based on vendor if not specified
        if (config.version == null) {
            config.version = "mysql".equalsIgnoreCase(config.vendor) ? "5.7.44" : "10.11";
        }

        return config;
    }

    // ========== Container Creation ==========

    private static Container createContainer(Config config) {
        if (!config.useTestContainer) {
            return new ExternalContainer(config);
        }

        if ("mariadb".equalsIgnoreCase(config.vendor)) {
            return new MariaDbContainer(config);
        }

        return new MySqlContainer(config);
    }

    // ========== JDBC DataSource Creation ==========

    private static HikariDataSource createJdbcDataSource(Container container, Config config) {
        HikariConfig hikariConfig = new HikariConfig();

        if ("mariadb".equalsIgnoreCase(config.vendor)) {
            // Use MariaDB JDBC driver
            hikariConfig.setJdbcUrl(String.format(
                "jdbc:mariadb://%s:%d/%s",
                container.getHost(),
                container.getPort(),
                container.getDatabase()
            ));
            hikariConfig.setDriverClassName("org.mariadb.jdbc.Driver");
        } else {
            // Use MySQL JDBC driver
            hikariConfig.setJdbcUrl(String.format(
                "jdbc:mysql://%s:%d/%s",
                container.getHost(),
                container.getPort(),
                container.getDatabase()
            ));
            hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        }

        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setMinimumIdle(1);

        return new HikariDataSource(hikariConfig);
    }

    // ========== Public API for Subclasses ==========

    /**
     * Get the underlying container (for advanced usage).
     *
     * @return the container instance
     */
    protected static Container getContainer() {
        return CONTAINER;
    }

    /**
     * Get a JDBC connection for test setup and verification.
     * Uses mysql-connector-j or mariadb-connector-j depending on vendor.
     *
     * @return a JDBC connection from the connection pool
     * @throws SQLException if a database access error occurs
     */
    protected static Connection getJdbcConnection() throws SQLException {
        return JDBC_DATASOURCE.getConnection();
    }

    /**
     * Execute SQL statement via JDBC (convenience method).
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

    /**
     * Execute SQL update statement via JDBC and return result count.
     *
     * @param sql the SQL update statement
     * @return the number of affected rows
     * @throws SQLException if a database access error occurs
     */
    protected static int executeUpdateJdbc(String sql) throws SQLException {
        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * Check if current vendor is MariaDB.
     *
     * @return true if using MariaDB, false if MySQL
     */
    protected static boolean isMariaDb() {
        return CONTAINER.isMariaDb();
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

    /**
     * Create R2DBC connection configuration with customization.
     *
     * @param customizer function to customize the builder
     * @return the configured MySqlConnectionConfiguration
     */
    protected static MySqlConnectionConfiguration configuration(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {

        String localInfilePath = getLocalInfilePath();

        MySqlConnectionConfiguration.Builder builder =
            MySqlConnectionConfiguration.builder()
                .host(CONTAINER.getHost())
                .port(CONTAINER.getPort())
                .user(CONTAINER.getUsername())
                .password(CONTAINER.getPassword())
                .database(CONTAINER.getDatabase())
                .connectTimeout(Duration.ofSeconds(3));

        if (localInfilePath != null) {
            builder.allowLoadLocalInfileInPath(localInfilePath);
        }

        return customizer.apply(builder).build();
    }

    private static String getLocalInfilePath() {
        try {
            URL url = AbstractMySqlContainerHolder.class.getResource("/local/");
            if (url == null) {
                return null;
            }
            Path path = Paths.get(url.toURI());
            return path.toString();
        } catch (Exception e) {
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
     * Execute R2DBC operation expecting timeout error.
     *
     * @param runner function that executes operations on the connection
     */
    protected void timeout(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyError(R2dbcTimeoutException.class);
    }

    /**
     * Execute R2DBC operation expecting illegal argument error.
     *
     * @param runner function that executes operations on the connection
     */
    protected void illegalArgument(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).expectError(IllegalArgumentException.class).verify(Duration.ofSeconds(3));
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

    // ========== Version Check Helpers ==========

    /**
     * Check if environment is less than MySQL 5.6.
     *
     * @return true if MySQL version is less than 5.6, false otherwise or if MariaDB
     */
    protected boolean envIsLessThanMySql56() {
        if (isMariaDb()) {
            return false;
        }
        final ServerVersion ver = getServerVersion();
        return ver.isLessThan(ServerVersion.create(5, 6, 0));
    }

    /**
     * Check if environment is less than MySQL 5.7.8 or MariaDB 10.2.
     *
     * @return true if version is less than the specified version
     */
    protected boolean envIsLessThanMySql578OrMariaDb102() {
        final ServerVersion ver = getServerVersion();
        if (isMariaDb()) {
            return ver.isLessThan(ServerVersion.create(10, 2, 0));
        }
        return ver.isLessThan(ServerVersion.create(5, 7, 8));
    }

    /**
     * Check if environment is MariaDB 10.5.1 or later.
     *
     * @return true if MariaDB version is 10.5.1 or later
     */
    protected static boolean envIsMariaDb10_5_1() {
        if (!isMariaDb()) {
            return false;
        }
        final ServerVersion ver = getServerVersion();
        return ver.isGreaterThanOrEqualTo(ServerVersion.create(10, 5, 1));
    }

    /**
     * Check if environment is less than MySQL 5.7.4 or MariaDB 10.1.1.
     *
     * @return true if version is less than the specified version
     */
    protected boolean envIsLessThanMySql574OrMariaDb1011() {
        final ServerVersion ver = getServerVersion();
        if (isMariaDb()) {
            return ver.isLessThan(ServerVersion.create(10, 1, 1));
        }
        return ver.isLessThan(ServerVersion.create(5, 7, 4));
    }

    // ========== Container Abstraction ==========

    /**
     * Container abstraction for TestContainer or External database.
     */
    protected interface Container {
        void start();
        void stop();
        String getHost();
        int getPort();
        String getDatabase();
        String getUsername();
        String getPassword();
        boolean isMariaDb();
    }

    /**
     * MySQL TestContainer implementation.
     */
    private static class MySqlContainer implements Container {
        private final MySQLContainer<?> container;

        @SuppressWarnings("resource")
        MySqlContainer(Config config) {
            this.container = new MySQLContainer<>("mysql:" + config.version)
                .withUsername(config.username)
                .withPassword(config.password)
                .withDatabaseName(config.database)
                .withCommand(
                    "--local-infile=true",
                    "--character-set-server=utf8mb4",
                    "--collation-server=utf8mb4_unicode_ci"
                )
                .withNetwork(Network.newNetwork());

            // MySQL 5.5 compatibility
            if (config.version.startsWith("5.5")) {
                container.withConfigurationOverride("testcontainer/mysql-5.5");
            }
        }

        @Override
        public void start() {
            container.start();
        }

        @Override
        public void stop() {
            container.stop();
        }

        @Override
        public String getHost() {
            return container.getHost();
        }

        @Override
        public int getPort() {
            return container.getMappedPort(3306);
        }

        @Override
        public String getDatabase() {
            return container.getDatabaseName();
        }

        @Override
        public String getUsername() {
            return container.getUsername();
        }

        @Override
        public String getPassword() {
            return container.getPassword();
        }

        @Override
        public boolean isMariaDb() {
            return false;
        }
    }

    /**
     * MariaDB TestContainer implementation.
     */
    private static class MariaDbContainer implements Container {
        private final MariaDBContainer<?> container;

        @SuppressWarnings("resource")
        MariaDbContainer(Config config) {
            this.container = new MariaDBContainer<>("mariadb:" + config.version)
                .withUsername(config.username)
                .withPassword(config.password)
                .withDatabaseName(config.database)
                .withCommand(
                    "--character-set-server=utf8mb4",
                    "--collation-server=utf8mb4_unicode_ci"
                )
                .withNetwork(Network.newNetwork());
        }

        @Override
        public void start() {
            container.start();
        }

        @Override
        public void stop() {
            container.stop();
        }

        @Override
        public String getHost() {
            return container.getHost();
        }

        @Override
        public int getPort() {
            return container.getMappedPort(3306);
        }

        @Override
        public String getDatabase() {
            return container.getDatabaseName();
        }

        @Override
        public String getUsername() {
            return container.getUsername();
        }

        @Override
        public String getPassword() {
            return container.getPassword();
        }

        @Override
        public boolean isMariaDb() {
            return true;
        }
    }

    /**
     * External database implementation (for local development).
     */
    private static class ExternalContainer implements Container {
        private final Config config;

        ExternalContainer(Config config) {
            this.config = config;
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        public void stop() {
            // no-op
        }

        @Override
        public String getHost() {
            return config.host;
        }

        @Override
        public int getPort() {
            return config.port;
        }

        @Override
        public String getDatabase() {
            return config.database;
        }

        @Override
        public String getUsername() {
            return config.username;
        }

        @Override
        public String getPassword() {
            return config.password;
        }

        @Override
        public boolean isMariaDb() {
            return "mariadb".equalsIgnoreCase(config.vendor);
        }
    }

    /**
     * Configuration holder.
     */
    private static class Config {
        boolean useTestContainer = true;
        String vendor = "mysql";
        String version = null;  // Will be set based on vendor
        String host = "127.0.0.1";
        int port = 3306;
        String database = "test";
        String username = "root";
        String password = "root";

        void loadFromProperties(Properties props) {
            if (props.containsKey("preference")) {
                useTestContainer = !"external".equals(props.getProperty("preference"));
            }
            if (props.containsKey("testcontainer")) {
                useTestContainer = Boolean.parseBoolean(props.getProperty("testcontainer"));
            }
            vendor = props.getProperty("vendor", vendor);
            vendor = props.getProperty("type", vendor);
            version = props.getProperty("version", version);
            host = props.getProperty("host", host);
            if (props.containsKey("port")) {
                port = Integer.parseInt(props.getProperty("port"));
            }
            database = props.getProperty("database", database);
            username = props.getProperty("username", username);
            password = props.getProperty("password", password);
        }
    }
}
