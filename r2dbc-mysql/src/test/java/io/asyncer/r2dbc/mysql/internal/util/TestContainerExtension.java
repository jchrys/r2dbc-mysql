package io.asyncer.r2dbc.mysql.internal.util;

import io.netty.util.internal.SystemPropertyUtil;
import org.jetbrains.annotations.VisibleForTesting;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;

/**
 * JUnit 5 extension to start a test container with MySQL or MariaDB.
 * The extension starts a test container with MySQL or MariaDB based on the system properties:
 * <ul>
 *     <li>{@code test.db.testcontainer} - whether to use a test container or not (default: {@code true})</li>
 *     <li>{@code test.db.type} - the test-container database type (default: {@code mysql})</li>
 *     <li>{@code test.db.version} - the test-container database version (default: {@code 5.7.44})</li>
 *     <li>{@code test.db.host} - the self-hosted database server host (default: {@code 127.0.0.1})</li>
 *     <li>{@code test.db.port} - the self-hosted database server port (default: {@code 3306})</li>
 *     <li>{@code test.db.database} - the self-hoseted database server database name (default: {@code test})</li>
 *     <li>{@code test.db.username} - the self-hosted database server username (default: {@code root})</li>
 *     <li>{@code test.db.password} - the self-hosted database server password (default: {@code root})</li>
 * </ul>
 */
public final class TestContainerExtension implements BeforeAllCallback {

    static final Container server;

    static final boolean useTestContainer;

    static final String dbType;

    static final String dbVersion;

    static {
        useTestContainer = SystemPropertyUtil.getBoolean("test.db.testcontainer", true);
        dbType = SystemPropertyUtil.get("test.db.type", "mysql");
        dbVersion = SystemPropertyUtil.get("test.db.version", "5.7.44");
        if (useTestContainer) {
            server = new TestContainer(dbType, dbVersion);
        } else {
            server = new SelfHostedContainer();
        }
        server.start();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        // NOOP - initialized in the static block
    }

    private static final class TestContainer implements Container {

        private final JdbcDatabaseContainer<?> container;

        @SuppressWarnings("resource")
        private TestContainer(final String dbType, final String dbVersion) {
            if ("mariadb".equalsIgnoreCase(dbType)) {
                container = new MariaDBContainer<>(dbType + ':' + dbVersion)
                        .withUsername("root")
                        .withPassword("")
                        .withNetwork(Network.newNetwork())
                        .withCommand("--character-set-server=utf8mb4",
                                     "--collation-server=utf8mb4_unicode_ci");
            } else {
                container = new MySQLContainer<>(dbType + ':' + dbVersion)
                        .withUsername("root")
                        .withNetwork(Network.newNetwork())
                        .withCommand("--local-infile=true",
                                     "--character-set-server=utf8mb4",
                                     "--collation-server=utf8mb4_unicode_ci");
                if (dbVersion.startsWith("5.5")) {
                    // mysql 5.5.x does not support host_cache_size but latest test container utilizes it.
                    // so we need to remove host_cache_size option when mysql version is 5.5.x and lower
                    // ref: https://github.com/testcontainers/testcontainers-java/issues/8130
                    ((MySQLContainer<?>) container).withConfigurationOverride("testcontainer/mysql-5.5");
                }
            }
        }

        @Override
        public void start() {
            container.start();
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
    }

    private static final class SelfHostedContainer implements Container {

        @Override
        public void start() {
            // NOOP
        }

        @Override
        public String getHost() {
            return SystemPropertyUtil.get("test.db.host", "127.0.0.1");
        }

        @Override
        public int getPort() {
            return SystemPropertyUtil.getInt("test.db.port", 3306);
        }

        @Override
        public String getDatabase() {
            return SystemPropertyUtil.get("test.db.database", "test");
        }

        @Override
        public String getUsername() {
            return SystemPropertyUtil.get("test.db.username", "root");
        }

        @Override
        public String getPassword() {
            return SystemPropertyUtil.get("test.db.password", "root");
        }
    }

    interface Container {

        void start();

        String getHost();

        int getPort();

        String getDatabase();

        String getUsername();

        String getPassword();
    }
}
