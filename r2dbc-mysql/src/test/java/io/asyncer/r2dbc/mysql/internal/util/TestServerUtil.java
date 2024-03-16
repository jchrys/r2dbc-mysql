package io.asyncer.r2dbc.mysql.internal.util;

import com.zaxxer.hikari.HikariDataSource;
import io.asyncer.r2dbc.mysql.ServerVersion;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class that provides access to the test server configuration.
 */
public final class TestServerUtil {

    static final AtomicReference<DataSource> dataSource = new AtomicReference<>();

    static final AtomicReference<Connection> connection = new AtomicReference<>();

    /**
     * Returns the host of the test server.
     *
     * @return the host of the test server
     */
    public static String getHost() {
        ensureTestContainerIsRunning();
        return TestContainerExtension.server.getHost();
    }

    /**
     * Returns the port of the test server.
     * @return the port of the test server
     */
    public static int getPort() {
        ensureTestContainerIsRunning();
        return TestContainerExtension.server.getPort();
    }

    /**
     * Returns the database name of the test server.
     * @return the database name of the test server
     */
    public static String getDatabase() {
        ensureTestContainerIsRunning();
        return TestContainerExtension.server.getDatabase();
    }

    /**
     * Returns the username of the test server.
     * @return the username of the test server
     */
    public static String getUsername() {
        ensureTestContainerIsRunning();
        return TestContainerExtension.server.getUsername();
    }

    /**
     * Returns the password of the test server.
     * @return the password of the test server
     */
    public static String getPassword() {
        ensureTestContainerIsRunning();
        return TestContainerExtension.server.getPassword();
    }

    /**
     * Returns whether the test server is a MariaDB server.
     * @return whether the test server is a MariaDB server
     */
    public static boolean isMariaDb() {
        ensureTestContainerIsRunning();
        try {
            return !"MySQL".equalsIgnoreCase(getSharedConnection().getMetaData().getDatabaseProductName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the server version of the test server.
     * @return the server version of the test server
     */
    public static ServerVersion getServerVersion() {
        ensureTestContainerIsRunning();
        try {
            return ServerVersion.parse(getSharedConnection().getMetaData().getDatabaseProductVersion());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a shared jdbc connection to the test server.
     * @return a shared jdbc connection to the test server
     */
    public static Connection getSharedConnection() {
        ensureTestContainerIsRunning();
        if (connection.get() == null) {
            connection.compareAndSet(null, getConnection0());
        }
        return connection.get();
    }

    private static Connection getConnection0() {
        ensureTestContainerIsRunning();
        DataSource source = dataSource.get();
        if (source == null) {
            final String connectionString = String.format(
                    "jdbc:mariadb://%s:%s/%s?user=%s&password=%s",
                    // should use mariadb to get correct metadata.getDatabaseProductName()
                    getHost(),
                    getPort(),
                    getDatabase(),
                    getUsername(),
                    getPassword());
            HikariDataSource hikariDataSource = new HikariDataSource();
            hikariDataSource.setJdbcUrl(connectionString);
            dataSource.compareAndSet(null, hikariDataSource);
        }
        try {
            return dataSource.get().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void ensureTestContainerIsRunning() {
        if (TestContainerExtension.server == null) {
            throw new IllegalStateException("Test server is not configured");
        }
        TestContainerExtension.server.start(); // ensure running
    }

    private TestServerUtil() {
        // Utility class
    }
}
