/*
 * Copyright 2023 asyncer.io projects
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

import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import io.asyncer.r2dbc.mysql.constant.HaProtocol;
import io.asyncer.r2dbc.mysql.constant.ProtocolDriver;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.asyncer.r2dbc.mysql.extension.Extension;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.netty.handler.ssl.SslContextBuilder;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;

import javax.net.ssl.HostnameVerifier;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A configuration of MySQL connection.
 */
public final class MySqlConnectionConfiguration {

    private final SocketClientConfiguration client;

    private final SocketConfiguration socket;

    private final MySqlSslConfiguration ssl;

    private final boolean autoReconnect;

    private final boolean preserveInstants;

    private final String connectionTimeZone;

    private final boolean forceConnectionTimeZoneToSession;

    private final ZeroDateOption zeroDateOption;

    private final Mono<String> user;

    private final Mono<Optional<CharSequence>> password;

    private final String database;

    private final boolean createDatabaseIfNotExist;

    @Nullable
    private final Predicate<String> preferPrepareStatement;

    private final List<String> sessionVariables;

    @Nullable
    private final Duration lockWaitTimeout;

    @Nullable
    private final Duration statementTimeout;

    @Nullable
    private final Path loadLocalInfilePath;

    private final int localInfileBufferSize;

    private final int queryCacheSize;

    private final int prepareCacheSize;

    private final Set<CompressionAlgorithm> compressionAlgorithms;

    private final int zstdCompressionLevel;

    private final Extensions extensions;

    private MySqlConnectionConfiguration(
        SocketClientConfiguration client,
        SocketConfiguration socket,
        MySqlSslConfiguration ssl,
        boolean autoReconnect,
        ZeroDateOption zeroDateOption,
        boolean preserveInstants,
        String connectionTimeZone,
        boolean forceConnectionTimeZoneToSession,
        Mono<String> user,
        Mono<Optional<CharSequence>> password,
        @Nullable String database,
        boolean createDatabaseIfNotExist, @Nullable Predicate<String> preferPrepareStatement,
        List<String> sessionVariables, @Nullable Duration lockWaitTimeout, @Nullable Duration statementTimeout,
        @Nullable Path loadLocalInfilePath, int localInfileBufferSize,
        int queryCacheSize, int prepareCacheSize,
        Set<CompressionAlgorithm> compressionAlgorithms, int zstdCompressionLevel,
        Extensions extensions
    ) {
        this.client = requireNonNull(client, "client must not be null");
        this.socket = requireNonNull(socket, "socket must not be null");
        this.ssl = requireNonNull(ssl, "ssl must not be null");
        this.autoReconnect = autoReconnect;
        this.preserveInstants = preserveInstants;
        this.connectionTimeZone = requireNonNull(connectionTimeZone, "connectionTimeZone must not be null");
        this.forceConnectionTimeZoneToSession = forceConnectionTimeZoneToSession;
        this.zeroDateOption = requireNonNull(zeroDateOption, "zeroDateOption must not be null");
        this.user = requireNonNull(user, "user must not be null");
        this.password = requireNonNull(password, "password must not be null");
        this.database = database == null || database.isEmpty() ? "" : database;
        this.createDatabaseIfNotExist = createDatabaseIfNotExist;
        this.preferPrepareStatement = preferPrepareStatement;
        this.sessionVariables = sessionVariables;
        this.lockWaitTimeout = lockWaitTimeout;
        this.statementTimeout = statementTimeout;
        this.loadLocalInfilePath = loadLocalInfilePath;
        this.localInfileBufferSize = localInfileBufferSize;
        this.queryCacheSize = queryCacheSize;
        this.prepareCacheSize = prepareCacheSize;
        this.compressionAlgorithms = compressionAlgorithms;
        this.zstdCompressionLevel = zstdCompressionLevel;
        this.extensions = extensions;
    }

    /**
     * Creates a builder of the configuration. All options are default.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    SocketClientConfiguration getClient() {
        return client;
    }

    SocketConfiguration getSocket() {
        return socket;
    }

    MySqlSslConfiguration getSsl() {
        return ssl;
    }

    boolean isAutoReconnect() {
        return autoReconnect;
    }

    ZeroDateOption getZeroDateOption() {
        return zeroDateOption;
    }

    boolean isPreserveInstants() {
        return preserveInstants;
    }

    String getConnectionTimeZone() {
        return connectionTimeZone;
    }

    @Nullable
    ZoneId retrieveConnectionZoneId() {
        String timeZone = this.connectionTimeZone;

        if ("LOCAL".equalsIgnoreCase(timeZone)) {
            return ZoneId.systemDefault().normalized();
        } else if ("SERVER".equalsIgnoreCase(timeZone)) {
            return null;
        }

        return StringUtils.parseZoneId(timeZone);
    }

    boolean isForceConnectionTimeZoneToSession() {
        return forceConnectionTimeZoneToSession;
    }

    Mono<Credential> getCredential() {
        return Mono.zip(user, password, (u, p) -> new Credential(u, p.orElse(null)));
    }

    String getDatabase() {
        return database;
    }

    boolean isCreateDatabaseIfNotExist() {
        return createDatabaseIfNotExist;
    }

    @Nullable
    Predicate<String> getPreferPrepareStatement() {
        return preferPrepareStatement;
    }

    List<String> getSessionVariables() {
        return sessionVariables;
    }

    @Nullable
    Duration getLockWaitTimeout() {
        return lockWaitTimeout;
    }

    @Nullable
    Duration getStatementTimeout() {
        return statementTimeout;
    }

    @Nullable
    Path getLoadLocalInfilePath() {
        return loadLocalInfilePath;
    }

    int getLocalInfileBufferSize() {
        return localInfileBufferSize;
    }

    int getQueryCacheSize() {
        return queryCacheSize;
    }

    int getPrepareCacheSize() {
        return prepareCacheSize;
    }

    Set<CompressionAlgorithm> getCompressionAlgorithms() {
        return compressionAlgorithms;
    }

    int getZstdCompressionLevel() {
        return zstdCompressionLevel;
    }

    Extensions getExtensions() {
        return extensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlConnectionConfiguration)) {
            return false;
        }

        MySqlConnectionConfiguration that = (MySqlConnectionConfiguration) o;

        return client.equals(that.client) &&
            socket.equals(that.socket) &&
            ssl.equals(that.ssl) &&
            autoReconnect == that.autoReconnect &&
            preserveInstants == that.preserveInstants &&
            connectionTimeZone.equals(that.connectionTimeZone) &&
            forceConnectionTimeZoneToSession == that.forceConnectionTimeZoneToSession &&
            zeroDateOption == that.zeroDateOption &&
            user.equals(that.user) &&
            password.equals(that.password) &&
            database.equals(that.database) &&
            createDatabaseIfNotExist == that.createDatabaseIfNotExist &&
            Objects.equals(preferPrepareStatement, that.preferPrepareStatement) &&
            sessionVariables.equals(that.sessionVariables) &&
            Objects.equals(lockWaitTimeout, that.lockWaitTimeout) &&
            Objects.equals(statementTimeout, that.statementTimeout) &&
            Objects.equals(loadLocalInfilePath, that.loadLocalInfilePath) &&
            localInfileBufferSize == that.localInfileBufferSize &&
            queryCacheSize == that.queryCacheSize &&
            prepareCacheSize == that.prepareCacheSize &&
            compressionAlgorithms.equals(that.compressionAlgorithms) &&
            zstdCompressionLevel == that.zstdCompressionLevel &&
            extensions.equals(that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            client,
            socket, ssl,
            autoReconnect,
            preserveInstants,
            connectionTimeZone,
            forceConnectionTimeZoneToSession,
            zeroDateOption,
            user, password,
            database,
            createDatabaseIfNotExist,
            preferPrepareStatement,
            sessionVariables,
            lockWaitTimeout,
            statementTimeout,
            loadLocalInfilePath, localInfileBufferSize,
            queryCacheSize, prepareCacheSize,
            compressionAlgorithms, zstdCompressionLevel,
            extensions);
    }

    @Override
    public String toString() {
        return "MySqlConnectionConfiguration{client=" + client +
            ", socket=" + socket +
            ", ssl=" + ssl +
            ", autoReconnect=" + autoReconnect +
            ", preserveInstants=" + preserveInstants +
            ", connectionTimeZone='" + connectionTimeZone + '\'' +
            ", forceConnectionTimeZoneToSession=" + forceConnectionTimeZoneToSession +
            ", zeroDateOption=" + zeroDateOption +
            ", user=" + user +
            ", password=REDACTED" +
            ", database='" + database + '\'' +
            ", createDatabaseIfNotExist=" + createDatabaseIfNotExist +
            ", preferPrepareStatement=" + preferPrepareStatement +
            ", sessionVariables=" + sessionVariables +
            ", lockWaitTimeout=" + lockWaitTimeout +
            ", statementTimeout=" + statementTimeout +
            ", loadLocalInfilePath=" + loadLocalInfilePath +
            ", localInfileBufferSize=" + localInfileBufferSize +
            ", queryCacheSize=" + queryCacheSize +
            ", prepareCacheSize=" + prepareCacheSize +
            ", compressionAlgorithms=" + compressionAlgorithms +
            ", zstdCompressionLevel=" + zstdCompressionLevel +
            ", extensions=" + extensions +
            '}';
    }

    /**
     * A builder for {@link MySqlConnectionConfiguration} creation.
     */
    public static final class Builder {

        private final SocketClientConfiguration.Builder client = new SocketClientConfiguration.Builder();

        @Nullable
        private TcpSocketConfiguration.Builder tcpSocket;

        @Nullable
        private UnixDomainSocketConfiguration.Builder unixSocket;

        private final MySqlSslConfiguration.Builder ssl = new MySqlSslConfiguration.Builder();

        private boolean autoReconnect;

        @Nullable
        private String database;

        private boolean createDatabaseIfNotExist;

        @Nullable
        private Mono<String> user;

        @Nullable
        private Mono<CharSequence> password;

        private ZeroDateOption zeroDateOption = ZeroDateOption.USE_NULL;

        private boolean preserveInstants = true;

        private String connectionTimeZone = "LOCAL";

        private boolean forceConnectionTimeZoneToSession;

        @Nullable
        private Predicate<String> preferPrepareStatement;

        @Nullable
        private Duration lockWaitTimeout;

        @Nullable
        private Duration statementTimeout;

        private List<String> sessionVariables = Collections.emptyList();

        @Nullable
        private Path loadLocalInfilePath;

        private int localInfileBufferSize = 8192;

        private int queryCacheSize = 0;

        private int prepareCacheSize = 256;

        private Set<CompressionAlgorithm> compressionAlgorithms =
            Collections.singleton(CompressionAlgorithm.UNCOMPRESSED);

        private int zstdCompressionLevel = 3;

        private boolean autodetectExtensions = true;

        private final List<Extension> extensions = new ArrayList<>();

        /**
         * Builds an immutable {@link MySqlConnectionConfiguration} with current options.
         *
         * @return the {@link MySqlConnectionConfiguration}.
         */
        public MySqlConnectionConfiguration build() {
            Mono<String> user = requireNonNull(this.user, "User must be configured");
            Mono<CharSequence> auth = this.password;
            Mono<Optional<CharSequence>> password = auth == null ? Mono.just(Optional.empty()) : auth.singleOptional();
            SocketConfiguration socket;
            boolean preferredSsl;

            if (unixSocket == null) {
                socket = requireNonNull(tcpSocket, "Connection must be either TCP/SSL or Unix Domain Socket").build();
                preferredSsl = true;
            } else {
                // Since 1.2.0, we support SSL over Unix Domain Socket, default SSL mode is DISABLED.
                // But, if a Unix Domain Socket can be listened to by someone, this indicates that the system itself
                // has been compromised, and enabling SSL does not improve the security of the connection.
                socket = unixSocket.build();
                preferredSsl = false;
            }

            int prepareCacheSize = preferPrepareStatement == null ? 0 : this.prepareCacheSize;

            return new MySqlConnectionConfiguration(
                client.build(),
                socket,
                ssl.build(preferredSsl),
                autoReconnect,
                zeroDateOption,
                preserveInstants,
                connectionTimeZone,
                forceConnectionTimeZoneToSession,
                user.single(),
                password,
                database,
                createDatabaseIfNotExist,
                preferPrepareStatement,
                sessionVariables,
                lockWaitTimeout,
                statementTimeout,
                loadLocalInfilePath,
                localInfileBufferSize,
                queryCacheSize,
                prepareCacheSize,
                compressionAlgorithms,
                zstdCompressionLevel,
                Extensions.from(extensions, autodetectExtensions));
        }

        /**
         * Configures the database.  Default no database.
         *
         * @param database the database, or {@code null} if no database want to be login.
         * @return {@link Builder this}
         * @since 0.8.1
         */
        public Builder database(@Nullable String database) {
            this.database = database;
            return this;
        }

        /**
         * Configures to create the database given in the configuration if it does not yet exist.  Default to
         * {@code false}.
         *
         * @param enabled to discover and register extensions.
         * @return {@link Builder this}
         * @since 1.0.6
         */
        public Builder createDatabaseIfNotExist(boolean enabled) {
            this.createDatabaseIfNotExist = enabled;
            return this;
        }

        /**
         * Configures the Unix Domain Socket to connect to.
         * <p>
         * Note: It will override all TCP and SSL configurations if configured.
         *
         * @param path the socket file path.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code path} is {@code null} or empty.
         * @since 0.8.1
         */
        public Builder unixSocket(String path) {
            requireNonEmpty(path, "path must not be null");

            requireUnixSocket().path(path);
            return this;
        }

        /**
         * Configures the single-host.
         * <p>
         * Note: Used only if the {@link #unixSocket(String)} and {@link #addHost multiple hosts} is not configured.
         *
         * @param host the host.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code host} is {@code null} or empty.
         * @since 0.8.1
         */
        public Builder host(String host) {
            requireNonEmpty(host, "host must not be empty");

            requireTcpSocket().host(host);
            return this;
        }

        /**
         * Configures the port of {@link #host(String)}.  Defaults to {@code 3306}.
         * <p>
         * Note: Used only if the {@link #unixSocket(String)} and {@link #addHost multiple hosts} is not configured.
         *
         * @param port the port.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if the {@code port} is negative or bigger than {@literal 65535}.
         * @since 0.8.1
         */
        public Builder port(int port) {
            require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535");

            requireTcpSocket().port(port);
            return this;
        }

        /**
         * Adds a host with default port 3306 to the list of multiple hosts to connect to.
         * <p>
         * Note: Used only if the {@link #unixSocket(String)} and {@link #host single host} is not configured.
         *
         * @param host the host to add.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code host} is {@code null} or empty.
         * @since 1.2.0
         */
        public Builder addHost(String host) {
            requireNonEmpty(host, "host must not be empty");

            requireTcpSocket().addHost(host);
            return this;
        }

        /**
         * Adds a host to the list of multiple hosts to connect to.
         * <p>
         * Note: Used only if the {@link #unixSocket(String)} and {@link #host single host} is not configured.
         *
         * @param host the host to add.
         * @param port the port of the host.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if the {@code host} is empty or the {@code port} is not between 0 and
         *                                  65535.
         * @since 1.2.0
         */
        public Builder addHost(String host, int port) {
            requireNonEmpty(host, "host must not be empty");
            require(port >= 0 && port <= 0xFFFF, "port must be between 0 and 65535");

            requireTcpSocket().addHost(host, port);
            return this;
        }

        /**
         * Configures the failover and high availability protocol driver.  Default to {@link ProtocolDriver#MYSQL}. Used
         * only if the {@link #unixSocket(String)} is not configured.
         *
         * @param driver the protocol driver.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code driver} is {@code null}.
         * @since 1.2.0
         */
        public Builder driver(ProtocolDriver driver) {
            requireNonNull(driver, "driver must not be null");

            requireTcpSocket().driver(driver);
            return this;
        }

        /**
         * Configures the failover and high availability protocol.  Default to {@link HaProtocol#DEFAULT}. Used only if
         * the {@link #unixSocket(String)} is not configured.
         *
         * @param protocol the failover and high availability protocol.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code protocol} is {@code null}.
         * @since 1.2.0
         */
        public Builder protocol(HaProtocol protocol) {
            requireNonNull(protocol, "protocol must not be null");

            requireTcpSocket().protocol(protocol);
            return this;
        }

        /**
         * Configures whether to perform failover reconnection.  Default is {@code false}.
         * <p>
         * It is not recommended due to it may lead to unexpected results. For example, it may recover a transaction
         * state from a failed server node to an available node, the user can not aware of it, and continuing to execute
         * more queries in the transaction will lead to unexpected inconsistencies.
         *
         * @param enabled {@code true} to enable failover reconnection.
         * @return {@link Builder this}
         * @see <a href="https://dev.mysql.com/doc/connector-j/en/connector-j-config-failover.html">JDBC Failover</a>
         * @since 1.2.0
         */
        public Builder autoReconnect(boolean enabled) {
            this.autoReconnect = enabled;
            return this;
        }

        /**
         * Configures the connection timeout.  Default no timeout.
         *
         * @param connectTimeout the connection timeout, or {@code null} if no timeout.
         * @return {@link Builder this}
         * @since 0.8.1
         */
        public Builder connectTimeout(@Nullable Duration connectTimeout) {
            this.client.connectTimeout(connectTimeout);
            return this;
        }

        /**
         * Configures the user for login the database.
         *
         * @param user the user.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code user} is {@code null} or empty.
         * @since 0.8.2
         */
        public Builder user(String user) {
            requireNonEmpty(user, "user must not be empty");

            this.user = Mono.just(user);
            return this;
        }

        /**
         * Configures the user for login the database.
         *
         * @param user a {@link Supplier} to retrieve user.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code user} is {@code null}.
         * @since 1.2.0
         */
        public Builder user(Supplier<String> user) {
            requireNonNull(user, "user must not be null");

            this.user = Mono.fromSupplier(user);
            return this;
        }

        /**
         * Configures the user for login the database.
         *
         * @param user a {@link Publisher} to retrieve user.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code user} is {@code null}.
         * @since 1.2.0
         */
        public Builder user(Publisher<String> user) {
            requireNonNull(user, "user must not be null");

            this.user = Mono.from(user);
            return this;
        }

        /**
         * Configures the user for login the database. Since 0.8.2, it is an alias of {@link #user(String)}.
         *
         * @param user the user.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code user} is {@code null}.
         * @since 0.8.1
         */
        public Builder username(String user) {
            return user(user);
        }

        /**
         * Configures the password.  Default login without password.
         * <p>
         * Note: for memory security, should not use intern {@link String} for password.
         *
         * @param password the password, or {@code null} when user has no password.
         * @return {@link Builder this}
         * @since 0.8.1
         */
        public Builder password(@Nullable CharSequence password) {
            this.password = Mono.justOrEmpty(password);
            return this;
        }

        /**
         * Configures the password.  Default login without password.
         *
         * @param password a {@link Supplier} to retrieve password.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code password} is {@code null}.
         * @since 1.2.0
         */
        public Builder password(Supplier<? extends CharSequence> password) {
            requireNonNull(password, "password must not be null");

            this.password = Mono.fromSupplier(password);
            return this;
        }

        /**
         * Configures the password.  Default login without password.
         *
         * @param password a {@link Publisher} to retrieve password.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code password} is {@code null}.
         * @since 1.2.0
         */
        public Builder password(Publisher<? extends CharSequence> password) {
            requireNonNull(password, "password must not be null");

            this.password = Mono.from(password);
            return this;
        }

        /**
         * Configures the time zone conversion.  Default to {@code true} means enable conversion between JVM and
         * {@link #connectionTimeZone(String)}.
         * <p>
         * Note: disable it will ignore the time zone of connection, and use the JVM local time zone.
         *
         * @param enabled {@code true} to preserve instants, or {@code false} to disable conversion.
         * @return {@link Builder this}
         * @since 1.1.2
         */
        public Builder preserveInstants(boolean enabled) {
            this.preserveInstants = enabled;
            return this;
        }

        /**
         * Configures the time zone of connection.  Default to {@code LOCAL} means use JVM local time zone.
         * {@code "SERVER"} means querying the server-side timezone during initialization.
         *
         * @param connectionTimeZone {@code "LOCAL"}, {@code "SERVER"}, or a valid ID of {@code ZoneId}.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code connectionTimeZone} is {@code null} or empty.
         * @since 1.1.2
         */
        public Builder connectionTimeZone(String connectionTimeZone) {
            requireNonEmpty(connectionTimeZone, "connectionTimeZone must not be empty");

            this.connectionTimeZone = connectionTimeZone;
            return this;
        }

        /**
         * Configures to force the connection time zone to session time zone.  Default to {@code false}. Used only if
         * the {@link #connectionTimeZone(String)} is not {@code "SERVER"}.
         * <p>
         * Note: alter the time zone of session will affect the results of MySQL date/time functions, e.g.
         * {@code NOW([n])}, {@code CURRENT_TIME([n])}, {@code CURRENT_DATE()}, etc. Please use with caution.
         *
         * @param enabled {@code true} to force the connection time zone to session time zone.
         * @return {@link Builder this}
         * @since 1.1.2
         */
        public Builder forceConnectionTimeZoneToSession(boolean enabled) {
            this.forceConnectionTimeZoneToSession = enabled;
            return this;
        }

        /**
         * Configures the time zone of server.  Since 1.1.2, default to use JVM local time zone.
         *
         * @param serverZoneId the {@link ZoneId}, or {@code null} if query server during initialization.
         * @return {@link Builder this}
         * @since 0.8.2
         * @deprecated since 1.1.2, use {@link #connectionTimeZone(String)} instead.
         */
        @Deprecated
        public Builder serverZoneId(@Nullable ZoneId serverZoneId) {
            return connectionTimeZone(serverZoneId == null ? "SERVER" : serverZoneId.getId());
        }

        /**
         * Configures the {@link ZeroDateOption}.  Default to {@link ZeroDateOption#USE_NULL}.  It is a behavior option
         * when this driver receives a value of zero-date.
         *
         * @param zeroDate the {@link ZeroDateOption}.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code zeroDate} is {@code null}.
         * @since 0.8.1
         */
        public Builder zeroDateOption(ZeroDateOption zeroDate) {
            this.zeroDateOption = requireNonNull(zeroDate, "zeroDateOption must not be null");
            return this;
        }

        /**
         * Configures ssl mode.  See also {@link SslMode}.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param sslMode the SSL mode to use.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code sslMode} is {@code null}.
         * @since 0.8.1
         */
        public Builder sslMode(SslMode sslMode) {
            requireNonNull(sslMode, "sslMode must not be null");

            this.ssl.sslMode(sslMode);
            return this;
        }

        /**
         * Configures TLS versions, see {@link io.asyncer.r2dbc.mysql.constant.TlsVersions TlsVersions}.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param tlsVersion TLS versions.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if the array {@code tlsVersion} is {@code null}.
         * @since 0.8.1
         */
        public Builder tlsVersion(String... tlsVersion) {
            requireNonNull(tlsVersion, "tlsVersion must not be null");

            this.ssl.tlsVersions(tlsVersion);
            return this;
        }

        /**
         * Configures SSL {@link HostnameVerifier}, it is available only set {@link #sslMode(SslMode)} as
         * {@link SslMode#VERIFY_IDENTITY}. It is useful when server was using special Certificates or need special
         * verification.
         * <p>
         * Default is builtin {@link HostnameVerifier} which use RFC standards.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param sslHostnameVerifier the custom {@link HostnameVerifier}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code sslHostnameVerifier} is {@code null}
         * @since 0.8.2
         */
        public Builder sslHostnameVerifier(HostnameVerifier sslHostnameVerifier) {
            requireNonNull(sslHostnameVerifier, "sslHostnameVerifier must not be null");

            this.ssl.sslHostnameVerifier(sslHostnameVerifier);
            return this;
        }

        /**
         * Configures SSL root certification for server certificate validation. It is only available if the
         * {@link #sslMode(SslMode)} is configured for verify server certification.
         * <p>
         * Default is {@code null}, which means that the default algorithm is used for the trust manager.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param sslCa an X.509 certificate chain file in PEM format.
         * @return {@link Builder this}
         * @since 0.8.1
         */
        public Builder sslCa(@Nullable String sslCa) {
            this.ssl.sslCa(sslCa);
            return this;
        }

        /**
         * Configures client SSL certificate for client authentication.
         * <p>
         * It and {@link #sslKey} must be both non-{@code null} or both {@code null}.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param sslCert an X.509 certificate chain file in PEM format, or {@code null} if no SSL cert.
         * @return {@link Builder this}
         * @since 0.8.2
         */
        public Builder sslCert(@Nullable String sslCert) {
            this.ssl.sslCert(sslCert);
            return this;
        }

        /**
         * Configures client SSL key for client authentication.
         * <p>
         * It and {@link #sslCert} must be both non-{@code null} or both {@code null}.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param sslKey a PKCS#8 private key file in PEM format, or {@code null} if no SSL key.
         * @return {@link Builder this}
         * @since 0.8.2
         */
        public Builder sslKey(@Nullable String sslKey) {
            this.ssl.sslKey(sslKey);
            return this;
        }

        /**
         * Configures the password of SSL key file for client certificate authentication.
         * <p>
         * It will be used only if {@link #sslKey} and {@link #sslCert} non-null.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param sslKeyPassword the password of the {@link #sslKey}, or {@code null} if it's not password-protected.
         * @return {@link Builder this}
         * @since 0.8.2
         */
        public Builder sslKeyPassword(@Nullable CharSequence sslKeyPassword) {
            this.ssl.sslKeyPassword(sslKeyPassword);
            return this;
        }

        /**
         * Configures a {@link SslContextBuilder} customizer. The customizer gets applied on each SSL connection attempt
         * to allow for just-in-time configuration updates. The {@link Function} gets called with the prepared
         * {@link SslContextBuilder} that has all configuration options applied. The customizer may return the same
         * builder or return a new builder instance to be used to build the SSL context.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param customizer customizer function
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code customizer} is {@code null}
         * @since 0.8.1
         */
        public Builder sslContextBuilderCustomizer(Function<SslContextBuilder, SslContextBuilder> customizer) {
            requireNonNull(customizer, "sslContextBuilderCustomizer must not be null");

            this.ssl.sslContextBuilderCustomizer(customizer);
            return this;
        }

        /**
         * Configures TCP KeepAlive.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param enabled whether to enable TCP KeepAlive
         * @return this {@link Builder}
         * @see Socket#setKeepAlive(boolean)
         * @since 0.8.2
         */
        public Builder tcpKeepAlive(boolean enabled) {
            requireTcpSocket().tcpKeepAlive(enabled);
            return this;
        }

        /**
         * Configures TCP NoDelay.
         * <p>
         * Note: It is used only if the {@link #unixSocket(String)} is not configured.
         *
         * @param enabled whether to enable TCP NoDelay
         * @return this {@link Builder}
         * @see Socket#setTcpNoDelay(boolean)
         * @since 0.8.2
         */
        public Builder tcpNoDelay(boolean enabled) {
            requireTcpSocket().tcpNoDelay(enabled);
            return this;
        }

        /**
         * Configures the protocol of parameterized statements to the text protocol.
         * <p>
         * The text protocol is default protocol that's using client-preparing. See also MySQL documentations.
         *
         * @return this {@link Builder}
         * @since 0.8.1
         */
        public Builder useClientPrepareStatement() {
            this.preferPrepareStatement = null;
            return this;
        }

        /**
         * Configures the protocol of parameterized statements to the binary protocol.
         * <p>
         * The binary protocol is compact protocol that's using server-preparing. See also MySQL documentations.
         *
         * @return {@link Builder this}
         * @since 0.8.1
         */
        public Builder useServerPrepareStatement() {
            return useServerPrepareStatement((sql) -> false);
        }

        /**
         * Configures the protocol of parameterized statements and prepare-preferred simple statements to the binary
         * protocol.
         * <p>
         * The {@code preferPrepareStatement} configures whether to prefer prepare execution on a statement-by-statement
         * basis (simple statements). The {@link Predicate} accepts the simple SQL query string and returns a boolean
         * flag indicating preference. {@code true} prepare-preferred, {@code false} prefers direct execution (text
         * protocol). Defaults to direct execution.
         * <p>
         * The binary protocol is compact protocol that's using server-preparing. See also MySQL documentations.
         *
         * @param preferPrepareStatement the above {@link Predicate}.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code preferPrepareStatement} is {@code null}.
         * @since 0.8.1
         */
        public Builder useServerPrepareStatement(Predicate<String> preferPrepareStatement) {
            requireNonNull(preferPrepareStatement, "preferPrepareStatement must not be null");

            this.preferPrepareStatement = preferPrepareStatement;
            return this;
        }

        /**
         * Configures the session variables, used to set session variables immediately after login. Default no session
         * variables to set.  It should be a list of key-value pairs. e.g.
         * {@code ["sql_mode='ANSI_QUOTES,STRICT_TRANS_TABLES'", "time_zone=00:00"]}.
         *
         * @param sessionVariables the session variables to set.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code sessionVariables} is {@code null}.
         * @since 1.1.2
         */
        public Builder sessionVariables(String... sessionVariables) {
            requireNonNull(sessionVariables, "sessionVariables must not be null");

            this.sessionVariables = InternalArrays.toImmutableList(sessionVariables);
            return this;
        }

        /**
         * <<<<<<< HEAD Configures the lock wait timeout.  Default to use the server-side default value.
         *
         * @param lockWaitTimeout the lock wait timeout, or {@code null} to use the server-side default value.
         * @return {@link Builder this}
         * @since 1.1.3
         */
        public Builder lockWaitTimeout(@Nullable Duration lockWaitTimeout) {
            this.lockWaitTimeout = lockWaitTimeout;
            return this;
        }

        /**
         * Configures the statement timeout.  Default to use the server-side default value.
         *
         * @param statementTimeout the statement timeout, or {@code null} to use the server-side default value.
         * @return {@link Builder this}
         * @since 1.1.3
         */
        public Builder statementTimeout(@Nullable Duration statementTimeout) {
            this.statementTimeout = statementTimeout;
            return this;
        }

        /**
         * Configures to allow the {@code LOAD DATA LOCAL INFILE} statement in the given {@code path} or disallow the
         * statement.  Default to {@code null} which means not allow the statement.
         *
         * @param path which parent path are allowed to load file data, {@code null} means not be allowed.
         * @return {@link Builder this}.
         * @throws java.nio.file.InvalidPathException if the string cannot be converted to a {@link Path}.
         * @since 1.1.0
         */
        public Builder allowLoadLocalInfileInPath(@Nullable String path) {
            this.loadLocalInfilePath = path == null ? null : Paths.get(path);

            return this;
        }

        /**
         * Configures the buffer size for {@code LOAD DATA LOCAL INFILE} statement.  Default to {@code 8192}.
         * <p>
         * It is used only if {@link #allowLoadLocalInfileInPath(String)} is set.
         *
         * @param localInfileBufferSize the buffer size.
         * @return {@link Builder this}.
         * @throws IllegalArgumentException if {@code localInfileBufferSize} is not positive.
         * @since 1.1.0
         */
        public Builder localInfileBufferSize(int localInfileBufferSize) {
            require(localInfileBufferSize > 0, "localInfileBufferSize must be positive");

            this.localInfileBufferSize = localInfileBufferSize;
            return this;
        }

        /**
         * Configures the maximum size of the {@link Query} parsing cache. Usually it should be power of two. Default to
         * {@code 0}. Driver will use unbounded cache if size is less than {@code 0}.
         * <p>
         * Notice: the cache is using EL model (the PACELC theorem) which provider better performance. That means it is
         * an elastic cache. So this size is not a hard-limit. It should be over 16 in average.
         *
         * @param queryCacheSize the above size, {@code 0} means no cache, {@code -1} means unbounded cache.
         * @return {@link Builder this}
         * @since 0.8.3
         */
        public Builder queryCacheSize(int queryCacheSize) {
            this.queryCacheSize = queryCacheSize;
            return this;
        }

        /**
         * Configures the maximum size of the server-preparing cache. Usually it should be power of two. Default to
         * {@code 256}. Driver will use unbounded cache if size is less than {@code 0}. It is used only if using
         * server-preparing parameterized statements, i.e. the {@link #useServerPrepareStatement} is set.
         * <p>
         * Notice: the cache is using EC model (the PACELC theorem) for ensure consistency. Consistency is very
         * important because MySQL contains a hard limit of all server-prepared statements which has been opened, see
         * also {@code max_prepared_stmt_count}. And, the cache is one-to-one connection, which means it will not work
         * on thread-concurrency.
         *
         * @param prepareCacheSize the above size, {@code 0} means no cache, {@code -1} means unbounded cache.
         * @return {@link Builder this}
         * @since 0.8.3
         */
        public Builder prepareCacheSize(int prepareCacheSize) {
            this.prepareCacheSize = prepareCacheSize;
            return this;
        }

        /**
         * Configures the compression algorithms.  Default to [{@link CompressionAlgorithm#UNCOMPRESSED}].
         * <p>
         * It will auto choose an algorithm that's contained in the list and supported by the server, preferring zstd,
         * then zlib. If the list does not contain {@link CompressionAlgorithm#UNCOMPRESSED} and the server does not
         * support any algorithm in the list, an exception will be thrown when connecting.
         * <p>
         * Note: zstd requires a dependency {@code com.github.luben:zstd-jni}.
         *
         * @param compressionAlgorithms the list of compression algorithms.
         * @return {@link Builder this}.
         * @throws IllegalArgumentException if {@code compressionAlgorithms} is {@code null} or empty.
         * @since 1.1.2
         */
        public Builder compressionAlgorithms(CompressionAlgorithm... compressionAlgorithms) {
            requireNonNull(compressionAlgorithms, "compressionAlgorithms must not be null");
            require(compressionAlgorithms.length != 0, "compressionAlgorithms must not be empty");

            if (compressionAlgorithms.length == 1) {
                requireNonNull(compressionAlgorithms[0], "compressionAlgorithms must not contain null");
                this.compressionAlgorithms = Collections.singleton(compressionAlgorithms[0]);
            } else {
                Set<CompressionAlgorithm> algorithms = EnumSet.noneOf(CompressionAlgorithm.class);

                for (CompressionAlgorithm algorithm : compressionAlgorithms) {
                    requireNonNull(algorithm, "compressionAlgorithms must not contain null");
                    algorithms.add(algorithm);
                }

                this.compressionAlgorithms = algorithms;
            }

            return this;
        }

        /**
         * Configures the zstd compression level.  Default to {@code 3}.
         * <p>
         * It is only used if zstd is chosen for the connection.
         * <p>
         * Note: MySQL protocol does not allow to set the zlib compression level of the server, only zstd is
         * configurable.
         *
         * @param level the compression level.
         * @return {@link Builder this}.
         * @throws IllegalArgumentException if {@code level} is not between 1 and 22.
         * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/connection-options.html">
         * MySQL Connection Options --zstd-compression-level</a>
         * @since 1.1.2
         */
        public Builder zstdCompressionLevel(int level) {
            require(level >= 1 && level <= 22, "level must be between 1 and 22");

            this.zstdCompressionLevel = level;
            return this;
        }

        /**
         * Configures the {@link LoopResources} for the driver. Default to
         * {@link TcpResources#get() global tcp resources}.
         *
         * @param loopResources the {@link LoopResources}.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code loopResources} is {@code null}.
         * @since 1.1.2
         */
        public Builder loopResources(LoopResources loopResources) {
            this.client.loopResources(loopResources);
            return this;
        }

        /**
         * Configures whether to use {@link ServiceLoader} to discover and register extensions. Defaults to
         * {@code true}.
         *
         * @param enabled to discover and register extensions.
         * @return {@link Builder this}
         * @since 0.8.2
         */
        public Builder autodetectExtensions(boolean enabled) {
            this.autodetectExtensions = enabled;
            return this;
        }

        /**
         * Registers a {@link Extension} to extend driver functionality and manually.
         * <p>
         * Notice: the driver will not deduplicate {@link Extension}s of autodetect discovered and manually extended. So
         * if a {@link Extension} is registered by this function and autodetect discovered, it will get two
         * {@link Extension} as same.
         *
         * @param extension extension to extend driver functionality.
         * @return {@link Builder this}
         * @throws IllegalArgumentException if {@code extension} is {@code null}.
         * @since 0.8.2
         */
        public Builder extendWith(Extension extension) {
            this.extensions.add(requireNonNull(extension, "extension must not be null"));
            return this;
        }

        /**
         * Registers a password publisher function.  Since 1.2.0, it is an alias of {@link #password(Publisher)}.
         *
         * @param password a {@link Publisher} to retrieve password before making connection.
         * @return {@link Builder this}
         */
        public Builder passwordPublisher(Publisher<? extends CharSequence> password) {
            return password(password);
        }

        private TcpSocketConfiguration.Builder requireTcpSocket() {
            TcpSocketConfiguration.Builder tcpSocket = this.tcpSocket;

            if (tcpSocket == null) {
                this.tcpSocket = tcpSocket = new TcpSocketConfiguration.Builder();
            }

            return tcpSocket;
        }

        private UnixDomainSocketConfiguration.Builder requireUnixSocket() {
            UnixDomainSocketConfiguration.Builder unixSocket = this.unixSocket;

            if (unixSocket == null) {
                this.unixSocket = unixSocket = new UnixDomainSocketConfiguration.Builder();
            }

            return unixSocket;
        }

        private Builder() {
        }
    }
}
