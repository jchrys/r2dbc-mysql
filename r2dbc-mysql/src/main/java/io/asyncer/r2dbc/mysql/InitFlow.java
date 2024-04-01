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

import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.authentication.MySqlAuthProvider;
import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.client.FluxExchangeable;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.codec.CodecsBuilder;
import io.asyncer.r2dbc.mysql.constant.CompressionAlgorithm;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.extension.CodecRegistrar;
import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.asyncer.r2dbc.mysql.message.client.AuthResponse;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.HandshakeResponse;
import io.asyncer.r2dbc.mysql.message.client.InitDbMessage;
import io.asyncer.r2dbc.mysql.message.client.SslRequest;
import io.asyncer.r2dbc.mysql.message.client.SubsequenceClientMessage;
import io.asyncer.r2dbc.mysql.message.server.AuthMoreDataMessage;
import io.asyncer.r2dbc.mysql.message.server.ChangeAuthMessage;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.asyncer.r2dbc.mysql.message.server.ErrorMessage;
import io.asyncer.r2dbc.mysql.message.server.HandshakeHeader;
import io.asyncer.r2dbc.mysql.message.server.HandshakeRequest;
import io.asyncer.r2dbc.mysql.message.server.OkMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.asyncer.r2dbc.mysql.message.server.SyntheticSslResponseMessage;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.Readable;
import org.jetbrains.annotations.Nullable;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;
import reactor.util.concurrent.Queues;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A message flow utility that can initializes the session of {@link Client}.
 * <p>
 * It should not use server-side prepared statements, because {@link PrepareCache} will be initialized after the session
 * is initialized.
 */
final class InitFlow {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(InitFlow.class);

    private static final ServerVersion MARIA_11_1_1 = ServerVersion.create(11, 1, 1, true);

    private static final ServerVersion MYSQL_8_0_3 = ServerVersion.create(8, 0, 3);

    private static final ServerVersion MYSQL_5_7_20 = ServerVersion.create(5, 7, 20);

    private static final ServerVersion MYSQL_8 = ServerVersion.create(8, 0, 0);

    private static final BiConsumer<ServerMessage, SynchronousSink<Boolean>> INIT_DB = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            ErrorMessage msg = (ErrorMessage) message;
            logger.debug("Use database failed: [{}] [{}] {}", msg.getCode(), msg.getSqlState(), msg.getMessage());
            sink.next(false);
            sink.complete();
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            sink.next(true);
            sink.complete();
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    private static final BiConsumer<ServerMessage, SynchronousSink<Void>> INIT_DB_AFTER = (message, sink) -> {
        if (message instanceof ErrorMessage) {
            sink.error(((ErrorMessage) message).toException());
        } else if (message instanceof CompleteMessage && ((CompleteMessage) message).isDone()) {
            sink.complete();
        } else {
            ReferenceCountUtil.safeRelease(message);
        }
    };

    /**
     * Initializes handshake and login a {@link Client}.
     *
     * @param client                the {@link Client} to exchange messages with.
     * @param sslMode               the {@link SslMode} defines SSL capability and behavior.
     * @param database              the database that will be connected.
     * @param user                  the user that will be login.
     * @param password              the password of the {@code user}.
     * @param compressionAlgorithms the list of compression algorithms.
     * @param zstdCompressionLevel  the zstd compression level.
     * @return a {@link Mono} that indicates the initialization is done, or an error if the initialization failed.
     */
    static Mono<Void> initHandshake(Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password, Set<CompressionAlgorithm> compressionAlgorithms, int zstdCompressionLevel) {
        return client.exchange(new HandshakeExchangeable(
            client,
            sslMode,
            database,
            user,
            password,
            compressionAlgorithms,
            zstdCompressionLevel
        )).then();
    }

    /**
     * Initializes the session and {@link Codecs} of a {@link Client}.
     *
     * @param client           the client
     * @param database         the database to use after session initialization
     * @param prepareCacheSize the size of prepare cache
     * @param sessionVariables the session variables to set
     * @param forceTimeZone    if the timezone should be set to session
     * @param lockWaitTimeout  the lock wait timeout that should be set to session
     * @param statementTimeout the statement timeout that should be set to session
     * @return a {@link Mono} that indicates the {@link Codecs}, or an error if the initialization failed
     */
    static Mono<Codecs> initSession(
        Client client,
        String database,
        int prepareCacheSize,
        List<String> sessionVariables,
        boolean forceTimeZone,
        @Nullable Duration lockWaitTimeout,
        @Nullable Duration statementTimeout,
        Extensions extensions
    ) {
        return Mono.defer(() -> {
            ByteBufAllocator allocator = client.getByteBufAllocator();
            CodecsBuilder builder = Codecs.builder();

            extensions.forEach(CodecRegistrar.class, registrar ->
                registrar.register(allocator, builder));

            Codecs codecs = builder.build();

            List<String> variables = mergeSessionVariables(client, sessionVariables, forceTimeZone, statementTimeout);

            logger.debug("Initializing client session: {}", variables);

            return QueryFlow.setSessionVariables(client, variables)
                .then(loadSessionVariables(client, codecs))
                .flatMap(data -> loadAndInitInnoDbEngineStatus(data, client, codecs, lockWaitTimeout))
                .flatMap(data -> {
                    ConnectionContext context = client.getContext();

                    logger.debug("Initializing connection {} context: {}", context.getConnectionId(), data);
                    context.initSession(
                        Caches.createPrepareCache(prepareCacheSize),
                        data.level,
                        data.lockWaitTimeoutSupported,
                        data.lockWaitTimeout,
                        data.product,
                        data.timeZone
                    );

                    if (!data.lockWaitTimeoutSupported) {
                        logger.info(
                            "Lock wait timeout is not supported by server, all related operations will be ignored");
                    }

                    return database.isEmpty() ? Mono.just(codecs) :
                        initDatabase(client, database).then(Mono.just(codecs));
                });
        });
    }

    private static Mono<SessionState> loadAndInitInnoDbEngineStatus(
        SessionState data,
        Client client,
        Codecs codecs,
        @Nullable Duration lockWaitTimeout
    ) {
        return new TextSimpleStatement(client, codecs, "SHOW VARIABLES LIKE 'innodb\\\\_lock\\\\_wait\\\\_timeout'")
            .execute()
            .flatMap(r -> r.map(readable -> {
                String value = readable.get(1, String.class);

                if (value == null || value.isEmpty()) {
                    return data;
                } else {
                    return data.lockWaitTimeout(Duration.ofSeconds(Long.parseLong(value)));
                }
            }))
            .single(data)
            .flatMap(d -> {
                if (lockWaitTimeout != null) {
                    // Do not use context.isLockWaitTimeoutSupported() here, because its session variable is not set
                    if (d.lockWaitTimeoutSupported) {
                        return QueryFlow.executeVoid(client, StringUtils.lockWaitTimeoutStatement(lockWaitTimeout))
                            .then(Mono.fromSupplier(() -> d.lockWaitTimeout(lockWaitTimeout)));
                    }

                    logger.warn("Lock wait timeout is not supported by server, ignore initial setting");
                    return Mono.just(d);
                }
                return Mono.just(d);
            });
    }

    private static Mono<SessionState> loadSessionVariables(Client client, Codecs codecs) {
        ConnectionContext context = client.getContext();
        StringBuilder query = new StringBuilder(128)
            .append("SELECT ")
            .append(transactionIsolationColumn(context))
            .append(",@@version_comment AS v");

        Function<MySqlResult, Flux<SessionState>> handler;

        if (context.isTimeZoneInitialized()) {
            handler = r -> convertSessionData(r, false);
        } else {
            query.append(",@@system_time_zone AS s,@@time_zone AS t");
            handler = r -> convertSessionData(r, true);
        }

        return new TextSimpleStatement(client, codecs, query.toString())
            .execute()
            .flatMap(handler)
            .last();
    }

    private static Mono<Void> initDatabase(Client client, String database) {
        return client.exchange(new InitDbMessage(database), INIT_DB)
            .last()
            .flatMap(success -> {
                if (success) {
                    return Mono.empty();
                }

                String sql = "CREATE DATABASE IF NOT EXISTS " + StringUtils.quoteIdentifier(database);

                return QueryFlow.executeVoid(client, sql)
                    .then(client.exchange(new InitDbMessage(database), INIT_DB_AFTER).then());
            });
    }

    private static List<String> mergeSessionVariables(
        Client client,
        List<String> sessionVariables,
        boolean forceTimeZone,
        @Nullable Duration statementTimeout
    ) {
        ConnectionContext context = client.getContext();

        if ((!forceTimeZone || !context.isTimeZoneInitialized()) && statementTimeout == null) {
            return sessionVariables;
        }

        List<String> variables = new ArrayList<>(sessionVariables.size() + 2);

        variables.addAll(sessionVariables);

        if (forceTimeZone && context.isTimeZoneInitialized()) {
            variables.add(timeZoneVariable(context.getTimeZone()));
        }

        if (statementTimeout != null) {
            if (context.isStatementTimeoutSupported()) {
                variables.add(StringUtils.statementTimeoutVariable(statementTimeout, context.isMariaDb()));
            } else {
                logger.warn("Statement timeout is not supported in {}, ignore initial setting",
                    context.getServerVersion());
            }
        }

        return variables;
    }

    private static String timeZoneVariable(ZoneId timeZone) {
        String offerStr = timeZone instanceof ZoneOffset && "Z".equalsIgnoreCase(timeZone.getId()) ?
            "+00:00" : timeZone.getId();

        return "time_zone='" + offerStr + "'";
    }

    private static Flux<SessionState> convertSessionData(MySqlResult r, boolean timeZone) {
        return r.map(readable -> {
            IsolationLevel level = convertIsolationLevel(readable.get(0, String.class));
            String product = readable.get(1, String.class);

            return new SessionState(level, product, timeZone ? readZoneId(readable) : null);
        });
    }

    /**
     * Resolves the column of session isolation level, the {@literal @@tx_isolation} has been marked as deprecated.
     * <p>
     * If server is MariaDB, {@literal @@transaction_isolation} is used starting from {@literal 11.1.1}.
     * <p>
     * If the server is MySQL, use {@literal @@transaction_isolation} starting from {@literal 8.0.3}, or between
     * {@literal 5.7.20} and {@literal 8.0.0} (exclusive).
     */
    private static String transactionIsolationColumn(ConnectionContext context) {
        ServerVersion version = context.getServerVersion();

        if (context.isMariaDb()) {
            return version.isGreaterThanOrEqualTo(MARIA_11_1_1) ? "@@transaction_isolation AS i" :
                "@@tx_isolation AS i";
        }

        return version.isGreaterThanOrEqualTo(MYSQL_8_0_3) ||
            (version.isGreaterThanOrEqualTo(MYSQL_5_7_20) && version.isLessThan(MYSQL_8)) ?
            "@@transaction_isolation AS i" : "@@tx_isolation AS i";
    }

    private static ZoneId readZoneId(Readable readable) {
        String systemTimeZone = readable.get(2, String.class);
        String timeZone = readable.get(3, String.class);

        if (timeZone == null || timeZone.isEmpty() || "SYSTEM".equalsIgnoreCase(timeZone)) {
            if (systemTimeZone == null || systemTimeZone.isEmpty()) {
                logger.warn("MySQL does not return any timezone, trying to use system default timezone");
                return ZoneId.systemDefault().normalized();
            } else {
                return convertZoneId(systemTimeZone);
            }
        } else {
            return convertZoneId(timeZone);
        }
    }

    private static ZoneId convertZoneId(String id) {
        try {
            return StringUtils.parseZoneId(id);
        } catch (DateTimeException e) {
            logger.warn("The server timezone is unknown <{}>, trying to use system default timezone", id, e);

            return ZoneId.systemDefault().normalized();
        }
    }

    private static IsolationLevel convertIsolationLevel(@Nullable String name) {
        if (name == null) {
            logger.warn("Isolation level is null in current session, fallback to repeatable read");

            return IsolationLevel.REPEATABLE_READ;
        }

        switch (name) {
            case "READ-UNCOMMITTED":
                return IsolationLevel.READ_UNCOMMITTED;
            case "READ-COMMITTED":
                return IsolationLevel.READ_COMMITTED;
            case "REPEATABLE-READ":
                return IsolationLevel.REPEATABLE_READ;
            case "SERIALIZABLE":
                return IsolationLevel.SERIALIZABLE;
        }

        logger.warn("Unknown isolation level {} in current session, fallback to repeatable read", name);

        return IsolationLevel.REPEATABLE_READ;
    }

    private InitFlow() {
    }

    private static final class SessionState {

        private final IsolationLevel level;

        @Nullable
        private final String product;

        @Nullable
        private final ZoneId timeZone;

        private final Duration lockWaitTimeout;

        private final boolean lockWaitTimeoutSupported;

        SessionState(IsolationLevel level, @Nullable String product, @Nullable ZoneId timeZone) {
            this(level, product, timeZone, Duration.ZERO, false);
        }

        private SessionState(
            IsolationLevel level,
            @Nullable String product,
            @Nullable ZoneId timeZone,
            Duration lockWaitTimeout,
            boolean lockWaitTimeoutSupported
        ) {
            this.level = level;
            this.product = product;
            this.timeZone = timeZone;
            this.lockWaitTimeout = lockWaitTimeout;
            this.lockWaitTimeoutSupported = lockWaitTimeoutSupported;
        }

        SessionState lockWaitTimeout(Duration timeout) {
            return new SessionState(level, product, timeZone, timeout, true);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SessionState)) {
                return false;
            }

            SessionState that = (SessionState) o;

            return lockWaitTimeoutSupported == that.lockWaitTimeoutSupported &&
                level.equals(that.level) &&
                Objects.equals(product, that.product) &&
                Objects.equals(timeZone, that.timeZone) &&
                lockWaitTimeout.equals(that.lockWaitTimeout);
        }

        @Override
        public int hashCode() {
            int result = level.hashCode();
            result = 31 * result + (product != null ? product.hashCode() : 0);
            result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
            result = 31 * result + lockWaitTimeout.hashCode();
            return 31 * result + (lockWaitTimeoutSupported ? 1 : 0);
        }

        @Override
        public String toString() {
            return "SessionState{level=" + level +
                ", product='" + product +
                "', timeZone=" + timeZone +
                ", lockWaitTimeout=" + lockWaitTimeout +
                ", lockWaitTimeoutSupported=" + lockWaitTimeoutSupported +
                '}';
        }
    }
}

/**
 * An implementation of {@link FluxExchangeable} that considers login to the database.
 * <p>
 * Not like other {@link FluxExchangeable}s, it is started by a server-side message, which should be an implementation
 * of {@link HandshakeRequest}.
 */
final class HandshakeExchangeable extends FluxExchangeable<Void> {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(HandshakeExchangeable.class);

    private static final Map<String, String> ATTRIBUTES = Collections.emptyMap();

    private static final String CLI_SPECIFIC = "HY000";

    private static final int HANDSHAKE_VERSION = 10;

    private final Sinks.Many<SubsequenceClientMessage> requests = Sinks.many().unicast()
        .onBackpressureBuffer(Queues.<SubsequenceClientMessage>one().get());

    private final Client client;

    private final SslMode sslMode;

    private final String database;

    private final String user;

    @Nullable
    private final CharSequence password;

    private final Set<CompressionAlgorithm> compressions;

    private final int zstdCompressionLevel;

    private boolean handshake = true;

    private MySqlAuthProvider authProvider;

    private byte[] salt;

    private boolean sslCompleted;

    HandshakeExchangeable(Client client, SslMode sslMode, String database, String user,
        @Nullable CharSequence password, Set<CompressionAlgorithm> compressions,
        int zstdCompressionLevel) {
        this.client = client;
        this.sslMode = sslMode;
        this.database = database;
        this.user = user;
        this.password = password;
        this.compressions = compressions;
        this.zstdCompressionLevel = zstdCompressionLevel;
        this.sslCompleted = sslMode == SslMode.TUNNEL;
    }

    @Override
    public void subscribe(CoreSubscriber<? super ClientMessage> actual) {
        requests.asFlux().subscribe(actual);
    }

    @Override
    public void accept(ServerMessage message, SynchronousSink<Void> sink) {
        if (message instanceof ErrorMessage) {
            sink.error(((ErrorMessage) message).toException());
            return;
        }

        // Ensures it will be initialized only once.
        if (handshake) {
            handshake = false;
            if (message instanceof HandshakeRequest) {
                HandshakeRequest request = (HandshakeRequest) message;
                Capability capability = initHandshake(request);

                if (capability.isSslEnabled()) {
                    emitNext(SslRequest.from(capability, client.getContext().getClientCollation().getId()), sink);
                } else {
                    emitNext(createHandshakeResponse(capability), sink);
                }
            } else {
                sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                    message.getClass().getSimpleName() + "' in init phase"));
            }

            return;
        }

        if (message instanceof OkMessage) {
            logger.trace("Connection (id {}) login success", client.getContext().getConnectionId());
            client.loginSuccess();
            sink.complete();
        } else if (message instanceof SyntheticSslResponseMessage) {
            sslCompleted = true;
            emitNext(createHandshakeResponse(client.getContext().getCapability()), sink);
        } else if (message instanceof AuthMoreDataMessage) {
            AuthMoreDataMessage msg = (AuthMoreDataMessage) message;

            if (msg.isFailed()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Connection (id {}) fast authentication failed, use full authentication",
                        client.getContext().getConnectionId());
                }

                emitNext(createAuthResponse("full authentication"), sink);
            }
            // Otherwise success, wait until OK message or Error message.
        } else if (message instanceof ChangeAuthMessage) {
            ChangeAuthMessage msg = (ChangeAuthMessage) message;

            authProvider = MySqlAuthProvider.build(msg.getAuthType());
            salt = msg.getSalt();
            emitNext(createAuthResponse("change authentication"), sink);
        } else {
            sink.error(new R2dbcPermissionDeniedException("Unexpected message type '" +
                message.getClass().getSimpleName() + "' in login phase"));
        }
    }

    @Override
    public void dispose() {
        // No particular error condition handling for complete signal.
        this.requests.tryEmitComplete();
    }

    private void emitNext(SubsequenceClientMessage message, SynchronousSink<Void> sink) {
        Sinks.EmitResult result = requests.tryEmitNext(message);

        if (result != Sinks.EmitResult.OK) {
            sink.error(new IllegalStateException("Fail to emit a login request due to " + result));
        }
    }

    private AuthResponse createAuthResponse(String phase) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(authFails(authProvider.getType(), phase), CLI_SPECIFIC);
        }

        return new AuthResponse(authProvider.authentication(password, salt, client.getContext().getClientCollation()));
    }

    private Capability clientCapability(Capability serverCapability) {
        Capability.Builder builder = serverCapability.mutate();

        builder.disableSessionTrack();
        builder.disableDatabasePinned();
        builder.disableIgnoreAmbiguitySpace();
        builder.disableInteractiveTimeout();

        if (sslMode == SslMode.TUNNEL) {
            // Tunnel does not use MySQL SSL protocol, disable it.
            builder.disableSsl();
        } else if (!serverCapability.isSslEnabled()) {
            // Server unsupported SSL.
            if (sslMode.requireSsl()) {
                // Before handshake, Client.context does not be initialized
                throw new R2dbcPermissionDeniedException("Server does not support SSL but mode '" + sslMode +
                    "' requires SSL", CLI_SPECIFIC);
            } else if (sslMode.startSsl()) {
                // SSL has start yet, and client can disable SSL, disable now.
                client.sslUnsupported();
            }
        } else {
            // The server supports SSL, but the user does not want to use SSL, disable it.
            if (!sslMode.startSsl()) {
                builder.disableSsl();
            }
        }

        if (isZstdAllowed(serverCapability)) {
            if (isZstdSupported()) {
                builder.disableZlibCompression();
            } else {
                logger.warn("Server supports zstd, but zstd-jni dependency is missing");

                if (isZlibAllowed(serverCapability)) {
                    builder.disableZstdCompression();
                } else if (compressions.contains(CompressionAlgorithm.UNCOMPRESSED)) {
                    builder.disableCompression();
                } else {
                    throw new R2dbcNonTransientResourceException(
                        "Environment does not support a compression algorithm in " + compressions +
                            ", config does not allow uncompressed mode", CLI_SPECIFIC);
                }
            }
        } else if (isZlibAllowed(serverCapability)) {
            builder.disableZstdCompression();
        } else if (compressions.contains(CompressionAlgorithm.UNCOMPRESSED)) {
            builder.disableCompression();
        } else {
            throw new R2dbcPermissionDeniedException(
                "Environment does not support a compression algorithm in " + compressions +
                    ", config does not allow uncompressed mode", CLI_SPECIFIC);
        }

        if (database.isEmpty()) {
            builder.disableConnectWithDatabase();
        }

        if (client.getContext().getLocalInfilePath() == null) {
            builder.disableLoadDataLocalInfile();
        }

        if (ATTRIBUTES.isEmpty()) {
            builder.disableConnectAttributes();
        }

        return builder.build();
    }

    private Capability initHandshake(HandshakeRequest message) {
        HandshakeHeader header = message.getHeader();
        int handshakeVersion = header.getProtocolVersion();
        ServerVersion serverVersion = header.getServerVersion();

        if (handshakeVersion < HANDSHAKE_VERSION) {
            logger.warn("MySQL use handshake V{}, server version is {}, maybe most features are unavailable",
                handshakeVersion, serverVersion);
        }

        Capability capability = clientCapability(message.getServerCapability());

        // No need initialize server statuses because it has initialized by read filter.
        this.client.getContext().initHandshake(header.getConnectionId(), serverVersion, capability);
        this.authProvider = MySqlAuthProvider.build(message.getAuthType());
        this.salt = message.getSalt();

        return capability;
    }

    private MySqlAuthProvider getAndNextProvider() {
        MySqlAuthProvider authProvider = this.authProvider;
        this.authProvider = authProvider.next();
        return authProvider;
    }

    private HandshakeResponse createHandshakeResponse(Capability capability) {
        MySqlAuthProvider authProvider = getAndNextProvider();

        if (authProvider.isSslNecessary() && !sslCompleted) {
            throw new R2dbcPermissionDeniedException(authFails(authProvider.getType(), "handshake"),
                CLI_SPECIFIC);
        }

        byte[] authorization = authProvider.authentication(password, salt, client.getContext().getClientCollation());
        String authType = authProvider.getType();

        if (MySqlAuthProvider.NO_AUTH_PROVIDER.equals(authType)) {
            // Authentication type is not matter because of it has no authentication type.
            // Server need send a Change Authentication Message after handshake response.
            authType = MySqlAuthProvider.CACHING_SHA2_PASSWORD;
        }

        return HandshakeResponse.from(capability, client.getContext().getClientCollation().getId(), user, authorization,
            authType, database, ATTRIBUTES, zstdCompressionLevel);
    }

    private boolean isZstdAllowed(Capability capability) {
        return capability.isZstdCompression() && compressions.contains(CompressionAlgorithm.ZSTD);
    }

    private boolean isZlibAllowed(Capability capability) {
        return capability.isZlibCompression() && compressions.contains(CompressionAlgorithm.ZLIB);
    }

    private static String authFails(String authType, String phase) {
        return "Authentication type '" + authType + "' must require SSL in " + phase + " phase";
    }

    private static boolean isZstdSupported() {
        try {
            ClassLoader loader = AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                return cl == null ? ClassLoader.getSystemClassLoader() : cl;
            });
            Class.forName("com.github.luben.zstd.Zstd", false, loader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
