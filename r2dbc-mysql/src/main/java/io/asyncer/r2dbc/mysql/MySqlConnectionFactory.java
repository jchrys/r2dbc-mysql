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

import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.cache.QueryCache;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link ConnectionFactory} for creating connections to a MySQL database.
 */
public final class MySqlConnectionFactory implements ConnectionFactory {

    private final Mono<? extends MySqlConnection> client;

    private MySqlConnectionFactory(Mono<? extends MySqlConnection> client) {
        this.client = client;
    }

    @Override
    public Mono<? extends MySqlConnection> create() {
        return client;
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return MySqlConnectionFactoryMetadata.INSTANCE;
    }

    /**
     * Creates a {@link MySqlConnectionFactory} with a {@link MySqlConnectionConfiguration}.
     *
     * @param configuration the {@link MySqlConnectionConfiguration}.
     * @return configured {@link MySqlConnectionFactory}.
     */
    public static MySqlConnectionFactory from(MySqlConnectionConfiguration configuration) {
        requireNonNull(configuration, "configuration must not be null");

        LazyQueryCache queryCache = new LazyQueryCache(configuration.getQueryCacheSize());

        return new MySqlConnectionFactory(Mono.defer(() -> configuration.getSocket()
            .strategy(configuration)
            .connect()
            .flatMap(client -> {
                String sessionDb = configuration.isCreateDatabaseIfNotExist() ? configuration.getDatabase() : "";

                return InitFlow.initSession(
                    client,
                    sessionDb,
                    configuration.getPrepareCacheSize(),
                    configuration.getSessionVariables(),
                    configuration.isForceConnectionTimeZoneToSession(),
                    configuration.getLockWaitTimeout(),
                    configuration.getStatementTimeout(),
                    configuration.getExtensions()
                ).map(codecs -> new MySqlSimpleConnection(
                    client,
                    codecs,
                    queryCache.get(),
                    configuration.getPreferPrepareStatement()
                )).onErrorResume(e -> client.close().then(Mono.error(e)));
            })));
    }

    private static final class LazyQueryCache implements Supplier<QueryCache> {

        private final int capacity;

        private final ReentrantLock lock = new ReentrantLock();

        @Nullable
        private volatile QueryCache cache;

        private LazyQueryCache(int capacity) {
            this.capacity = capacity;
        }

        @Override
        public QueryCache get() {
            QueryCache cache = this.cache;
            if (cache == null) {
                lock.lock();
                try {
                    if ((cache = this.cache) == null) {
                        this.cache = cache = Caches.createQueryCache(capacity);
                    }
                    return cache;
                } finally {
                    lock.unlock();
                }
            }
            return cache;
        }
    }
}
