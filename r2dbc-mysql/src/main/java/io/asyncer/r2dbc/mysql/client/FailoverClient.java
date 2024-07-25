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

package io.asyncer.r2dbc.mysql.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * An implementation of {@link Client} that supports failover.
 */
public final class FailoverClient implements Client {

    private final Mono<ReactorNettyClient> failover;

    private final AtomicReference<ReactorNettyClient> client;

    public FailoverClient(ReactorNettyClient client, Mono<ReactorNettyClient> failover) {
        this.client = new AtomicReference<>(client);
        this.failover = failover;
    }

    private Mono<ReactorNettyClient> reconnectIfNecessary() {
        return Mono.defer(() -> {
            ReactorNettyClient client = this.client.get();

            if (client.isChannelOpen() || client.isClosingOrClosed()) {
                // Open, or closed by user
                return Mono.just(client);
            }

            return this.failover.flatMap(c -> {
                if (this.client.compareAndSet(client, c)) {
                    // TODO: re-init session variables, transaction state, clear prepared cache, etc.
                    return Mono.just(c);
                }

                // Reconnected by other thread, close this one and retry
                return c.forceClose().then(reconnectIfNecessary());
            });
        });
    }

    @Override
    public <T> Flux<T> exchange(ClientMessage request, BiConsumer<ServerMessage, SynchronousSink<T>> handler) {
        return reconnectIfNecessary().flatMapMany(c -> c.exchange(request, handler));
    }

    @Override
    public <T> Flux<T> exchange(FluxExchangeable<T> exchangeable) {
        return reconnectIfNecessary().flatMapMany(c -> c.exchange(exchangeable));
    }

    @Override
    public Mono<Void> close() {
        return Mono.fromSupplier(this.client::get).flatMap(ReactorNettyClient::close);
    }

    @Override
    public Mono<Void> forceClose() {
        return Mono.fromSupplier(this.client::get).flatMap(ReactorNettyClient::forceClose);
    }

    @Override
    public ByteBufAllocator getByteBufAllocator() {
        return this.client.get().getByteBufAllocator();
    }

    @Override
    public ConnectionContext getContext() {
        return this.client.get().getContext();
    }

    @Override
    public boolean isConnected() {
        return this.client.get().isConnected();
    }
}
