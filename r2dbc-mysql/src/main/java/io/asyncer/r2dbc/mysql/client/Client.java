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

package io.asyncer.r2dbc.mysql.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.MySqlSslConfiguration;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.server.ServerMessage;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.netty.tcp.TcpClient;

import java.util.function.BiConsumer;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An abstraction that wraps the networking part of exchanging methods.
 */
public interface Client {

    InternalLogger logger = InternalLoggerFactory.getInstance(Client.class);

    /**
     * Perform an exchange of a request message. Calling this method while a previous exchange is active will return a
     * deferred handle and queue the request until the previous exchange terminates.
     *
     * @param request one and only one request message for get server responses
     * @param handler response handler, {@link SynchronousSink#complete()} should be called after the last response
     *                frame is sent to complete the stream and prevent multiple subscribers from consuming previous,
     *                active response streams
     * @param <T>     handling response type
     * @return A {@link Flux} of incoming messages that ends with the end of the frame
     */
    <T> Flux<T> exchange(ClientMessage request, BiConsumer<ServerMessage, SynchronousSink<T>> handler);

    /**
     * Perform an exchange of multi-request messages. Calling this method while a previous exchange is active will
     * return a deferred handle and queue the request until the previous exchange terminates.
     *
     * @param exchangeable request messages and response handler
     * @param <T>          handling response type
     * @return A {@link Flux} of incoming messages that ends with the end of the frame
     */
    <T> Flux<T> exchange(FluxExchangeable<T> exchangeable);

    /**
     * Close the connection of the {@link Client} with close request.
     * <p>
     * Notice: should not use it before connection login phase.
     *
     * @return A {@link Mono} that will emit a complete signal after connection closed
     */
    Mono<Void> close();

    /**
     * Force close the connection of the {@link Client}. It is useful when login phase emit an error.
     *
     * @return A {@link Mono} that will emit a complete signal after connection closed
     */
    Mono<Void> forceClose();

    /**
     * Returns the {@link ByteBufAllocator}.
     *
     * @return the {@link ByteBufAllocator}
     */
    ByteBufAllocator getByteBufAllocator();

    /**
     * Returns the current {@link ConnectionContext}. It should not be retained long-term as it may change on reconnects
     * or redirects.
     *
     * @return the {@link ConnectionContext}
     */
    ConnectionContext getContext();

    /**
     * Checks if the connection is open.
     *
     * @return if connection is valid
     */
    boolean isConnected();

    /**
     * Sends a signal to the connection, which means server does not support SSL.
     */
    void sslUnsupported();

    /**
     * Sends a signal to {@link Client this}, which means login has succeeded.
     */
    void loginSuccess();

    /**
     * Connects to a MySQL server using the provided {@link TcpClient} and {@link MySqlSslConfiguration}.
     *
     * @param tcpClient the configured TCP client
     * @param ssl       the SSL configuration
     * @param context   the connection context
     * @return A {@link Mono} that will emit a connected {@link Client}.
     * @throws IllegalArgumentException if {@code tcpClient}, {@code ssl} or {@code context} is {@code null}.
     */
    static Mono<Client> connect(TcpClient tcpClient, MySqlSslConfiguration ssl, ConnectionContext context) {
        requireNonNull(tcpClient, "tcpClient must not be null");
        requireNonNull(ssl, "ssl must not be null");
        requireNonNull(context, "context must not be null");

        return tcpClient.connect().map(conn -> new ReactorNettyClient(conn, ssl, context));
    }
}
