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

import io.asyncer.r2dbc.mysql.client.Client;
import io.netty.channel.unix.DomainSocketAddress;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;

/**
 * An implementation of {@link ConnectionStrategy} that connects to a Unix Domain Socket.
 */
final class UnixDomainSocketConnectionStrategy implements ConnectionStrategy {

    private final Mono<Client> client;

    UnixDomainSocketConnectionStrategy(
        UnixDomainSocketConfiguration socket,
        MySqlConnectionConfiguration configuration
    ) {
        this.client = configuration.getCredential().flatMap(credential -> {
            String path = socket.getPath();
            TcpClient tcpClient = ConnectionStrategy.createTcpClient(configuration.getClient(), false)
                .remoteAddress(() -> new DomainSocketAddress(path));

            return ConnectionStrategy.connectWithInit(tcpClient, credential, configuration);
        });
    }

    @Override
    public Mono<Client> connect() {
        return client;
    }
}
