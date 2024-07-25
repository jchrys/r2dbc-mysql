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
import io.asyncer.r2dbc.mysql.client.ReactorNettyClient;
import io.asyncer.r2dbc.mysql.internal.NodeAddress;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * An implementation of {@link ConnectionStrategy} that connects to a single host. It can be wrapped to a
 * FailoverClient.
 */
final class SingleHostConnectionStrategy implements ConnectionStrategy {

    private final Mono<Client> client;

    SingleHostConnectionStrategy(
        MySqlConnectionConfiguration configuration,
        NodeAddress address,
        boolean tcpKeepAlive,
        boolean tcpNoDelay
    ) {
        this.client = configuration.getCredential().flatMap(credential -> {
            logger.debug("Connect to a single host: {}", address);

            InetConnectFunction login = new InetConnectFunction(
                true,
                tcpKeepAlive,
                tcpNoDelay,
                credential,
                configuration
            );

            return connectHost(login, address, 0, 3);
        });
    }

    @Override
    public Mono<Client> connect() {
        return client;
    }

    private static Mono<ReactorNettyClient> connectHost(
        InetConnectFunction login,
        NodeAddress address,
        int attempts,
        int maxAttempts
    ) {
        return login.apply(address::toUnresolved)
            .onErrorResume(t -> resumeConnect(t, address, login, attempts, maxAttempts));
    }

    private static Mono<ReactorNettyClient> resumeConnect(
        Throwable t,
        NodeAddress address,
        InetConnectFunction login,
        int attempts,
        int maxAttempts
    ) {
        logger.warn("Fail to connect to {}", address, t);

        if (attempts >= maxAttempts) {
            return Mono.error(ConnectionStrategy.retryFail(
                "Fail to establish connection, retried " + attempts + " times", t));
        }

        logger.warn("Failed to establish connection, auto-try again after 250ms.", t);

        // Ignore waiting error, e.g. interrupted, scheduler rejected
        return Mono.delay(Duration.ofMillis(250))
            .onErrorComplete()
            .then(Mono.defer(() -> connectHost(
                login,
                address,
                attempts + 1,
                maxAttempts
            )));
    }
}
