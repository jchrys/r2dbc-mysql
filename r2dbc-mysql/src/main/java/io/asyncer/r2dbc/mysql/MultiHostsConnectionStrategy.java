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
import io.asyncer.r2dbc.mysql.client.FailoverClient;
import io.asyncer.r2dbc.mysql.client.ReactorNettyClient;
import io.asyncer.r2dbc.mysql.constant.ProtocolDriver;
import io.asyncer.r2dbc.mysql.internal.NodeAddress;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.NameResolver;
import io.netty.util.concurrent.Future;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * An abstraction for {@link ConnectionStrategy} that consider multiple hosts.
 */
final class MultiHostsConnectionStrategy implements ConnectionStrategy {

    private final Mono<? extends Client> client;

    MultiHostsConnectionStrategy(
        MySqlConnectionConfiguration configuration,
        List<NodeAddress> addresses,
        ProtocolDriver driver,
        int retriesAllDown,
        boolean shuffle,
        boolean tcpKeepAlive,
        boolean tcpNoDelay
    ) {
        Mono<ReactorNettyClient> client = configuration.getCredential().flatMap(credential -> {
            if (ProtocolDriver.DNS_SRV.equals(driver)) {
                logger.debug("Resolve hosts via DNS SRV: {}", addresses);

                LoopResources resources = configuration.getClient().getLoopResources();
                LoopResources loopResources = resources == null ? TcpResources.get() : resources;
                InetConnectFunction login = new InetConnectFunction(
                    false,
                    tcpKeepAlive,
                    tcpNoDelay,
                    credential,
                    configuration
                );

                return resolveAllHosts(loopResources, addresses, shuffle).flatMap(addrs -> {
                    logger.debug("Connect to multiple addresses: {}", addrs);

                    return connectHost(
                        addrs,
                        login,
                        shuffle,
                        0,
                        retriesAllDown
                    );
                });
            } else {
                List<NodeAddress> availableHosts = copyAvailableAddresses(addresses, shuffle);
                logger.debug("Connect to multiple hosts: {}", availableHosts);

                int size = availableHosts.size();
                InetSocketAddress[] array = new InetSocketAddress[availableHosts.size()];

                for (int i = 0; i < size; i++) {
                    array[i] = availableHosts.get(i).toUnresolved();
                }

                List<InetSocketAddress> addrs = InternalArrays.asImmutableList(array);
                InetConnectFunction login = new InetConnectFunction(
                    true,
                    tcpKeepAlive,
                    tcpNoDelay,
                    credential,
                    configuration
                );

                return connectHost(
                    addrs,
                    login,
                    shuffle,
                    0,
                    retriesAllDown
                );
            }
        });

        this.client = client.map(c -> new FailoverClient(c, client));
    }

    @Override
    public Mono<? extends Client> connect() {
        return client;
    }

    private static Mono<ReactorNettyClient> connectHost(
        List<InetSocketAddress> addresses,
        InetConnectFunction login,
        boolean shuffle,
        int attempts,
        int maxAttempts
    ) {
        Iterator<InetSocketAddress> iter = addresses.iterator();

        if (!iter.hasNext()) {
            return Mono.error(ConnectionStrategy.retryFail("Fail to establish connection: no available host", null));
        }


        InetSocketAddress address = iter.next();

        return login.apply(() -> address).onErrorResume(error -> resumeConnect(
            error,
            address,
            addresses,
            iter,
            login,
            shuffle,
            attempts,
            maxAttempts
        ));
    }

    private static Mono<ReactorNettyClient> resumeConnect(
        Throwable t,
        InetSocketAddress failed,
        List<InetSocketAddress> addresses,
        Iterator<InetSocketAddress> iter,
        InetConnectFunction login,
        boolean shuffle,
        int attempts,
        int maxAttempts
    ) {
        logger.warn("Fail to connect to {}", failed, t);

        if (!iter.hasNext()) {
            // The last host failed to connect
            if (attempts >= maxAttempts) {
                return Mono.error(ConnectionStrategy.retryFail(
                    "Fail to establish connections, retried " + attempts + " times", t));
            }

            logger.warn("All hosts failed to establish connections, auto-try again after 250ms.", t);

            // Ignore waiting error, e.g. interrupted, scheduler rejected
            return Mono.delay(Duration.ofMillis(250))
                .onErrorComplete()
                .then(Mono.defer(() -> connectHost(
                    addresses,
                    login,
                    shuffle,
                    attempts + 1,
                    maxAttempts
                )));
        }

        InetSocketAddress address = iter.next();

        return login.apply(() -> address).onErrorResume(error -> resumeConnect(
            error,
            address,
            addresses,
            iter,
            login,
            shuffle,
            attempts,
            maxAttempts
        ));
    }

    private static Mono<List<InetSocketAddress>> resolveAllHosts(
        LoopResources loopResources,
        List<NodeAddress> addresses,
        boolean shuffle
    ) {
        // Or DnsNameResolver? It is non-blocking but requires native dependencies, hard configurations, and maybe
        // behaves differently. Currently, we use DefaultNameResolver which is blocking but simple and easy to use.
        @SuppressWarnings("resource")
        DefaultNameResolver resolver = new DefaultNameResolver(loopResources.onClient(true).next());

        return Flux.fromIterable(addresses)
            .flatMap(address -> resolveAll(resolver, address.getHost())
                .flatMapIterable(Function.identity())
                .map(inet -> new InetSocketAddress(inet, address.getPort())))
            .doFinally(ignore -> resolver.close())
            .collectList()
            .map(list -> {
                if (shuffle) {
                    Collections.shuffle(list);
                }

                return list;
            });
    }

    private static Mono<List<InetAddress>> resolveAll(NameResolver<InetAddress> resolver, String host) {
        Future<List<InetAddress>> future = resolver.resolveAll(host);

        return Mono.<List<InetAddress>>create(sink -> future.addListener(f -> {
            if (f.isSuccess()) {
                try {
                    @SuppressWarnings("unchecked")
                    List<InetAddress> t = (List<InetAddress>) f.getNow();

                    logger.debug("Resolve {} in DNS succeed, {} records", host, t.size());
                    sink.success(t);
                } catch (Throwable e) {
                    logger.warn("Resolve {} in DNS succeed but failed to get result", host, e);
                    sink.success(Collections.emptyList());
                }
            } else {
                logger.warn("Resolve {} in DNS failed", host, f.cause());
                sink.success(Collections.emptyList());
            }
        })).doOnCancel(() -> future.cancel(false));
    }

    private static List<NodeAddress> copyAvailableAddresses(List<NodeAddress> addresses, boolean shuffle) {
        if (shuffle) {
            List<NodeAddress> copied = new ArrayList<>(addresses);
            Collections.shuffle(copied);
            return copied;
        }

        return InternalArrays.asImmutableList(addresses.toArray(new NodeAddress[0]));
    }
}
