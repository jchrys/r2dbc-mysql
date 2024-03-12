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
import io.asyncer.r2dbc.mysql.constant.ProtocolDriver;
import io.asyncer.r2dbc.mysql.internal.NodeAddress;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.netty.channel.ChannelOption;
import io.netty.resolver.DefaultNameResolver;
import io.netty.resolver.NameResolver;
import io.netty.util.concurrent.Future;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpClient;
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

    private final Mono<Client> client;

    MultiHostsConnectionStrategy(
        TcpSocketConfiguration tcp,
        MySqlConnectionConfiguration configuration,
        boolean shuffle
    ) {
        this.client = Mono.defer(() -> {
            if (ProtocolDriver.DNS_SRV.equals(tcp.getDriver())) {
                LoopResources resources = configuration.getClient().getLoopResources();
                LoopResources loopResources = resources == null ? TcpResources.get() : resources;

                return resolveAllHosts(loopResources, tcp.getAddresses(), shuffle)
                    .flatMap(addresses -> connectHost(addresses, tcp, configuration, false, shuffle, 0));
            } else {
                List<NodeAddress> availableHosts = copyAvailableAddresses(tcp.getAddresses(), shuffle);
                int size = availableHosts.size();
                InetSocketAddress[] addresses = new InetSocketAddress[availableHosts.size()];

                for (int i = 0; i < size; i++) {
                    NodeAddress address = availableHosts.get(i);
                    addresses[i] = InetSocketAddress.createUnresolved(address.getHost(), address.getPort());
                }

                return connectHost(InternalArrays.asImmutableList(addresses), tcp, configuration, true, shuffle, 0);
            }
        });
    }

    @Override
    public Mono<Client> connect() {
        return client;
    }

    private Mono<Client> connectHost(
        List<InetSocketAddress> addresses,
        TcpSocketConfiguration tcp,
        MySqlConnectionConfiguration configuration,
        boolean balancedDns,
        boolean shuffle,
        int attempts
    ) {
        Iterator<InetSocketAddress> iter = addresses.iterator();

        if (!iter.hasNext()) {
            return Mono.error(fail("Fail to establish connection: no available host", null));
        }

        return configuration.getCredential().flatMap(credential -> attemptConnect(
            iter.next(), credential, tcp, configuration, balancedDns
        ).onErrorResume(t -> resumeConnect(
            t, addresses, iter, credential, tcp, configuration, balancedDns, shuffle, attempts
        )));
    }

    private Mono<Client> resumeConnect(
        Throwable t,
        List<InetSocketAddress> addresses,
        Iterator<InetSocketAddress> iter,
        Credential credential,
        TcpSocketConfiguration tcp,
        MySqlConnectionConfiguration configuration,
        boolean balancedDns,
        boolean shuffle,
        int attempts
    ) {
        if (!iter.hasNext()) {
            // The last host failed to connect
            if (attempts >= tcp.getRetriesAllDown()) {
                return Mono.error(fail(
                    "Fail to establish connection, retried " + attempts + " times: " + t.getMessage(), t));
            }

            logger.warn("All hosts failed to establish connections, auto-try again after 250ms.");

            // Ignore waiting error, e.g. interrupted, scheduler rejected
            return Mono.delay(Duration.ofMillis(250))
                .onErrorComplete()
                .then(Mono.defer(() -> connectHost(addresses, tcp, configuration, balancedDns, shuffle, attempts + 1)));
        }

        return attemptConnect(iter.next(), credential, tcp, configuration, balancedDns).onErrorResume(tt ->
            resumeConnect(tt, addresses, iter, credential, tcp, configuration, balancedDns, shuffle, attempts));
    }

    private Mono<Client> attemptConnect(
        InetSocketAddress address,
        Credential credential,
        TcpSocketConfiguration tcp,
        MySqlConnectionConfiguration configuration,
        boolean balancedDns
    ) {
        TcpClient tcpClient = ConnectionStrategy.createTcpClient(configuration.getClient(), balancedDns)
            .option(ChannelOption.SO_KEEPALIVE, tcp.isTcpKeepAlive())
            .option(ChannelOption.TCP_NODELAY, tcp.isTcpNoDelay())
            .remoteAddress(() -> address);

        return ConnectionStrategy.connectWithInit(tcpClient, credential, configuration)
            .doOnError(e -> logger.warn("Fail to connect: ", e));
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

    private static R2dbcNonTransientResourceException fail(String message, @Nullable Throwable cause) {
        return new R2dbcNonTransientResourceException(
            message,
            "H1000",
            9000,
            cause
        );
    }
}
