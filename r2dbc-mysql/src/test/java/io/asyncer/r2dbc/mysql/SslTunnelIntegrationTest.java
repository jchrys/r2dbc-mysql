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

import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.internal.util.TestContainerExtension;
import io.asyncer.r2dbc.mysql.internal.util.TestServerUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestContainerExtension.class)
public class SslTunnelIntegrationTest {

    private SelfSignedCertificate server;

    private SelfSignedCertificate client;

    private SslTunnelServer sslTunnelServer;

    @BeforeAll
    static void initCachingSha2Password() {
        // If the server uses caching_sha2_password, the first time a client connects to the server, the
        // server will require a native SSL connection. So all the SSL tunnel tests should be run after
        // the caching_sha2_password initialization.

        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host(TestServerUtil.getHost())
            .port(TestServerUtil.getPort())
            .connectTimeout(Duration.ofSeconds(3))
            .user(TestServerUtil.getUsername())
            .password(TestServerUtil.getPassword())
            .database(TestServerUtil.getDatabase())
            .createDatabaseIfNotExist(true)
            .build();

        MySqlConnectionFactory.from(configuration).create()
            .flatMap(connection -> connection.validate(ValidationDepth.REMOTE)
                .flatMap(it -> connection.close().then(Mono.just(it))))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @BeforeEach
    void setUp() throws CertificateException, SSLException, InterruptedException {
        server = new SelfSignedCertificate();
        client = new SelfSignedCertificate();
        final SslContext sslContext = SslContextBuilder.forServer(server.key(), server.cert()).build();
        sslTunnelServer = new SslTunnelServer(TestServerUtil.getHost(),
                                              TestServerUtil.getPort(),
                                              sslContext);
        sslTunnelServer.setUp();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        server.delete();
        client.delete();
        sslTunnelServer.tearDown();
    }

    @Test
    void sslTunnelConnectionTest() {
        final MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host("localhost")
            .port(sslTunnelServer.getLocalPort())
            .connectTimeout(Duration.ofSeconds(3))
            .user(TestServerUtil.getUsername())
            .password(TestServerUtil.getPassword())
            .database(TestServerUtil.getDatabase())
            .sslMode(SslMode.TUNNEL)
            .sslKey(client.privateKey().getAbsolutePath())
            .sslCert(client.certificate().getAbsolutePath())
            .sslCa(server.certificate().getAbsolutePath())
            .build();

        final MySqlConnectionFactory connectionFactory = MySqlConnectionFactory.from(configuration);

        final MySqlConnection connection = connectionFactory.create().block();
        assert null != connection;
        connection.createStatement("SELECT 3").execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Long.class)))
            .doOnNext(it -> assertThat(it).isEqualTo(3L))
            .blockLast();

        connection.close().block();
    }

    private static class SslTunnelServer {

        private final String remoteHost;

        private final int remotePort;

        private final SslContext sslContext;

        private volatile ChannelFuture channelFuture;

        private SslTunnelServer(String remoteHost, int remotePort, SslContext sslContext) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
            this.sslContext = sslContext;
        }

        void setUp() throws InterruptedException {
            // Configure the server.
            ServerBootstrap b = new ServerBootstrap();
            b.localAddress(0)
                .group(new NioEventLoopGroup())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ProxyInitializer(remoteHost, remotePort, sslContext))
                .childOption(ChannelOption.AUTO_READ, false);

            // Start the server.
            channelFuture = b.bind().sync();
        }

        void tearDown() throws InterruptedException {
            channelFuture.channel().close().sync();
        }

        int getLocalPort() {
            return ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        }

    }

    private static class ProxyInitializer extends ChannelInitializer<SocketChannel> {

        private final String remoteHost;

        private final int remotePort;

        private final SslContext sslContext;

        ProxyInitializer(String remoteHost, int remotePort, SslContext sslContext) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
            this.sslContext = sslContext;
        }

        @Override
        public void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
            ch.pipeline().addLast(new ProxyFrontendHandler(remoteHost, remotePort));
        }
    }

    private static class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

        private final String remoteHost;

        private final int remotePort;

        // As we use inboundChannel.eventLoop() when building the Bootstrap this does not need to be
        // volatile as the outboundChannel will use the same EventLoop (and therefore Thread) as the
        // inboundChannel.
        private Channel outboundChannel;

        private ProxyFrontendHandler(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            final Channel inboundChannel = ctx.channel();

            // Start the connection attempt.
            Bootstrap b = new Bootstrap();
            b.group(inboundChannel.eventLoop())
                .channel(ctx.channel().getClass())
                .handler(new ProxyBackendHandler(inboundChannel))
                .option(ChannelOption.AUTO_READ, false);
            ChannelFuture f = b.connect(remoteHost, remotePort);
            outboundChannel = f.channel();
            f.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    // connection complete start to read first data
                    inboundChannel.read();
                } else {
                    // Close the connection if the connection attempt has failed.
                    inboundChannel.close();
                }
            });
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        // was able to flush out data, start to read the next chunk
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (outboundChannel != null) {
                closeOnFlush(outboundChannel);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            closeOnFlush(ctx.channel());
        }

        /**
         * Closes the specified channel after all queued write requests are flushed.
         */
        static void closeOnFlush(Channel ch) {
            if (ch.isActive()) {
                ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    private static class ProxyBackendHandler extends ChannelInboundHandlerAdapter {

        private final Channel inboundChannel;

        private ProxyBackendHandler(Channel inboundChannel) {
            this.inboundChannel = inboundChannel;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            if (!inboundChannel.isActive()) {
                ProxyFrontendHandler.closeOnFlush(ctx.channel());
            } else {
                ctx.read();
            }
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    ctx.channel().read();
                } else {
                    future.channel().close();
                }
            });
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            ProxyFrontendHandler.closeOnFlush(inboundChannel);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ProxyFrontendHandler.closeOnFlush(ctx.channel());
        }
    }

}
