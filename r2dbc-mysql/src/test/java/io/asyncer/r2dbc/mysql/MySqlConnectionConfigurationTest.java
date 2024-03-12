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
import io.asyncer.r2dbc.mysql.constant.TlsVersions;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.asyncer.r2dbc.mysql.extension.Extension;
import io.asyncer.r2dbc.mysql.internal.NodeAddress;
import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.assertj.core.api.ThrowableTypeAssert;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link MySqlConnectionConfiguration}.
 */
class MySqlConnectionConfigurationTest {

    private static final String HOST = "localhost";

    private static final String UNIX_SOCKET = "/path/to/mysql.sock";

    private static final String USER = "root";

    private static final String SSL_CA = "/path/to/mysql/ca.pem";

    @Test
    void invalid() {
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().host(HOST).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().unixSocket(UNIX_SOCKET).build());
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().user(USER).build());
    }

    @Test
    void unixSocket() {
        for (SslMode mode : SslMode.values()) {
            assertThat(unixSocketSslMode(mode)).isNotNull();
        }

        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .build();

        assertThat(((UnixDomainSocketConfiguration) configuration.getSocket()).getPath()).isEqualTo(UNIX_SOCKET);
        assertThat(configuration.getSsl().getSslMode()).isEqualTo(SslMode.DISABLED);
        configuration.getCredential()
            .as(StepVerifier::create)
            .expectNext(new Credential(USER, null))
            .verifyComplete();
    }

    @Test
    void hosted() {
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .build();
        assertThat(((TcpSocketConfiguration) configuration.getSocket()).getAddresses())
            .isEqualTo(Collections.singletonList(new NodeAddress(HOST)));
        assertThat(configuration.getSsl().getSslMode()).isEqualTo(SslMode.PREFERRED);
        configuration.getCredential().as(StepVerifier::create)
            .expectNext(new Credential(USER, null))
            .verifyComplete();
    }

    @Test
    void allSslModeHosted() {
        String sslCa = "/path/to/ca.pem";

        for (SslMode mode : SslMode.values()) {
            MySqlConnectionConfiguration configuration = hostedSslMode(mode, sslCa);

            assertThat(configuration.getSsl().getSslMode()).isEqualTo(mode);
            assertThat(((TcpSocketConfiguration) configuration.getSocket()).getAddresses())
                .isEqualTo(Collections.singletonList(new NodeAddress(HOST)));

            if (mode.startSsl()) {
                assertThat(configuration.getSsl().getSslCa()).isSameAs(sslCa);
            }

            configuration.getCredential()
                .as(StepVerifier::create)
                .expectNext(new Credential(USER, null))
                .verifyComplete();
        }
    }

    @Test
    void invalidPort() {
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().port(-1));
        asserted.isThrownBy(() -> MySqlConnectionConfiguration.builder().port(65536));
    }

    @Test
    void allFillUp() {
        assertThat(filledUp().getSsl()).isNotNull();
    }

    @Test
    void sslContextBuilderCustomizer() {
        String message = "Worked!";
        Function<SslContextBuilder, SslContextBuilder> customizer = ignored -> {
            throw new IllegalStateException(message);
        };
        MySqlConnectionConfiguration configuration = MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .sslMode(SslMode.REQUIRED)
            .sslContextBuilderCustomizer(customizer)
            .build();
        MySqlSslConfiguration ssl = configuration.getSsl();

        assertThatIllegalStateException()
            .isThrownBy(() -> ssl.customizeSslContext(SslContextBuilder.forClient()))
            .withMessage(message);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void invalidSslContextBuilderCustomizer() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> MySqlConnectionConfiguration.builder().sslContextBuilderCustomizer(null))
            .withMessageContaining("sslContextBuilderCustomizer");
    }

    @Test
    void autodetectExtensions() {
        List<Extension> list = new ArrayList<>();
        MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .build()
            .getExtensions()
            .forEach(Extension.class, list::add);
        assertThat(list).isNotEmpty();
    }

    @Test
    void nonAutodetectExtensions() {
        List<Extension> list = new ArrayList<>();
        MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .autodetectExtensions(false)
            .build()
            .getExtensions()
            .forEach(Extension.class, list::add);
        assertThat(list).isEmpty();
    }

    @Test
    void validPasswordSupplier() {
        Mono<String> passwordSupplier = Mono.just("123456");

        Mono.from(MySqlConnectionConfiguration.builder()
                .host(HOST)
                .user(USER)
                .passwordPublisher(passwordSupplier)
                .autodetectExtensions(false)
                .build()
                .getCredential())
            .as(StepVerifier::create)
            .expectNext(new Credential(USER, "123456"))
            .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "r2dbc:mysql://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql+srv://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql+srv://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql:replication://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql:replication://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql+srv:replication://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql+srv:replication://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql:loadbalance://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql:loadbalance://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql+srv:loadbalance://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql+srv:loadbalance://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql:sequential://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql:sequential://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbc:mysql+srv:sequential://my-db1:3309,my-db2:3310/r2dbc",
        "r2dbcs:mysql+srv:sequential://my-db1:3309,my-db2:3310/r2dbc",
    })
    void multipleHosts(String url) {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(url)
            .mutate()
            .option(ConnectionFactoryOptions.USER, "root")
            .build();
        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(options);

        assertThat(configuration.getSocket()).isInstanceOf(TcpSocketConfiguration.class);

        TcpSocketConfiguration tcp = (TcpSocketConfiguration) configuration.getSocket();

        assertThat(tcp.getAddresses()).isEqualTo(Arrays.asList(
            new NodeAddress("my-db1", 3309),
            new NodeAddress("my-db2", 3310)
        ));
        assertThat(tcp.getDriver()).isEqualTo(
            ProtocolDriver.from(options.getRequiredValue(ConnectionFactoryOptions.DRIVER).toString()));
        assertThat(tcp.getProtocol()).isEqualTo(
            HaProtocol.from(Optional.ofNullable(options.getValue(ConnectionFactoryOptions.PROTOCOL))
                .map(Object::toString)
                .orElse(""))
        );
    }

    private static MySqlConnectionConfiguration unixSocketSslMode(SslMode sslMode) {
        return MySqlConnectionConfiguration.builder()
            .unixSocket(UNIX_SOCKET)
            .user(USER)
            .sslMode(sslMode)
            .build();
    }

    private static MySqlConnectionConfiguration hostedSslMode(SslMode sslMode, @Nullable String sslCa) {
        return MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .sslMode(sslMode)
            .sslCa(sslCa)
            .build();
    }

    private static MySqlConnectionConfiguration filledUp() {
        // Since 1.0.5, the passwordPublisher is Mono, equals() and hashCode() are not reliable.
        return MySqlConnectionConfiguration.builder()
            .host(HOST)
            .user(USER)
            .port(3306)
            .password("database-password-in-here")
            .database("r2dbc")
            .createDatabaseIfNotExist(true)
            .tcpKeepAlive(true)
            .tcpNoDelay(true)
            .connectTimeout(Duration.ofSeconds(3))
            .sslMode(SslMode.VERIFY_IDENTITY)
            .sslCa(SSL_CA)
            .sslCert("/path/to/mysql/client-cert.pem")
            .sslKey("/path/to/mysql/client-key.pem")
            .sslKeyPassword("pem-password-in-here")
            .tlsVersion(TlsVersions.TLS1_1, TlsVersions.TLS1_2, TlsVersions.TLS1_3)
            .compressionAlgorithms(CompressionAlgorithm.ZSTD, CompressionAlgorithm.ZLIB,
                CompressionAlgorithm.UNCOMPRESSED)
            .preserveInstants(true)
            .connectionTimeZone("LOCAL")
            .forceConnectionTimeZoneToSession(true)
            .zeroDateOption(ZeroDateOption.USE_NULL)
            .sslHostnameVerifier((host, s) -> true)
            .queryCacheSize(128)
            .prepareCacheSize(0)
            .sessionVariables("sql_mode=ANSI_QUOTES")
            .lockWaitTimeout(Duration.ofSeconds(5))
            .statementTimeout(Duration.ofSeconds(10))
            .autodetectExtensions(false)
            .build();
    }
}
