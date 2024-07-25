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
import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.constant.ZeroDateOption;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.assertj.core.api.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.asyncer.r2dbc.mysql.MySqlConnectionFactoryProvider.PASSWORD_PUBLISHER;
import static io.asyncer.r2dbc.mysql.MySqlConnectionFactoryProvider.RESOLVER;
import static io.asyncer.r2dbc.mysql.MySqlConnectionFactoryProvider.USE_SERVER_PREPARE_STATEMENT;
import static io.r2dbc.spi.ConnectionFactoryOptions.CONNECT_TIMEOUT;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.SSL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit test for {@link MySqlConnectionFactoryProvider}.
 */
class MySqlConnectionFactoryProviderTest {

    @Test
    void validUrl() throws UnsupportedEncodingException {
        assertThat(ConnectionFactories.get("r2dbc:mysql://root@localhost:3306"))
            .isExactlyInstanceOf(MySqlConnectionFactory.class);
        assertThat(ConnectionFactories.get("r2dbcs:mysql://root@localhost:3306"))
            .isExactlyInstanceOf(MySqlConnectionFactory.class);
        assertThat(ConnectionFactories.get("r2dbc:mysql://root@localhost:3306?unixSocket=" +
            URLEncoder.encode("/path/to/mysql.sock", "UTF-8")))
            .isExactlyInstanceOf(MySqlConnectionFactory.class);

        assertThat(ConnectionFactories.get("r2dbcs:mysql://root@localhost:3306?" +
            "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8") +
            "&sslMode=disabled")).isNotNull();

        assertThat(ConnectionFactories.get("r2dbcs:mysql://root:123456@127.0.0.1:3306/r2dbc?" +
            "zeroDate=use_round&" +
            "sslMode=verify_identity&" +
            "serverPreparing=true" +
            String.format("tlsVersion=%s&", URLEncoder.encode("TLSv1.3,TLSv1.2,TLSv1.1", "UTF-8")) +
            String.format("sslCa=%s&", URLEncoder.encode("/path/to/ca.pem", "UTF-8")) +
            String.format("sslKey=%s&", URLEncoder.encode("/path/to/client-key.pem", "UTF-8")) +
            String.format("sslCert=%s&", URLEncoder.encode("/path/to/client-cert.pem", "UTF-8")) +
            "sslKeyPassword=ssl123456")).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void urlSslModeInUnixSocket() throws UnsupportedEncodingException {
        Assert<?, SslMode> that = assertThat(SslMode.DISABLED);

        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(
            ConnectionFactoryOptions.parse("r2dbcs:mysql://root@localhost:3306?unixSocket=" +
                URLEncoder.encode("/path/to/mysql.sock", "UTF-8")));

        that.isEqualTo(configuration.getSsl().getSslMode());

        for (SslMode mode : SslMode.values()) {
            configuration = MySqlConnectionFactoryProvider.setup(
                ConnectionFactoryOptions.parse("r2dbc:mysql://root@localhost:3306?" +
                    "unixSocket=" + URLEncoder.encode("/path/to/mysql.sock", "UTF-8") +
                    "&sslMode=" + mode.name().toLowerCase()));

            that.isEqualTo(configuration.getSsl().getSslMode());
        }
    }

    @Test
    void validProgrammaticHost() {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(SSL, true)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(PORT, 3307)
            .option(USER, "root")
            .option(PASSWORD, "123456")
            .option(SSL, true)
            .option(Option.valueOf(CONNECT_TIMEOUT.name()), Duration.ofSeconds(3).toString())
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("connectionTimeZone"), "Asia/Tokyo")
            .option(Option.valueOf("useServerPrepareStatement"), AllTruePredicate.class.getName())
            .option(Option.valueOf("zeroDate"), "use_round")
            .option(Option.valueOf("sslMode"), "verify_identity")
            .option(Option.valueOf("tlsVersion"), "TLSv1.3,TLSv1.2")
            .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
            .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
            .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
            .option(Option.valueOf("sslKeyPassword"), "ssl123456")
            .option(Option.valueOf("sslHostnameVerifier"), MyHostnameVerifier.class.getName())
            .option(Option.valueOf("sslContextBuilderCustomizer"), SslCustomizer.class.getName())
            .option(Option.valueOf("tcpKeepAlive"), "true")
            .option(Option.valueOf("tcpNoDelay"), "true")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(options);

        assertThat(configuration.getDomain()).isEqualTo("127.0.0.1");
        assertThat(configuration.isHost()).isTrue();
        assertThat(configuration.getPort()).isEqualTo(3307);
        assertThat(configuration.getUser()).isEqualTo("root");
        assertThat(configuration.getPassword()).isEqualTo("123456");
        assertThat(configuration.getConnectTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(configuration.getDatabase()).isEqualTo("r2dbc");
        assertThat(configuration.getZeroDateOption()).isEqualTo(ZeroDateOption.USE_ROUND);
        assertThat(configuration.isTcpKeepAlive()).isTrue();
        assertThat(configuration.isTcpNoDelay()).isTrue();
        assertThat(configuration.getConnectionTimeZone()).isEqualTo("Asia/Tokyo");
        assertThat(configuration.getPreferPrepareStatement()).isExactlyInstanceOf(AllTruePredicate.class);
        assertThat(configuration.getExtensions()).isEqualTo(Extensions.from(Collections.emptyList(), true));

        assertThat(configuration.getSsl().getSslMode()).isEqualTo(SslMode.VERIFY_IDENTITY);
        assertThat(configuration.getSsl().getTlsVersion()).isEqualTo(new String[] { "TLSv1.3", "TLSv1.2" });
        assertThat(configuration.getSsl().getSslCa()).isEqualTo("/path/to/ca.pem");
        assertThat(configuration.getSsl().getSslKey()).isEqualTo("/path/to/client-key.pem");
        assertThat(configuration.getSsl().getSslCert()).isEqualTo("/path/to/client-cert.pem");
        assertThat(configuration.getSsl().getSslKeyPassword()).isEqualTo("ssl123456");
        assertThat(configuration.getSsl().getSslHostnameVerifier())
            .isExactlyInstanceOf(MyHostnameVerifier.class);
        assertThatExceptionOfType(MockException.class)
            .isThrownBy(() -> configuration.getSsl().customizeSslContext(SslContextBuilder.forClient()));
    }

    @Test
    void invalidProgrammatic() {
        assertThatIllegalStateException()
            .isThrownBy(() -> MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(PORT, 3307)
                .option(USER, "root")
                .option(PASSWORD, "123456")
                .option(SSL, true)
                .option(CONNECT_TIMEOUT, Duration.ofSeconds(3))
                .option(DATABASE, "r2dbc")
                .option(Option.valueOf("zeroDate"), "use_round")
                .option(Option.valueOf("sslMode"), "verify_identity")
                .option(Option.valueOf("tlsVersion"), "TLSv1.3,TLSv1.2")
                .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
                .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
                .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
                .option(Option.valueOf("sslKeyPassword"), "ssl123456")
                .option(Option.valueOf("tcpKeepAlive"), "true")
                .option(Option.valueOf("tcpNoDelay"), "true")
                .build()))
            .withMessageContaining("host");

        assertThatIllegalStateException()
            .isThrownBy(() -> MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(PORT, 3307)
                .option(PASSWORD, "123456")
                .build()))
            .withMessageContaining("user");

        assertThatIllegalArgumentException()
            .isThrownBy(() -> MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(PORT, 3307)
                .option(USER, "root")
                .option(PASSWORD, "123456")
                .option(Option.valueOf("sslMode"), "verify_ca")
                .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
                .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
                .build()))
            .withMessageContaining("sslCert and sslKey");

        assertThatIllegalArgumentException()
            .isThrownBy(() -> MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(PORT, 3307)
                .option(USER, "root")
                .option(PASSWORD, "123456")
                .option(Option.valueOf("sslMode"), "verify_ca")
                .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
                .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
                .build()))
            .withMessageContaining("sslCert and sslKey");
    }

    @Test
    void validProgrammaticUnixSocket() {
        Assert<?, String> domain = assertThat("/path/to/mysql.sock");
        Assert<?, Boolean> isHost = assertThat(false);
        Assert<?, SslMode> sslMode = assertThat(SslMode.DISABLED);

        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(USER, "root")
            .option(SSL, true)
            .build();
        MySqlConnectionConfiguration configuration = MySqlConnectionFactoryProvider.setup(options);

        domain.isEqualTo(configuration.getDomain());
        isHost.isEqualTo(configuration.isHost());
        sslMode.isEqualTo(configuration.getSsl().getSslMode());

        for (SslMode mode : SslMode.values()) {
            configuration = MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
                .option(USER, "root")
                .option(Option.valueOf("sslMode"), mode.name().toLowerCase())
                .build());

            domain.isEqualTo(configuration.getDomain());
            isHost.isEqualTo(configuration.isHost());
            sslMode.isEqualTo(configuration.getSsl().getSslMode());
        }

        configuration = MySqlConnectionFactoryProvider.setup(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(Option.valueOf("unixSocket"), "/path/to/mysql.sock")
            .option(HOST, "127.0.0.1")
            .option(PORT, 3307)
            .option(USER, "root")
            .option(PASSWORD, "123456")
            .option(SSL, true)
            .option(Option.valueOf(CONNECT_TIMEOUT.name()), Duration.ofSeconds(3).toString())
            .option(DATABASE, "r2dbc")
            .option(Option.valueOf("createDatabaseIfNotExist"), true)
            .option(Option.valueOf("connectionTimeZone"), "Asia/Tokyo")
            .option(Option.valueOf("useServerPrepareStatement"), AllTruePredicate.class.getName())
            .option(Option.valueOf("zeroDate"), "use_round")
            .option(Option.valueOf("sslMode"), "verify_identity")
            .option(Option.valueOf("tlsVersion"), "TLSv1.3,TLSv1.2")
            .option(Option.valueOf("sslCa"), "/path/to/ca.pem")
            .option(Option.valueOf("sslKey"), "/path/to/client-key.pem")
            .option(Option.valueOf("sslCert"), "/path/to/client-cert.pem")
            .option(Option.valueOf("sslKeyPassword"), "ssl123456")
            .option(Option.valueOf("sslHostnameVerifier"), MyHostnameVerifier.class.getName())
            .option(Option.valueOf("sslContextBuilderCustomizer"), SslCustomizer.class.getName())
            .option(Option.valueOf("tcpKeepAlive"), "true")
            .option(Option.valueOf("tcpNoDelay"), "true")
            .build());

        assertThat(configuration.getDomain()).isEqualTo("/path/to/mysql.sock");
        assertThat(configuration.isHost()).isFalse();
        assertThat(configuration.getPort()).isEqualTo(3306);
        assertThat(configuration.getUser()).isEqualTo("root");
        assertThat(configuration.getPassword()).isEqualTo("123456");
        assertThat(configuration.getConnectTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(configuration.getDatabase()).isEqualTo("r2dbc");
        assertThat(configuration.isCreateDatabaseIfNotExist()).isTrue();
        assertThat(configuration.getZeroDateOption()).isEqualTo(ZeroDateOption.USE_ROUND);
        assertThat(configuration.isTcpKeepAlive()).isTrue();
        assertThat(configuration.isTcpNoDelay()).isTrue();
        assertThat(configuration.getConnectionTimeZone()).isEqualTo("Asia/Tokyo");
        assertThat(configuration.getPreferPrepareStatement()).isExactlyInstanceOf(AllTruePredicate.class);
        assertThat(configuration.getExtensions()).isEqualTo(Extensions.from(Collections.emptyList(), true));

        assertThat(configuration.getSsl().getSslMode()).isEqualTo(SslMode.DISABLED);
        assertThat(configuration.getSsl().getTlsVersion()).isEmpty();
        assertThat(configuration.getSsl().getSslCa()).isNull();
        assertThat(configuration.getSsl().getSslKey()).isNull();
        assertThat(configuration.getSsl().getSslCert()).isNull();
        assertThat(configuration.getSsl().getSslKeyPassword()).isNull();
        assertThat(configuration.getSsl().getSslHostnameVerifier()).isNull();
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        assertThat(sslContextBuilder)
            .isSameAs(configuration.getSsl().customizeSslContext(sslContextBuilder));
    }

    @Test
    void serverPreparing() {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, AllTruePredicate.class.getTypeName())
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, "true")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, "false")
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, AllTruePredicate.INSTANCE)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, true)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);

        options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(USE_SERVER_PREPARE_STATEMENT, false)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void invalidServerPreparing() {
        assertThatIllegalArgumentException().isThrownBy(() ->
            ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(USER, "root")
                .option(USE_SERVER_PREPARE_STATEMENT, NotPredicate.class.getTypeName())
                .build()));

        assertThatIllegalArgumentException().isThrownBy(() ->
            ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(USER, "root")
                .option(USE_SERVER_PREPARE_STATEMENT, NotPredicate.class.getPackage() + "NonePredicate")
                .build()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "uncompressed",
        "zlib",
        "zstd",
        "zlib,uncompressed",
        "zstd,uncompressed",
        "zstd,zlib",
        "zstd,zlib,uncompressed",
    })
    void validCompressionAlgorithms(String name) {
        Set<CompressionAlgorithm> algorithms = MySqlConnectionFactoryProvider.setup(
            ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(USER, "root")
                .option(Option.valueOf("compressionAlgorithms"), name)
                .build()).getCompressionAlgorithms();

        assertThat(algorithms).hasSize(name.split(",").length);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "gzip", "lz4", "lz4hc", "none", "snappy", "zlib,none", "zstd,none" })
    void invalidCompressionAlgorithms(String name) {
        assertThatIllegalArgumentException().isThrownBy(() -> MySqlConnectionFactoryProvider.setup(
            ConnectionFactoryOptions.builder()
                .option(DRIVER, "mysql")
                .option(HOST, "127.0.0.1")
                .option(USER, "root")
                .option(Option.valueOf("compressionAlgorithms"), name)
                .build()));
    }

    @Test
    void validPasswordSupplier() {
        final Publisher<String> passwordSupplier = Mono.just("123456");
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(PASSWORD_PUBLISHER, passwordSupplier)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void validResolver() {
        final AddressResolverGroup<?> resolver = DefaultAddressResolverGroup.INSTANCE;
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(RESOLVER, resolver)
            .build();

        assertThat(ConnectionFactories.get(options)).isExactlyInstanceOf(MySqlConnectionFactory.class);
    }

    @Test
    void allConfigurationOptions() {
        List<String> exceptConfigs = Arrays.asList(
            "extendWith",
            "username",
            "zeroDateOption");
        List<String> exceptOptions = Arrays.asList(
            "driver",
            "ssl",
            "protocol",
            "zeroDate");
        Set<String> allOptions = Stream.concat(
                Arrays.stream(ConnectionFactoryOptions.class.getFields()),
                Arrays.stream(MySqlConnectionFactoryProvider.class.getFields())
            )
            .filter(field -> Modifier.isStatic(field.getModifiers()) && field.getType() == Option.class)
            .map(field -> {
                try {
                    return ((Option<?>) field.get(null)).name();
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            })
            .filter(name -> !exceptOptions.contains(name))
            .collect(Collectors.toSet());
        Set<String> allBuilderOptions = Arrays.stream(MySqlConnectionConfiguration.Builder.class.getMethods())
            .filter(method -> method.getParameterCount() >= 1 &&
                method.getReturnType() == MySqlConnectionConfiguration.Builder.class &&
                !exceptConfigs.contains(method.getName()))
            .map(Method::getName)
            .collect(Collectors.toSet());

        assertThat(allBuilderOptions).containsExactlyInAnyOrderElementsOf(allOptions);
    }

    @ParameterizedTest
    @MethodSource
    void sessionVariables(String input, List<String> expected) {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(DRIVER, "mysql")
            .option(HOST, "127.0.0.1")
            .option(USER, "root")
            .option(Option.valueOf("sessionVariables"), input)
            .build();

        assertThat(MySqlConnectionFactoryProvider.setup(options).getSessionVariables()).isEqualTo(expected);
    }

    static Stream<Arguments> sessionVariables() {
        return Stream.of(
            Arguments.of("", Collections.emptyList()),
            Arguments.of(" ", Collections.singletonList("")),
            Arguments.of("a=b", Collections.singletonList("a=b")),
            Arguments.of(
                "sql_mode=ANSI_QUOTE,c=d;e=f",
                Arrays.asList("sql_mode=ANSI_QUOTE", "c=d", "e=f")),
            Arguments.of(
                "sql_mode='ANSI_QUOTES,b=c,c=d';c=d,e=f",
                Arrays.asList("sql_mode='ANSI_QUOTES,b=c,c=d'", "c=d", "e=f")),
            Arguments.of(
                "sql_mode=(ANSI_QUOTES,'b=c,c=d,max(');c=(d,e='f)');",
                Arrays.asList("sql_mode=(ANSI_QUOTES,'b=c,c=d,max(')", "c=(d,e='f)')", "")),
            Arguments.of(
                "sql_mode=(ANSI_QUOTES,'b=c,c=d,max(');c=(d,e='f)'); ",
                Arrays.asList("sql_mode=(ANSI_QUOTES,'b=c,c=d,max(')", "c=(d,e='f)')", "")),
            Arguments.of(
                "sql_mode=(ANSI_QUOTES,\"b=c',c=d,max(\");c=(d,'e=\"f)\");',)",
                Arrays.asList("sql_mode=(ANSI_QUOTES,\"b=c',c=d,max(\")", "c=(d,'e=\"f)\");',)")),
            Arguments.of(
                "sql_mode=(((;),);)",
                Collections.singletonList("sql_mode=(((;),);)")),
            Arguments.of(
                "sql_mode=(((';),););',);a=),);d=)",
                Arrays.asList("sql_mode=(((';),););',);a=),)", "d=)")),
            Arguments.of(
                "sql_mode=((\"(';),)\";);',);)a=,)';),b=(();)",
                Arrays.asList("sql_mode=((\"(';),)\";);',);)a=,)';)", "b=(();)")),
            Arguments.of(
                "sql_mode=((\"(';),)\";);',);)a=,)'b=;)\\,c=(();)",
                Arrays.asList("sql_mode=((\"(';),)\";);',);)a=,)'b=;)\\", "c=(();)")),
            Arguments.of(
                "sql_mode='\\','",
                Collections.singletonList("sql_mode='\\','")),
            Arguments.of(
                "sql_mode=\",\\\",'\\\\',',\"",
                Collections.singletonList("sql_mode=\",\\\",'\\\\',',\"")),
            Arguments.of(
                "sql_mode='ANSI_QUOTES,STRICT_TRANS_TABLES'," +
                    "transaction_isolation=(SELECT UPPER(`it's ``lvl```) FROM `lvl` WHERE `type` = 'r2dbc')" +
                    ",`foo``bar`='FOO,BAR'",
                Arrays.asList(
                    "sql_mode='ANSI_QUOTES,STRICT_TRANS_TABLES'",
                    "transaction_isolation=(SELECT UPPER(`it's ``lvl```) FROM `lvl` WHERE `type` = 'r2dbc')",
                    "`foo``bar`='FOO,BAR'"
                ))
        );
    }
}

final class MockException extends RuntimeException {

    static final MockException INSTANCE = new MockException();
}

final class SslCustomizer implements Function<SslContextBuilder, SslContextBuilder> {

    @Override
    public SslContextBuilder apply(SslContextBuilder sslContextBuilder) {
        throw MockException.INSTANCE;
    }
}

final class MyHostnameVerifier implements HostnameVerifier {

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        return true;
    }
}

final class AllTruePredicate implements Predicate<String> {

    static final Predicate<String> INSTANCE = new AllTruePredicate();

    @Override
    public boolean test(String s) {
        return true;
    }
}

final class NotPredicate {

    public boolean test(String s) {
        return !s.isEmpty();
    }
}
