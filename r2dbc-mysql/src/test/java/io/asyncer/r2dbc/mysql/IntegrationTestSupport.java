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
import io.asyncer.r2dbc.mysql.internal.util.TestContainerExtension;
import io.asyncer.r2dbc.mysql.internal.util.TestServerUtil;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class considers connection factory and general function for integration tests.
 */
@ExtendWith(TestContainerExtension.class)
abstract class IntegrationTestSupport {

    private final MySqlConnectionFactory connectionFactory;

    IntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        this.connectionFactory = MySqlConnectionFactory.from(configuration);
    }

    void complete(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyComplete();
    }

    void badGrammar(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyError(R2dbcBadGrammarException.class);
    }

    void timeout(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).verifyError(R2dbcTimeoutException.class);
    }

    void illegalArgument(Function<? super MySqlConnection, Publisher<?>> runner) {
        process(runner).expectError(IllegalArgumentException.class).verify(Duration.ofSeconds(3));
    }

    Mono<? extends MySqlConnection> create() {
        return connectionFactory.create();
    }

    StepVerifier.FirstStep<Void> process(Function<? super MySqlConnection, Publisher<?>> runner) {
        return create()
            .flatMap(connection -> Flux.from(runner.apply(connection))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
                .then())
            .as(StepVerifier::create);
    }

    static Mono<Long> extractRowsUpdated(Result result) {
        return Mono.from(result.getRowsUpdated());
    }

    static MySqlConnectionConfiguration configuration(
        Function<MySqlConnectionConfiguration.Builder, MySqlConnectionConfiguration.Builder> customizer
    ) {

        String localInfilePath;

        try {
            URL url = Objects.requireNonNull(IntegrationTestSupport.class.getResource("/local/"));
            Path path = Paths.get(url.toURI());
            localInfilePath = path.toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .host(TestServerUtil.getHost())
            .port(TestServerUtil.getPort())
            .user(TestServerUtil.getUsername())
            .password(TestServerUtil.getPassword())
            .database(TestServerUtil.getDatabase())
            .connectTimeout(Duration.ofSeconds(3))
            .allowLoadLocalInfileInPath(localInfilePath);

        return customizer.apply(builder).build();
    }

    boolean envIsLessThanMySql56() {
        if (TestServerUtil.isMariaDb()) {
            return false;
        }
        final ServerVersion ver = TestServerUtil.getServerVersion();
        return ver.isLessThan(ServerVersion.create(5, 6, 0));
    }

    boolean envIsLessThanMySql578OrMariaDb102() {
        final ServerVersion ver = TestServerUtil.getServerVersion();
        if (TestServerUtil.isMariaDb()) {
            return ver.isLessThan(ServerVersion.create(10, 2, 0));
        }

        return ver.isLessThan(ServerVersion.create(5, 7, 8));
    }

    static boolean envIsMariaDb10_5_1() {
        if (!TestServerUtil.isMariaDb()) {
            return false;
        }

        final ServerVersion ver = TestServerUtil.getServerVersion();

        return ver.isGreaterThanOrEqualTo(ServerVersion.create(10, 5, 1));
    }

    boolean envIsLessThanMySql574OrMariaDb1011() {
        final ServerVersion ver = TestServerUtil.getServerVersion();
        if (TestServerUtil.isMariaDb()) {
            return ver.isLessThan(ServerVersion.create(10, 1, 1));
        }

        return ver.isLessThan(ServerVersion.create(5, 7, 4));
    }
}
