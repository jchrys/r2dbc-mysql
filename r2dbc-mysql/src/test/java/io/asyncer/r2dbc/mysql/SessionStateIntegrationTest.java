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

import io.asyncer.r2dbc.mysql.internal.util.StringUtils;
import io.asyncer.r2dbc.mysql.internal.util.TestContainerExtension;
import io.asyncer.r2dbc.mysql.internal.util.TestServerUtil;
import io.r2dbc.spi.R2dbcTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration tests for session states.
 */
@ExtendWith(TestContainerExtension.class)
class SessionStateIntegrationTest {

    @Test
    void forcedLocalTimeZone() {
        ZoneId zoneId = ZoneId.systemDefault().normalized();

        connectionFactory(builder -> builder.connectionTimeZone("local")
            .forceConnectionTimeZoneToSession(true))
            .create()
            .flatMapMany(
                connection -> connection.createStatement("SELECT @@time_zone").execute()
                    .flatMap(result -> result.map(r -> r.get(0, String.class)))
                    .map(StringUtils::parseZoneId)
                    .onErrorResume(e -> connection.close().then(Mono.error(e)))
                    .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(zoneId)
            .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "America/New_York",
        "Asia/Seoul",
        "Asia/Shanghai",
        "Asia/Tokyo",
        "Europe/London",
        "Factory",
        "GMT",
        "JST",
        "ROC",
        "UTC",
        "+00:00",
        "+09:00",
        "-09:00",
    })
    void forcedConnectionTimeZone(String timeZone) {
        ZoneId zoneId = StringUtils.parseZoneId(timeZone);

        connectionFactory(builder -> builder.connectionTimeZone(timeZone)
            .forceConnectionTimeZoneToSession(true))
            .create()
            .flatMapMany(
                connection -> connection.createStatement("SELECT @@time_zone").execute()
                    .flatMap(result -> result.map(r -> r.get(0, String.class)))
                    .map(StringUtils::parseZoneId)
                    .onErrorResume(e -> connection.close().then(Mono.error(e)))
                    .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(zoneId)
            .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource
    void sessionVariables(Map<String, String> variables) {
        String[] pairs = variables.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .toArray(String[]::new);
        String[] keys = variables.keySet().toArray(new String[0]);
        String selection = variables.keySet().stream()
            .map(it -> "@@session." + it + " AS " + it)
            .collect(Collectors.joining(",", "SELECT ", ""));
        Map<String, String> expected = variables.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().startsWith("'") ?
                entry.getValue().substring(1, entry.getValue().length() - 1) : entry.getValue()));

        connectionFactory(builder -> builder.sessionVariables(pairs))
            .create()
            .flatMapMany(connection -> connection.createStatement(selection).execute()
                .flatMap(result -> result.map(r -> {
                    Map<String, String> map = new LinkedHashMap<>();
                    for (String key : keys) {
                        map.put(key, r.get(key, String.class));
                    }
                    return map;
                }))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(expected)
            .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = { "PT1S", "PT10S", "PT1M" })
    void initLockWaitTimeout(String timeout) {
        Duration lockWaitTimeout = Duration.parse(timeout);

        connectionFactory(builder -> builder.lockWaitTimeout(lockWaitTimeout))
            .create()
            .flatMapMany(connection -> connection.createStatement("SELECT @@innodb_lock_wait_timeout").execute()
                .flatMap(result -> result.map(r -> r.get(0, Long.class)))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .expectNext(lockWaitTimeout.getSeconds())
            .verifyComplete();
    }

    @EnabledIf("isGreaterThanOrEqualToMariaDB10_1_1MySql5_7_4")
    @ParameterizedTest
    @ValueSource(strings = { "PT0.1S", "PT0.5S" })
    void initStatementTimeout(String timeout) {
        final String sql = "SELECT COUNT(*) " +
                "FROM information_schema.tables a cross join " +
                "information_schema.tables b cross join " +
                "information_schema.tables c cross join " +
                "information_schema.tables d cross join " +
                "information_schema.tables e";

        Duration statementTimeout = Duration.parse(timeout);

        connectionFactory(builder -> builder.statementTimeout(statementTimeout))
            .create()
            .flatMapMany(connection -> connection.createStatement(sql).execute()
                .flatMap(result -> result.map(r -> r.get(0)))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty()))
            )
            .as(StepVerifier::create)
            .verifyError(R2dbcTimeoutException.class);
    }

    static boolean isGreaterThanOrEqualToMariaDB10_1_1MySql5_7_4() {
        final ServerVersion ver = TestServerUtil.getServerVersion();
        if (TestServerUtil.isMariaDb()) {
            return ver.isGreaterThanOrEqualTo(ServerVersion.create(10, 1, 1));
        }

        return ver.isGreaterThanOrEqualTo(ServerVersion.create(5, 7, 4));
    }

    static Stream<Arguments> sessionVariables() {
        return Stream.of(
            Arguments.of(mapOf("sql_mode", "ANSI_QUOTES")),
            Arguments.of(mapOf("time_zone", "'+00:00'")),
            Arguments.of(mapOf("sql_mode", "'ANSI_QUOTES,STRICT_ALL_TABLES'", "time_zone", "'Asia/Tokyo'"))
        );
    }

    private static MySqlConnectionFactory connectionFactory(
        Function<MySqlConnectionConfiguration.Builder, MySqlConnectionConfiguration.Builder> customizer
    ) {

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .host(TestServerUtil.getHost())
            .port(TestServerUtil.getPort())
            .user(TestServerUtil.getUsername())
            .password(TestServerUtil.getPassword())
            .database(TestServerUtil.getDatabase());

        return MySqlConnectionFactory.from(customizer.apply(builder).build());
    }

    private static Map<String, String> mapOf(String... paris) {
        if (paris.length % 2 != 0) {
            throw new IllegalArgumentException("Pairs must be even");
        }

        Map<String, String> map = new LinkedHashMap<>();

        for (int i = 0; i < paris.length; i += 2) {
            map.put(paris[i], paris[i + 1]);
        }

        return map;
    }
}
