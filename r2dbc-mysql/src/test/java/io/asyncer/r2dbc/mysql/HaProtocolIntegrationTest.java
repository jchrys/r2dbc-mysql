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

import io.asyncer.r2dbc.mysql.constant.HaProtocol;
import io.asyncer.r2dbc.mysql.internal.util.TestContainerExtension;
import io.asyncer.r2dbc.mysql.internal.util.TestServerUtil;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link HaProtocol}.
 */
@ExtendWith(TestContainerExtension.class)
class HaProtocolIntegrationTest {

    @ParameterizedTest
    @ValueSource(strings = { "sequential", "loadbalance" })
    void anyAvailable(String protocol) {
        MySqlConnectionFactory.from(configuration(HaProtocol.from(protocol), true)).create()
            .flatMapMany(connection -> connection.validate(ValidationDepth.REMOTE)
                .onErrorReturn(false)
                .concatWith(connection.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @ParameterizedTest
    @ValueSource(strings = { "replication", "" })
    void firstAvailable(String protocol) {
        MySqlConnectionFactory.from(configuration(HaProtocol.from(protocol), false)).create()
            .flatMapMany(connection -> connection.validate(ValidationDepth.REMOTE)
                .onErrorReturn(false)
                .concatWith(connection.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    private MySqlConnectionConfiguration configuration(HaProtocol protocol, boolean badFirst) {
        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .protocol(protocol)
            .connectTimeout(Duration.ofSeconds(3))
            .user(TestServerUtil.getUsername())
            .password(TestServerUtil.getPassword())
            .database(TestServerUtil.getDatabase());

        if (badFirst) {
            builder.addHost(TestServerUtil.getHost(), 3310).addHost(TestServerUtil.getHost(), TestServerUtil.getPort());
        } else {
            builder.addHost(TestServerUtil.getHost(), TestServerUtil.getPort()).addHost(TestServerUtil.getHost(), 3310);
        }

        return builder.build();
    }
}
