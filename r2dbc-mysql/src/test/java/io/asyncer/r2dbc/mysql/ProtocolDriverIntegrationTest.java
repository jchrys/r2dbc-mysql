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

import io.asyncer.r2dbc.mysql.internal.util.TestContainerExtension;
import io.asyncer.r2dbc.mysql.internal.util.TestServerUtil;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Integration tests for DNS SRV records.
 */
@ExtendWith(TestContainerExtension.class)
class ProtocolDriverIntegrationTest {

    @Test
    void anyAvailable() {
        // Force to use localhost for DNS SRV testing
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "mysql+srv")
            .option(ConnectionFactoryOptions.PROTOCOL, "loadbalance")
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, TestServerUtil.getPort())
            .option(ConnectionFactoryOptions.USER, TestServerUtil.getUsername())
            .option(ConnectionFactoryOptions.PASSWORD, TestServerUtil.getPassword())
            .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(3))
            .build();
        // localhost should be resolved to 127.0.0.1 and [::1], but I can't make sure GitHub Actions support IPv6
        MySqlConnectionFactory.from(MySqlConnectionFactoryProvider.setup(options))
            .create()
            .flatMapMany(connection -> connection.validate(ValidationDepth.REMOTE)
                .onErrorReturn(false)
                .concatWith(connection.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }
}
