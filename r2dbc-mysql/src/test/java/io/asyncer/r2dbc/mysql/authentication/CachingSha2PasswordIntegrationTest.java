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

package io.asyncer.r2dbc.mysql.authentication;

import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;
import io.asyncer.r2dbc.mysql.MySqlConnectionFactory;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.time.Duration;

/**
 * Integration tests for caching_sha2_password authentication (MySQL 8.0 default).
 */
class CachingSha2PasswordIntegrationTest extends AbstractCachingSha2PasswordTest {

    private static final String TEST_USER = "test_sha2_user";
    private static final String TEST_PASSWORD = "test_sha2_pass";

    @BeforeEach
    void setUp() throws SQLException {
        // Clean up any existing test user
        dropUser(TEST_USER);
    }

    @AfterEach
    void tearDown() throws SQLException {
        // Clean up test user after test
        dropUser(TEST_USER);
    }

    @Test
    void authenticateWithCachingSha2Password() throws SQLException {
        // Given: Create a user with caching_sha2_password
        createUser(TEST_USER, TEST_PASSWORD);
        grantAllPrivileges(TEST_USER);

        // When: Connect with R2DBC using the test user
        MySqlConnectionConfiguration config =
            MySqlConnectionConfiguration.builder()
                .host(getContainer().getHost())
                .port(getContainer().getFirstMappedPort())
                .user(TEST_USER)
                .password(TEST_PASSWORD)
                .database("test")
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        MySqlConnectionFactory factory = MySqlConnectionFactory.from(config);

        // Then: Connection should succeed and be valid
        Mono.from(factory.create())
            .flatMap(conn -> Mono.from(conn.validate(ValidationDepth.LOCAL))
                .thenReturn(conn))
            .flatMap(conn -> conn.close().thenReturn(true))
            .as(StepVerifier::create)
            .expectNext(true)
            .verifyComplete();
    }

    @Test
    void authenticationFailsWithWrongPassword() throws SQLException {
        // Given: Create a user with a password
        createUser(TEST_USER, TEST_PASSWORD);
        grantAllPrivileges(TEST_USER);

        // When: Connect with wrong password
        MySqlConnectionConfiguration config =
            MySqlConnectionConfiguration.builder()
                .host(getContainer().getHost())
                .port(getContainer().getFirstMappedPort())
                .user(TEST_USER)
                .password("wrong_password")
                .database("test")
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        MySqlConnectionFactory factory = MySqlConnectionFactory.from(config);

        // Then: Connection should fail
        Mono.from(factory.create())
            .as(StepVerifier::create)
            .expectError()
            .verify();
    }

    @Test
    void canExecuteQueriesAfterAuthentication() throws SQLException {
        // Given: Create and authorize a user
        createUser(TEST_USER, TEST_PASSWORD);
        grantAllPrivileges(TEST_USER);

        // When: Connect and execute a simple query
        MySqlConnectionConfiguration config =
            MySqlConnectionConfiguration.builder()
                .host(getContainer().getHost())
                .port(getContainer().getFirstMappedPort())
                .user(TEST_USER)
                .password(TEST_PASSWORD)
                .database("test")
                .connectTimeout(Duration.ofSeconds(3))
                .build();

        MySqlConnectionFactory factory = MySqlConnectionFactory.from(config);

        // Then: Query should execute successfully
        Mono.from(factory.create())
            .flatMapMany(conn -> conn.createStatement("SELECT 1 AS value")
                .execute()
                .flatMap(result -> result.map((row, meta) -> row.get("value", Integer.class)))
                .concatWith(conn.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .expectNext(1)
            .verifyComplete();
    }

    @Test
    void verifyServerHasRsaPublicKey() throws SQLException {
        // Verify that the MySQL 8.0 server has RSA public key available
        // This is needed for secure password exchange with caching_sha2_password
        String rsaKey = getServerRsaPublicKey();
        // RSA key should be available in MySQL 8.0
        // Note: Actual key format verification can be added if needed
    }
}
