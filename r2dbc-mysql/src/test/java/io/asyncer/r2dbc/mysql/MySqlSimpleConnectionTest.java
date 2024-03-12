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

import io.asyncer.r2dbc.mysql.api.MySqlTransactionDefinition;
import io.asyncer.r2dbc.mysql.cache.Caches;
import io.asyncer.r2dbc.mysql.cache.PrepareCache;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.client.FluxExchangeable;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import io.asyncer.r2dbc.mysql.constant.ServerStatuses;
import io.asyncer.r2dbc.mysql.message.client.ClientMessage;
import io.asyncer.r2dbc.mysql.message.client.TextQueryMessage;
import io.asyncer.r2dbc.mysql.message.server.CompleteMessage;
import io.r2dbc.spi.IsolationLevel;
import org.assertj.core.api.ThrowableTypeAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MySqlSimpleConnection}.
 */
class MySqlSimpleConnectionTest {

    private static final Codecs CODECS = mock(Codecs.class);

    @Test
    void createStatement() {
        String condition = "SELECT * FROM test";
        MySqlSimpleConnection allPrepare = new MySqlSimpleConnection(
            mockClient(),
            CODECS,
            Caches.createQueryCache(0), sql -> true);
        MySqlSimpleConnection halfPrepare = new MySqlSimpleConnection(
            mockClient(),
            CODECS,
            Caches.createQueryCache(0), sql -> false);
        MySqlSimpleConnection conditionPrepare = new MySqlSimpleConnection(
            mockClient(),
            CODECS,
            Caches.createQueryCache(0), sql -> sql.equals(condition));
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());

        assertThat(noPrepare.createStatement("SELECT * FROM test WHERE id=1"))
            .isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(noPrepare.createStatement(condition))
            .isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(noPrepare.createStatement("SELECT * FROM test WHERE id=?"))
            .isExactlyInstanceOf(TextParameterizedStatement.class);

        assertThat(allPrepare.createStatement("SELECT * FROM test WHERE id=1"))
            .isExactlyInstanceOf(PrepareSimpleStatement.class);
        assertThat(allPrepare.createStatement(condition))
            .isExactlyInstanceOf(PrepareSimpleStatement.class);
        assertThat(allPrepare.createStatement("SELECT * FROM test WHERE id=?"))
            .isExactlyInstanceOf(PrepareParameterizedStatement.class);

        assertThat(halfPrepare.createStatement("SELECT * FROM test WHERE id=1"))
            .isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(halfPrepare.createStatement(condition))
            .isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(halfPrepare.createStatement("SELECT * FROM test WHERE id=?"))
            .isExactlyInstanceOf(PrepareParameterizedStatement.class);

        assertThat(conditionPrepare.createStatement("SELECT * FROM test WHERE id=1"))
            .isExactlyInstanceOf(TextSimpleStatement.class);
        assertThat(conditionPrepare.createStatement(condition))
            .isExactlyInstanceOf(PrepareSimpleStatement.class);
        assertThat(conditionPrepare.createStatement("SELECT * FROM test WHERE id=?"))
            .isExactlyInstanceOf(PrepareParameterizedStatement.class);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badCreateStatement() {
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());
        assertThatIllegalArgumentException().isThrownBy(() -> noPrepare.createStatement(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badCreateSavepoint() {
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> noPrepare.createSavepoint(""));
        asserted.isThrownBy(() -> noPrepare.createSavepoint(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badReleaseSavepoint() {
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> noPrepare.releaseSavepoint(""));
        asserted.isThrownBy(() -> noPrepare.releaseSavepoint(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badRollbackTransactionToSavepoint() {
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());
        ThrowableTypeAssert<?> asserted = assertThatIllegalArgumentException();

        asserted.isThrownBy(() -> noPrepare.rollbackTransactionToSavepoint(""));
        asserted.isThrownBy(() -> noPrepare.rollbackTransactionToSavepoint(null));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badSetTransactionIsolationLevel() {
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());
        assertThatIllegalArgumentException().isThrownBy(() -> noPrepare.setTransactionIsolationLevel(null));
    }

    @ParameterizedTest
    @ValueSource(strings = { "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE" })
    void shouldSetTransactionIsolationLevelSuccessfully(String levelSql) {
        Client client = mockClient();
        IsolationLevel level = IsolationLevel.valueOf(levelSql);
        ClientMessage message = new TextQueryMessage("SET SESSION TRANSACTION ISOLATION LEVEL " + levelSql);

        when(client.exchange(eq(message), any())).thenReturn(Flux.empty());

        MySqlSimpleConnection noPrepare = newNoPrepare(client);
        noPrepare.setTransactionIsolationLevel(level)
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(client.getContext().getCurrentIsolationLevel()).isEqualTo(level);
        assertThat(client.getContext().getSessionIsolationLevel()).isEqualTo(level);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "READ UNCOMMITTED,SERIALIZABLE",
        "READ COMMITTED,REPEATABLE READ",
        "REPEATABLE READ,READ UNCOMMITTED"
    })
    void shouldSetTransactionIsolationLevelInTransaction(String levels) {
        String[] levelStatements = levels.split(",");
        IsolationLevel currentLevel = IsolationLevel.valueOf(levelStatements[0]);
        IsolationLevel sessionLevel = IsolationLevel.valueOf(levelStatements[1]);
        Client client = mockClient();
        ClientMessage session = new TextQueryMessage("SET SESSION TRANSACTION ISOLATION LEVEL " + sessionLevel.asSql());
        CompleteMessage mockDone = mock(CompleteMessage.class);
        @SuppressWarnings("unchecked")
        SynchronousSink<ClientMessage> sink = (SynchronousSink<ClientMessage>) mock(SynchronousSink.class);
        AtomicBoolean completed = new AtomicBoolean(false);

        doAnswer(it -> {
            throw it.getArgument(0, Exception.class);
        }).when(sink).error(any());
        doAnswer(it -> {
            completed.set(true);
            return null;
        }).when(sink).complete();
        when(mockDone.isDone()).thenReturn(true);
        when(client.exchange(eq(session), any())).thenReturn(Flux.empty());
        when(client.exchange(any())).thenAnswer(it -> {
            FluxExchangeable<ClientMessage> exchangeable = it.getArgument(0);
            @SuppressWarnings("unchecked")
            CoreSubscriber<? super ClientMessage> subscriber = mock(CoreSubscriber.class);
            exchangeable.subscribe(subscriber);

            while (!completed.get()) {
                exchangeable.accept(mockDone, sink);
            }

            // Mock server status to be in transaction
            client.getContext().setServerStatuses(ServerStatuses.IN_TRANSACTION);

            return Flux.empty();
        });

        IsolationLevel mockLevel = IsolationLevel.valueOf("DEFAULT");
        client.getContext().initSession(
            mock(PrepareCache.class),
            mockLevel,
            false,
            Duration.ZERO,
            null,
            null
        );
        MySqlSimpleConnection noPrepare = newNoPrepare(client);

        assertThat(client.getContext().getCurrentIsolationLevel()).isEqualTo(mockLevel);
        assertThat(client.getContext().getSessionIsolationLevel()).isEqualTo(mockLevel);

        noPrepare.beginTransaction(MySqlTransactionDefinition.from(currentLevel))
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(client.getContext().getCurrentIsolationLevel()).isEqualTo(currentLevel);
        assertThat(client.getContext().getSessionIsolationLevel()).isEqualTo(mockLevel);

        noPrepare.setTransactionIsolationLevel(sessionLevel)
            .as(StepVerifier::create)
            .verifyComplete();

        assertThat(client.getContext().getCurrentIsolationLevel()).isEqualTo(currentLevel);
        assertThat(client.getContext().getSessionIsolationLevel()).isEqualTo(sessionLevel);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void badValidate() {
        MySqlSimpleConnection noPrepare = newNoPrepare(mockClient());
        assertThatIllegalArgumentException().isThrownBy(() -> noPrepare.validate(null));
    }

    private static Client mockClient() {
        Client client = mock(Client.class);

        when(client.getContext()).thenReturn(ConnectionContextTest.mock());

        return client;
    }

    private static MySqlSimpleConnection newNoPrepare(Client client) {
        return new MySqlSimpleConnection(
            client,
            CODECS,
            Caches.createQueryCache(0),
            null
        );
    }
}
