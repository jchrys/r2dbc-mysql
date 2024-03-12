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

import io.asyncer.r2dbc.mysql.api.MySqlBatch;
import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.codec.Codecs;
import reactor.core.publisher.Flux;

import java.util.StringJoiner;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * An implementation of {@link MySqlBatch} for executing a collection of statements in a batch against the MySQL
 * database.
 */
final class MySqlBatchingBatch implements MySqlBatch {

    private final Client client;

    private final Codecs codecs;

    private final StringJoiner queries = new StringJoiner(";");

    MySqlBatchingBatch(Client client, Codecs codecs) {
        this.client = requireNonNull(client, "client must not be null");
        this.codecs = requireNonNull(codecs, "codecs must not be null");
    }

    @Override
    public MySqlBatch add(String sql) {
        requireNonNull(sql, "sql must not be null");

        int index = lastNonWhitespace(sql);

        if (index >= 0 && sql.charAt(index) == ';') {
            // Skip last ';' and whitespaces that following last ';'.
            queries.add(sql.substring(0, index));
        } else {
            queries.add(sql);
        }

        return this;
    }

    @Override
    public Flux<MySqlResult> execute() {
        return QueryFlow.execute(client, getSql())
            .map(messages -> MySqlSegmentResult.toResult(false, client, codecs, null, messages));
    }

    @Override
    public String toString() {
        return "MySqlBatchingBatch{sql=REDACTED}";
    }

    /**
     * Accessible for unit test.
     *
     * @return current batching SQL statement
     */
    String getSql() {
        return queries.toString();
    }

    private static int lastNonWhitespace(String sql) {
        int size = sql.length();

        for (int i = size - 1; i >= 0; --i) {
            if (!Character.isWhitespace(sql.charAt(i))) {
                return i;
            }
        }

        return -1;
    }
}
