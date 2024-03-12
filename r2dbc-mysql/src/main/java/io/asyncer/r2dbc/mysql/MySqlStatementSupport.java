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

import io.asyncer.r2dbc.mysql.api.MySqlStatement;
import io.asyncer.r2dbc.mysql.client.Client;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import org.jetbrains.annotations.Nullable;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Base class considers generic logic for {@link MySqlStatement} implementations.
 */
abstract class MySqlStatementSupport implements MySqlStatement {

    private static final ServerVersion MARIA_10_5_1 = ServerVersion.create(10, 5, 1, true);

    private static final String LAST_INSERT_ID = "LAST_INSERT_ID";

    protected final Client client;

    @Nullable
    private String[] generatedColumns = null;

    MySqlStatementSupport(Client client) {
        this.client = requireNonNull(client, "client must not be null");
    }

    @Override
    public final MySqlStatement returnGeneratedValues(String... columns) {
        requireNonNull(columns, "columns must not be null");

        int len = columns.length;

        if (len == 0) {
            this.generatedColumns = InternalArrays.EMPTY_STRINGS;
        } else if (len == 1 || supportReturning(client.getContext())) {
            String[] result = new String[len];

            for (int i = 0; i < len; ++i) {
                requireNonEmpty(columns[i], "returning column must not be empty");
                result[i] = columns[i];
            }

            this.generatedColumns = result;
        } else {
            String db = client.getContext().isMariaDb() ? "MariaDB 10.5.0 or below" : "MySQL";
            throw new IllegalArgumentException(db + " can have only one column");
        }

        return this;
    }

    @Nullable
    final String syntheticKeyName() {
        String[] columns = this.generatedColumns;

        // MariaDB should use `RETURNING` clause instead.
        if (columns == null || supportReturning(this.client.getContext())) {
            return null;
        }

        if (columns.length == 0) {
            return LAST_INSERT_ID;
        }

        return columns[0];
    }

    final String returningIdentifiers() {
        String[] columns = this.generatedColumns;

        if (columns == null || !supportReturning(this.client.getContext())) {
            return "";
        }

        if (columns.length == 0) {
            return "*";
        }

        // Columns should not be quoted, it can be an expression. e.g. RETURNING CURRENT_TIMESTAMP
        // It would be confusing if we quoted it, e.g. RETURNING `CURRENT_TIMESTAMP`
        // Keep columns raw and let ORM/QueryBuilder handle it.
        return String.join(",", columns);
    }

    static boolean supportReturning(ConnectionContext context) {
        return context.isMariaDb() && context.getServerVersion().isGreaterThanOrEqualTo(MARIA_10_5_1);
    }
}
