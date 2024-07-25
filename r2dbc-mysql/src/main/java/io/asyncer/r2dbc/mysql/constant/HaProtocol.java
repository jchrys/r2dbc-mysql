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

package io.asyncer.r2dbc.mysql.constant;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * Failover and High-availability protocol.
 * <p>
 * The reconnect behavior is affected by the {@code autoReconnect} option.
 */
public enum HaProtocol {

    /**
     * Connecting: I want to connect sequentially until the first available node is found if multiple nodes are
     * provided, otherwise connect to the single node.
     * <p>
     * Using: I want to get back to the first node if either {@code secondsBeforeRetryPrimaryHost} or
     * {@code queriesBeforeRetryPrimaryHost} is set, and multiple nodes are provided.
     * <p>
     * Reconnect: I want to reconnect in the same order if the current node is not available and
     * {@code autoReconnect=true}.
     */
    DEFAULT(""),

    /**
     * Connecting: I want to connect sequentially until the first available node is found.
     * <p>
     * Using: I want to keep using the current node until it is not available.
     * <p>
     * Reconnect: I want to reconnect in the same order if the current node is not available and
     * {@code autoReconnect=true}.
     */
    SEQUENTIAL("sequential"),

    /**
     * Connecting: I want to connect in random order until the first available node is found.
     * <p>
     * Using: I want to keep using the current node until it is not available.
     * <p>
     * Reconnect: I want to re-randomize the order to reconnect if the current node is not available and
     * {@code autoReconnect=true}.
     */
    LOAD_BALANCE("loadbalance"),

    /**
     * Connecting: I want to use read-write connection for the first node, and read-only connections for other nodes.
     * <p>
     * Using: I want to use the first node for read-write if connection is set to read-write, and other nodes if
     * connection is set to read-only. R2DBC can not set a {@link io.r2dbc.spi.Connection Connection} to read-only mode.
     * So it will always use the first host. Perhaps in the future, R2DBC will support using read-only mode to create a
     * connection instead of modifying an existing connection.
     * <p>
     * Reconnect: I want to reconnect to the current node if the current node is unavailable  and
     * {@code autoReconnect=true}.
     *
     * @see <a href="https://github.com/r2dbc/r2dbc-spi/issues/98">Proposal: add Connection.setReadonly(boolean)</a>
     */
    REPLICATION("replication"),
    ;

    private final String name;

    HaProtocol(String name) {
        this.name = name;
    }

    public static HaProtocol from(String protocol) {
        requireNonNull(protocol, "HA protocol must not be null");

        for (HaProtocol haProtocol : HaProtocol.values()) {
            if (haProtocol.name.equalsIgnoreCase(protocol)) {
                return haProtocol;
            }
        }

        throw new IllegalArgumentException("Unknown HA protocol: " + protocol);
    }
}
