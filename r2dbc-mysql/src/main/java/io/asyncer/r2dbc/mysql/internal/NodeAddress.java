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

package io.asyncer.r2dbc.mysql.internal;

import java.net.InetSocketAddress;

/**
 * A value object representing a host and port. It will use the default port {@code 3306} if not specified.
 */
public final class NodeAddress {

    private static final int DEFAULT_PORT = 3306;

    private final String host;

    private final int port;

    public NodeAddress(String host) {
        this(host, DEFAULT_PORT);
    }

    public NodeAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public InetSocketAddress toUnresolved() {
        return InetSocketAddress.createUnresolved(this.host, this.port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeAddress)) {
            return false;
        }

        NodeAddress that = (NodeAddress) o;

        return port == that.port && host.equals(that.host);
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
