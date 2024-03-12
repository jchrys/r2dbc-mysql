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
import io.asyncer.r2dbc.mysql.constant.ProtocolDriver;
import io.asyncer.r2dbc.mysql.internal.NodeAddress;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonEmpty;

/**
 * A configuration for a TCP/SSL socket.
 */
final class TcpSocketConfiguration implements SocketConfiguration {

    private static final int DEFAULT_PORT = 3306;

    private final ProtocolDriver driver;

    private final HaProtocol protocol;

    private final List<NodeAddress> addresses;

    private final int retriesAllDown;

    private final boolean tcpKeepAlive;

    private final boolean tcpNoDelay;

    TcpSocketConfiguration(
        ProtocolDriver driver,
        HaProtocol protocol,
        List<NodeAddress> addresses,
        int retriesAllDown,
        boolean tcpKeepAlive,
        boolean tcpNoDelay
    ) {
        this.driver = driver;
        this.protocol = protocol;
        this.addresses = addresses;
        this.retriesAllDown = retriesAllDown;
        this.tcpKeepAlive = tcpKeepAlive;
        this.tcpNoDelay = tcpNoDelay;
    }

    ProtocolDriver getDriver() {
        return driver;
    }

    HaProtocol getProtocol() {
        return protocol;
    }

    NodeAddress getFirstAddress() {
        if (addresses.isEmpty()) {
            throw new IllegalStateException("No endpoints configured");
        }
        return addresses.get(0);
    }

    List<NodeAddress> getAddresses() {
        return addresses;
    }

    int getRetriesAllDown() {
        return retriesAllDown;
    }

    boolean isTcpKeepAlive() {
        return tcpKeepAlive;
    }

    boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TcpSocketConfiguration)) {
            return false;
        }

        TcpSocketConfiguration that = (TcpSocketConfiguration) o;

        return tcpKeepAlive == that.tcpKeepAlive &&
            tcpNoDelay == that.tcpNoDelay &&
            driver == that.driver &&
            protocol == that.protocol &&
            retriesAllDown == that.retriesAllDown &&
            addresses.equals(that.addresses);
    }

    @Override
    public int hashCode() {
        int result = driver.hashCode();

        result = 31 * result + protocol.hashCode();
        result = 31 * result + addresses.hashCode();
        result = 31 * result + retriesAllDown;
        result = 31 * result + (tcpKeepAlive ? 1 : 0);

        return 31 * result + (tcpNoDelay ? 1 : 0);
    }

    @Override
    public String toString() {
        return "TCP{driver=" + driver +
            ", protocol=" + protocol +
            ", addresses=" + addresses +
            ", retriesAllDown=" + retriesAllDown +
            ", tcpKeepAlive=" + tcpKeepAlive +
            ", tcpNoDelay=" + tcpNoDelay +
            '}';
    }

    static final class Builder {

        private ProtocolDriver driver = ProtocolDriver.MYSQL;

        private HaProtocol protocol = HaProtocol.DEFAULT;

        private final List<NodeAddress> addresses = new ArrayList<>();

        private String host = "";

        private int port = DEFAULT_PORT;

        private boolean tcpKeepAlive = false;

        private boolean tcpNoDelay = true;

        private int retriesAllDown = 10;

        void driver(ProtocolDriver driver) {
            this.driver = driver;
        }

        void protocol(HaProtocol protocol) {
            this.protocol = protocol;
        }

        void host(String host) {
            this.host = host;
        }

        void port(int port) {
            this.port = port;
        }

        void addHost(String host, int port) {
            this.addresses.add(new NodeAddress(host, port));
        }

        void addHost(String host) {
            this.addresses.add(new NodeAddress(host));
        }

        void retriesAllDown(int retriesAllDown) {
            this.retriesAllDown = retriesAllDown;
        }

        void tcpKeepAlive(boolean tcpKeepAlive) {
            this.tcpKeepAlive = tcpKeepAlive;
        }

        void tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
        }

        TcpSocketConfiguration build() {
            List<NodeAddress> addresses;

            if (this.addresses.isEmpty()) {
                requireNonEmpty(host, "Either single host or multiple hosts must be configured");

                addresses = Collections.singletonList(new NodeAddress(host, port));
            } else {
                require(host.isEmpty(), "Either single host or multiple hosts must be configured");

                addresses = InternalArrays.asImmutableList(this.addresses.toArray(new NodeAddress[0]));
            }

            return new TcpSocketConfiguration(
                driver,
                protocol,
                addresses,
                retriesAllDown,
                tcpKeepAlive,
                tcpNoDelay);
        }
    }

    @Override
    public ConnectionStrategy strategy(MySqlConnectionConfiguration configuration) {
        switch (protocol) {
            case REPLICATION:
                ConnectionStrategy.logger.warn(
                    "R2DBC Connection cannot be set to read-only, replication protocol will use the first host");
                return new SingleHostConnectionStrategy(this, configuration);
            case SEQUENTIAL:
                return new MultiHostsConnectionStrategy(this, configuration, false);
            case LOAD_BALANCE:
                return new MultiHostsConnectionStrategy(this, configuration, true);
            default:
                if (ProtocolDriver.MYSQL == driver && addresses.size() == 1) {
                    return new SingleHostConnectionStrategy(this, configuration);
                } else {
                    return new MultiHostsConnectionStrategy(this, configuration, false);
                }
        }
    }
}
