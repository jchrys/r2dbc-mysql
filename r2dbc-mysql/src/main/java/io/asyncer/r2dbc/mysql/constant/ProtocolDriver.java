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
 * Enumeration of driver connection schemes.
 */
public enum ProtocolDriver {

    /**
     * I want to use failover and high availability protocols for each host I set up. If I set a hostname that resolves
     * to multiple IP addresses, the driver should pick one randomly.
     * <p>
     * Recommended in most cases. The hostname is resolved <b>when</b> high availability protocols are applied.
     */
    MYSQL,

    /**
     * I want to use failover and high availability protocols for each IP address. If I set a hostname that resolves to
     * multiple IP addresses, the driver should flatten the list and try to connect to all of IP addresses.
     * <p>
     * The hostname is resolved <b>before</b> high availability protocols are applied.
     */
    DNS_SRV;

    /**
     * Default protocol driver name.
     */
    private static final String STANDARD_NAME = "mysql";

    /**
     * DNS SRV protocol driver name.
     */
    private static final String DNS_SRV_NAME = "mysql+srv";

    public static String standardDriver() {
        return STANDARD_NAME;
    }

    public static boolean supports(String driverName) {
        requireNonNull(driverName, "driverName must not be null");

        switch (driverName) {
            case STANDARD_NAME:
            case DNS_SRV_NAME:
                return true;
            default:
                return false;
        }
    }

    public static ProtocolDriver from(String driverName) {
        requireNonNull(driverName, "driverName must not be null");

        switch (driverName) {
            case STANDARD_NAME:
                return MYSQL;
            case DNS_SRV_NAME:
                return DNS_SRV;
            default:
                throw new IllegalArgumentException("Unknown driver name: " + driverName);
        }
    }
}
