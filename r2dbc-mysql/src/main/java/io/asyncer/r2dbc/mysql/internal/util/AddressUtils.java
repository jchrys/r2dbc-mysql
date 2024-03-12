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

package io.asyncer.r2dbc.mysql.internal.util;

import io.asyncer.r2dbc.mysql.internal.NodeAddress;

import java.net.InetSocketAddress;
import java.util.regex.Pattern;

/**
 * A utility for processing host/address.
 */
public final class AddressUtils {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
        "^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.)" +
            "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}" +
            "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");

    private static final Pattern IPV6_PATTERN = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");

    private static final Pattern IPV6_COMPRESSED_PATTERN = Pattern.compile(
        "^((([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,5})?)::(([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,5})?))$");

    private static final int IPV6_COLONS = 7;

    /**
     * Checks if the host is an address of IP version 4.
     *
     * @param host the host should be checked.
     * @return if is IPv4.
     */
    public static boolean isIpv4(String host) {
        // Maybe use faster matches instead of regex?
        return IPV4_PATTERN.matcher(host).matches();
    }

    /**
     * Checks if the host is an address of IP version 6.
     *
     * @param host the host should be checked.
     * @return if is IPv6.
     */
    public static boolean isIpv6(String host) {
        // Maybe use faster matches instead of regex?
        return IPV6_PATTERN.matcher(host).matches() || isIpv6Compressed(host);
    }

    /**
     * Parses a host to an {@link NodeAddress}, the {@code host} may contain port or not. If the {@code host} does
     * not contain a valid port, the default port {@code 3306} will be used. The {@code host} can be an IPv6, IPv4 or
     * host address. e.g. [::1]:3301, [::1], 127.0.0.1, host-name:3302
     * <p>
     * Note: It will not check if the host is a valid address. e.g. IPv6 address should be enclosed in square brackets,
     * hostname should not contain an underscore, etc.
     *
     * @param host the {@code host} should be parsed as socket address.
     * @return the parsed and unresolved {@link InetSocketAddress}
     */
    public static NodeAddress parseAddress(String host) {
        int len = host.length();
        int index;

        for (index = len - 1; index > 0; --index) {
            char ch = host.charAt(index);

            if (ch == ':') {
                break;
            } else if (ch < '0' || ch > '9') {
                return new NodeAddress(host);
            }
        }

        if (index == 0) {
            // index == 0, no host before number whatever host[0] is a colon or not, may be a hostname "a1234"
            return new NodeAddress(host);
        }

        int colonLen = len - index;

        if (colonLen < 2 || colonLen > 6) {
            // 1. no port after colon, not a port, may be an IPv6 address like "::"
            // 2. length of port > 5, max port is 65535, invalid port
            return new NodeAddress(host);
        }

        if (host.charAt(index - 1) == ']' && host.charAt(0) == '[') {
            // Seems like an IPv6 with port
            if (index <= 2) {
                // Host/Address must not be empty
                return new NodeAddress(host);
            }

            int port = parsePort(host, index + 1, len);

            if (port > 0xFFFF) {
                return new NodeAddress(host);
            }

            return new NodeAddress(host.substring(0, index), port);
        }

        int colonIndex = index;

        // IPv4 or host should not contain a colon, IPv6 should be enclosed in square brackets
        for (--index; index >= 0; --index) {
            if (host.charAt(index) == ':') {
                return new NodeAddress(host);
            }
        }

        int port = parsePort(host, colonIndex + 1, len);

        if (port > 0xFFFF) {
            return new NodeAddress(host);
        }

        return new NodeAddress(host.substring(0, colonIndex), port);
    }

    private static boolean isIpv6Compressed(String host) {
        int length = host.length();
        int colons = 0;

        for (int i = 0; i < length; ++i) {
            if (host.charAt(i) == ':') {
                ++colons;
            }
        }

        // Maybe use faster matches instead of regex?
        return colons <= IPV6_COLONS && IPV6_COMPRESSED_PATTERN.matcher(host).matches();
    }

    private static int parsePort(String input, int start, int end) {
        int r = 0;

        for (int i = start; i < end; ++i) {
            r = r * 10 + (input.charAt(i) - '0');
        }

        return r;
    }

    private AddressUtils() {
    }
}
