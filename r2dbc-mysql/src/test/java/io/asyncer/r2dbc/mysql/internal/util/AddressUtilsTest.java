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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AddressUtils}.
 */
class AddressUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "1.0.0.0",
        "127.0.0.1",
        "10.11.12.13",
        "192.168.0.0",
        "255.255.255.255",
    })
    void isIpv4(String address) {
        assertThat(AddressUtils.isIpv4(address)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "0.0.0.0",
        " 127.0.0.1 ",
        "01.11.12.13",
        "092.168.0.1",
        "055.255.255.255",
        "g.ar.ba.ge",
        "192.168.0",
        "192.168.0a.0",
        "256.255.255.255",
        "0.255.255.255",
        "::",
        "::1",
        "0:0:0:0:0:0:0:0",
        "0:0:0:0:0:0:0:1",
        "2001:0acd:0000:0000:0000:0000:3939:21fe",
        "2001:acd:0:0:0:0:3939:21fe",
        "2001:0acd:0:0::3939:21fe",
        "2001:0acd::3939:21fe",
        "2001:acd::3939:21fe",
    })
    void isNotIpv4(String address) {
        assertThat(AddressUtils.isIpv4(address)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "::",
        "::1",
        "0:0:0:0:0:0:0:0",
        "0:0:0:0:0:0:0:1",
        "2001:0acd:0000:0000:0000:0000:3939:21fe",
        "2001:acd:0:0:0:0:3939:21fe",
        "2001:0acd:0:0::3939:21fe",
        "2001:0acd::3939:21fe",
        "2001:acd::3939:21fe",
    })
    void isIpv6(String address) {
        assertThat(AddressUtils.isIpv6(address)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        ":1",
        "0:0:0:0:0:0:0",
        "0:0:0:0:0:0:0:0:0",
        "2001:0acd:0000:garb:age0:0000:3939:21fe",
        "2001:0agd:0000:0000:0000:0000:3939:21fe",
        "2001:0acd::0000::21fe",
        "1:2:3:4:5:6:7::9",
        "1::3:4:5:6:7:8:9",
        "::3:4:5:6:7:8:9",
        "1:2::4:5:6:7:8:9",
        "1:2:3:4:5:6::8:9",
        "0.0.0.0",
        "1.0.0.0",
        "127.0.0.1",
        "10.11.12.13",
        "192.168.0.0",
        "255.255.255.255",
    })
    void isNotIpv6(String address) {
        assertThat(AddressUtils.isIpv6(address)).isFalse();
    }

    @ParameterizedTest
    @MethodSource
    void parseAddress(String host, NodeAddress except) {
        assertThat(AddressUtils.parseAddress(host)).isEqualTo(except);
    }

    static Stream<Arguments> parseAddress() {
        return Stream.of(
            Arguments.of("localhost", new NodeAddress("localhost")),
            Arguments.of("localhost:", new NodeAddress("localhost:")),
            Arguments.of("localhost:1", new NodeAddress("localhost", 1)),
            Arguments.of("localhost:3307", new NodeAddress("localhost", 3307)),
            Arguments.of("localhost:65535", new NodeAddress("localhost", 65535)),
            Arguments.of("localhost:65536", new NodeAddress("localhost:65536")),
            Arguments.of("localhost:165536", new NodeAddress("localhost:165536")),
            Arguments.of("a1234", new NodeAddress("a1234")),
            Arguments.of(":1234", new NodeAddress(":1234")),
            Arguments.of("[]:3305", new NodeAddress("[]:3305")),
            Arguments.of("[::1]", new NodeAddress("[::1]")),
            Arguments.of("[::1]:2", new NodeAddress("[::1]", 2)),
            Arguments.of("[::1]:567", new NodeAddress("[::1]", 567)),
            Arguments.of("[::1]:65535", new NodeAddress("[::1]", 65535)),
            Arguments.of("[::1]:65536", new NodeAddress("[::1]:65536")),
            Arguments.of("[::]", new NodeAddress("[::]")),
            Arguments.of("[1::]", new NodeAddress("[1::]")),
            Arguments.of("[::]:3", new NodeAddress("[::]", 3)),
            Arguments.of("[::]:65536", new NodeAddress("[::]:65536")),
            Arguments.of(
                "[2001::2:3307]",
                new NodeAddress("[2001::2:3307]")
            ),
            Arguments.of(
                "[2001::2]:3307",
                new NodeAddress("[2001::2]", 3307)
            ),
            Arguments.of(
                "[a772:8380:7adf:77fd:4d58:d629:a237:0b5e]",
                new NodeAddress("[a772:8380:7adf:77fd:4d58:d629:a237:0b5e]")
            ),
            Arguments.of(
                "[ff19:7c3d:8ddb:c86c:647b:17d6:b64a:7930]:4",
                new NodeAddress("[ff19:7c3d:8ddb:c86c:647b:17d6:b64a:7930]", 4)
            ),
            Arguments.of(
                "[1234:fd2:5621:1:89::45]:567",
                new NodeAddress("[1234:fd2:5621:1:89::45]", 567)
            ),
            Arguments.of(
                "[2001:470:26:12b:9a65:b818:6c96:4271]:65535",
                new NodeAddress("[2001:470:26:12b:9a65:b818:6c96:4271]", 65535)
            ),
            Arguments.of("168.10.0.9", new NodeAddress("168.10.0.9")),
            Arguments.of("168.10.0.9:5", new NodeAddress("168.10.0.9", 5)),
            Arguments.of("168.10.0.9:1234", new NodeAddress("168.10.0.9", 1234)),
            Arguments.of("168.10.0.9:65535", new NodeAddress("168.10.0.9", 65535)),
            // See also https://github.com/asyncer-io/r2dbc-mysql/issues/255
            Arguments.of("my_db", new NodeAddress("my_db")),
            Arguments.of("my_db:6", new NodeAddress("my_db", 6)),
            Arguments.of("my_db:3307", new NodeAddress("my_db", 3307)),
            Arguments.of("my_db:65535", new NodeAddress("my_db", 65535)),
            Arguments.of("db-service", new NodeAddress("db-service")),
            Arguments.of("db-service:7", new NodeAddress("db-service", 7)),
            Arguments.of("db-service:3307", new NodeAddress("db-service", 3307)),
            Arguments.of("db-service:65535", new NodeAddress("db-service", 65535)),
            Arguments.of(
                "region_asia.rds3.some-cloud.com",
                new NodeAddress("region_asia.rds3.some-cloud.com")
            ),
            Arguments.of(
                "region_asia.rds4.some-cloud.com:8",
                new NodeAddress("region_asia.rds4.some-cloud.com", 8)
            ),
            Arguments.of(
                "region_asia.rds5.some-cloud.com:425",
                new NodeAddress("region_asia.rds5.some-cloud.com", 425)
            ),
            Arguments.of(
                "region_asia.rds6.some-cloud.com:65535",
                new NodeAddress("region_asia.rds6.some-cloud.com", 65535)
            )
        );
    }
}
