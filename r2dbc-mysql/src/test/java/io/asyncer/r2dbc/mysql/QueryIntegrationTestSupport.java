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

import com.fasterxml.jackson.core.type.TypeReference;
import io.asyncer.r2dbc.mysql.api.MySqlConnection;
import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.api.MySqlRow;
import io.asyncer.r2dbc.mysql.api.MySqlStatement;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class considers integration tests for queries and data.
 */
abstract class QueryIntegrationTestSupport extends IntegrationTestSupport {

    private static final short UINT8_MAX_VALUE = 255;

    private static final int UINT16_MAX_VALUE = 65535;

    private static final int INT24_MIN_VALUE = -8388608;

    private static final int INT24_MAX_VALUE = 8388607;

    private static final int UINT24_MAX_VALUE = 16777215;

    private static final long UINT32_MAX_VALUE = 4294967295L;

    private static final BigInteger UINT64_MAX_VALUE = new BigInteger("18446744073709551615");

    static final LocalDate MIN_DATE = LocalDate.of(1000, 1, 1);

    static final LocalDate MAX_DATE = LocalDate.of(9999, 12, 31);

    QueryIntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(configuration);
    }

    @SuppressWarnings({ "varargs", "unchecked", "SqlSourceToSinkFlow" })
    <T> void testType(Type type, boolean valueSelect, String defined, T... values) {
        String tdl = String.format("CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value %s)",
            defined);
        complete(connection -> Mono.from(connection.createStatement(tdl).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(Flux.fromIterable(convertOptional(values))
                .concatMap(value -> testOne(connection, type, valueSelect, value.orElse(null)))));
    }

    @Test
    void lightweightPing() {
        complete(connection -> Flux.from(connection.createStatement("/* ping */ SELECT 1").execute())
            .flatMap(result -> result.map(r -> 1))
            .reduce(0, Integer::sum)
            .doOnNext(number -> assertThat(number).isEqualTo(0)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/* PING */ SELECT 1", "SELECT 1 /* ping*/", " /* ping */ SELECT 1",
        "/*ping*/ SELECT 1", "/*to ping or not to ping*/ SELECT 1"
    })
    @SuppressWarnings("SqlSourceToSinkFlow")
    void badLightweightPing(String value) {
        complete(connection -> Flux.from(connection.createStatement(value).execute())
            .flatMap(result -> result.map(r -> r.get(0, Integer.class)))
            .reduce(0, Integer::sum)
            .doOnNext(number -> assertThat(number).isEqualTo(1)));
    }

    @Test
    void tinyintSigned() {
        testType(Byte.class, true, "TINYINT", (byte) 1, (byte) -1, null, Byte.MIN_VALUE, Byte.MAX_VALUE);
    }

    @Test
    void tinyintUnsigned() {
        testType(Short.class, true, "TINYINT UNSIGNED", (short) 1, null, (short) 0, UINT8_MAX_VALUE);
    }

    @Test
    void smallintSigned() {
        testType(Short.class, true, "SMALLINT", (short) 1, (short) -1, null, Short.MIN_VALUE,
            Short.MAX_VALUE);
    }

    @Test
    void smallintUnsigned() {
        testType(Integer.class, true, "SMALLINT UNSIGNED", 1, null, 0, UINT16_MAX_VALUE);
    }

    @Test
    void mediumintSigned() {
        testType(Integer.class, true, "MEDIUMINT", 1, -1, null, INT24_MIN_VALUE, INT24_MAX_VALUE);
    }

    @Test
    void mediumintUnsigned() {
        testType(Integer.class, true, "MEDIUMINT UNSIGNED", 1, null, 0, UINT24_MAX_VALUE);
    }

    @Test
    void intSigned() {
        testType(Integer.class, true, "INT", 1, -1, null, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Test
    void intUnsigned() {
        testType(Long.class, true, "INT UNSIGNED", 1L, null, 0L, UINT32_MAX_VALUE);
    }

    @Test
    void bigintSigned() {
        testType(Long.class, true, "BIGINT", 1L, -1L, null, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    void bigintUnsigned() {
        testType(BigInteger.class, true, "BIGINT UNSIGNED", BigInteger.ONE, null, BigInteger.ZERO,
            UINT64_MAX_VALUE);
    }

    @Test
    void floatSigned() {
        testType(Float.class, true, "FLOAT", null, 1.0f, -1.0f);
    }

    @Test
    void floatUnsigned() {
        testType(Float.class, true, "FLOAT UNSIGNED", null, 1.0f);
    }

    @Test
    void doubleSigned() {
        testType(Double.class, true, "DOUBLE", null, 1.0, -1.0, Double.MIN_VALUE, Double.MIN_NORMAL,
            Double.MAX_VALUE);
    }

    @Test
    void doubleUnsigned() {
        testType(Double.class, true, "DOUBLE UNSIGNED", null, 1.0, Double.MIN_VALUE, Double.MIN_NORMAL,
            Double.MAX_VALUE);
    }

    @Test
    void decimalSigned() {
        testType(BigDecimal.class, true, "DECIMAL(10,2)", null, new BigDecimal("1.00"),
            new BigDecimal("-1.00"), new BigDecimal("1.99"));
    }

    @Test
    void decimalUnsigned() {
        testType(BigDecimal.class, true, "DECIMAL(10,2) UNSIGNED", null, new BigDecimal("1.00"),
            new BigDecimal("1.99"));
    }

    @Test
    void year() {
        testType(Year.class, true, "YEAR", null, Year.of(1901), Year.of(2155));
        testType(Short.class, true, "YEAR", null, (short) 1901, (short) 2155);
    }

    @Test
    void varchar() {
        testType(String.class, true, "VARCHAR(50)", "", null, "data", "1' \\ OR\r\n \"1\" = '1");
        testType(String.class, true, "CHAR(50)", "", null, "data", "1' \\ OR\r\n \"1\" = '1");
    }

    @Test
    void enumerable() {
        testType(String.class, true, "ENUM('ONE','TWO','THREE')", null, "ONE", "TWO", "THREE");
        testType(EnumData.class, true, "ENUM('ONE','TWO','THREE')", null, EnumData.ONE, EnumData.TWO,
            EnumData.THREE);
    }

    /**
     * See <a href="https://github.com/mirromutth/r2dbc-mysql/issues/62">Issue 62</a> .
     */
    @Test
    void varbinary() {
        testType(byte[].class, true, "VARBINARY(50)", new byte[0], null, new byte[] { 1, 2, 3, 4, 5 });
        testType(ByteBuffer.class, true, "VARBINARY(50)", ByteBuffer.allocate(0), null,
            ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 }));
    }

    @Test
    void text() {
        testType(byte[].class, true, "TEXT", null, new byte[0], new byte[] { 1, 2, 3, 4, 5 });
        testType(ByteBuffer.class, true, "TEXT", null, ByteBuffer.allocate(0),
            ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 }));
    }

    @Test
    void bit() {
        testType(Boolean.class, true, "BIT(1)", null, false, true);
        testType(BitSet.class, true, "BIT(16)", null, BitSet.valueOf(new byte[] { (byte) 0xEF, (byte) 0xCD }),
            BitSet.valueOf(new byte[0]), BitSet.valueOf(new byte[] { 0, 0 }),
            BitSet.valueOf(new byte[] { (byte) 0xCD }));
        testType(BitSet.class, true, "BIT(64)", null, BitSet.valueOf(new long[0]),
            BitSet.valueOf(new long[] { 0 }), BitSet.valueOf(new long[] { 0xEFCD }),
            BitSet.valueOf(new long[] { Long.MAX_VALUE }));
        testType(BitSet.class, false, "BIT(64)", BitSet.valueOf(new long[] { -1 }),
            BitSet.valueOf(new long[] { Long.MIN_VALUE }));
        testType(byte[].class, false, "BIT(16)", null, new byte[] { (byte) 0xCD, (byte) 0xEF },
            new byte[] { 0, 0 });
        testType(ByteBuffer.class, false, "BIT(16)", null, ByteBuffer.wrap(new byte[] { 1, 2 }),
            ByteBuffer.wrap(new byte[] { 0, 0 }));
    }

    @Test
    void bool() {
        testType(Boolean.class, true, "BOOLEAN", null, false, true);
    }

    @SuppressWarnings("unchecked")
    @Test
    void set() {
        Type stringSet = new TypeReference<Set<String>>() { }.getType();
        Type enumSet = new TypeReference<Set<EnumData>>() { }.getType();

        testType(String.class, true, "SET('ONE','TWO','THREE')", null, "ONE,TWO,THREE", "ONE", "",
            "ONE,THREE");
        testType(String[].class, true, "SET('ONE','TWO','THREE')", null,
            new String[] { "ONE", "TWO", "THREE" },
            new String[] { "ONE" },
            new String[] { },
            new String[] { "ONE", "THREE" });
        testType(stringSet, true, "SET('ONE','TWO','THREE')", null,
            new HashSet<>(Arrays.asList("ONE", "TWO", "THREE")),
            Collections.singleton("ONE"),
            Collections.emptySet(),
            new HashSet<>(Arrays.asList("ONE", "THREE")));
        testType(enumSet, true, "SET('ONE','TWO','THREE')", null,
            EnumSet.allOf(EnumData.class),
            EnumSet.of(EnumData.ONE),
            EnumSet.noneOf(EnumData.class),
            EnumSet.of(EnumData.ONE, EnumData.THREE));
    }

    @DisabledIf("envIsLessThanMySql578OrMariaDb102")
    @Test
    void json() {
        testType(String.class, false, "JSON", null, "{\"data\": 1}", "[\"data\", 1]", "1", "null",
            "\"R2DBC\"", "2.56");

    }

    @Test
    void date() {
        testType(LocalDate.class, true, "DATE", null, MIN_DATE, LocalDate.of(2020, 1, 1), MAX_DATE);
    }

    @Test
    void time() {
        LocalTime minTime = LocalTime.MIDNIGHT;
        LocalTime aTime = LocalTime.of(10, 5, 28);
        LocalTime maxTime = LocalTime.of(23, 59, 59);
        Duration minDuration = Duration.ofSeconds(-TimeUnit.HOURS.toSeconds(838) -
            TimeUnit.MINUTES.toSeconds(59) - 59);
        Duration aDuration = Duration.ofSeconds(1854672);
        Duration maxDuration = Duration.ofSeconds(TimeUnit.HOURS.toSeconds(838) +
            TimeUnit.MINUTES.toSeconds(59) + 59);

        testType(LocalTime.class, true, "TIME", null, minTime, aTime, maxTime);
        testType(Duration.class, true, "TIME", null, minDuration, aDuration, maxDuration);
    }

    @DisabledIf("envIsLessThanMySql56")
    @Test
    void time6() {
        LocalTime smallTime = LocalTime.of(0, 0, 0, 1000);
        LocalTime aTime = LocalTime.of(10, 5, 28, 5_410_000);
        LocalTime maxTime = LocalTime.of(23, 59, 59, 999_999_000);
        Duration smallDuration = Duration.ofSeconds(-TimeUnit.HOURS.toSeconds(838) -
            TimeUnit.MINUTES.toSeconds(59) - 58, -999_999_000);
        Duration aDuration = Duration.ofSeconds(1854672, 5_410_000);
        Duration bigDuration = Duration.ofSeconds(TimeUnit.HOURS.toSeconds(838) +
            TimeUnit.MINUTES.toSeconds(59) + 58, 999_999_000);

        testType(LocalTime.class, true, "TIME(6)", null, smallTime, aTime, maxTime);
        testType(Duration.class, true, "TIME(6)", null, smallDuration, aDuration, bigDuration);
    }

    @Test
    void timeDuration() {
        String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value TIME)";
        complete(connection -> Mono.from(connection.createStatement(tdl).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(timeDurationCases()
                .concatMap(pair -> testTimeDuration(connection, pair.getT1(), pair.getT2()))));
    }

    @DisabledIf("envIsLessThanMySql56")
    @Test
    void timeDuration6() {
        String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value TIME(6))";
        complete(connection -> Mono.from(connection.createStatement(tdl).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(timeDuration6Cases()
                .concatMap(pair -> testTimeDuration(connection, pair.getT1(), pair.getT2()))));
    }

    @Test
    void dateTime() {
        LocalDateTime minDateTime = LocalDateTime.of(1000, 1, 1, 0, 0, 0);
        LocalDateTime aDateTime = LocalDateTime.of(2020, 5, 12, 8, 4, 10);
        LocalDateTime maxDateTime = LocalDateTime.of(9999, 12, 31, 23, 59, 59);

        testType(LocalDateTime.class, true, "DATETIME", null, minDateTime, aDateTime, maxDateTime);
    }

    @DisabledIf("envIsLessThanMySql56")
    @Test
    void dateTime6() {
        LocalDateTime smallDateTime = LocalDateTime.of(1000, 1, 1, 0, 0, 0, 1000);
        LocalDateTime aDateTime = LocalDateTime.of(2020, 5, 12, 8, 4, 10, 5_4210_000);
        LocalDateTime maxDateTime = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999_999_000);

        testType(LocalDateTime.class, true, "DATETIME(6)", null, smallDateTime, aDateTime, maxDateTime);
    }

    @Test
    void timestamp() {
        LocalDateTime minTimestamp = LocalDateTime.of(1970, 1, 3, 0, 0, 0);
        LocalDateTime aTimestamp = LocalDateTime.of(2020, 5, 12, 8, 4, 10);
        LocalDateTime maxTimestamp = LocalDateTime.of(2038, 1, 15, 23, 59, 59);

        // TIMESTAMP must not be null when database version less than 8.0
        testType(LocalDateTime.class, true, "TIMESTAMP", minTimestamp, aTimestamp, maxTimestamp);
    }

    @DisabledIf("envIsLessThanMySql56")
    @Test
    void timestamp6() {
        LocalDateTime minTimestamp = LocalDateTime.of(1970, 1, 3, 0, 0, 0, 1000);
        LocalDateTime aTimestamp = LocalDateTime.of(2020, 5, 12, 8, 4, 10, 5_4210_000);
        LocalDateTime maxTimestamp = LocalDateTime.of(2038, 1, 15, 23, 59, 59, 999_999_000);

        // TIMESTAMP must not be null when database version less than 8.0
        testType(LocalDateTime.class, true, "TIMESTAMP(6)", minTimestamp, aTimestamp, maxTimestamp);
    }

    /**
     * SQL query {@code SELECT 1} is a ping approach which is cross-database (maybe some are not supported)
     * and is easy to understand.
     * <p>
     * Note: looks like {@code SELECT 1} result value type returned by the MySQL server is BIGINT, try using
     * Number.class to eliminate {@code assertEquals} fail because of the value type.
     */
    @Test
    void selectOne() {
        complete(connection -> Mono.from(connection.createStatement("SELECT 1").execute())
            .flatMapMany(result -> result.map((row, metadata) -> row.get(0, Number.class)))
            .doOnNext(number -> assertThat(number.intValue()).isEqualTo(1))
            .reduce((x, y) -> Math.addExact(x.intValue(), y.intValue()))
            .doOnNext(number -> assertThat(number.intValue()).isEqualTo(1)));
    }

    /**
     * See <a href="https://github.com/mirromutth/r2dbc-mysql/issues/45">Issue 45</a> .
     */
    @Test
    void selectFromOtherDatabase() {
        complete(conn -> Flux.from(conn.createStatement("SELECT * FROM `information_schema`.`innodb_trx`")
                .execute())
            .flatMap(result -> result.map((row, metadata) -> row.get(0))));
    }

    /**
     * See <a href="https://github.com/mirromutth/r2dbc-mysql/issues/50">Issue 50</a> .
     */
    @Test
    void multiQueries() {
        String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT," +
            "email VARCHAR(190),password VARCHAR(190),updated_at DATETIME,created_at DATETIME)";
        complete(connection -> Mono.from(connection.createStatement(tdl).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(Flux.range(0, 10))
            .flatMap(it -> Flux.from(connection.createStatement("INSERT INTO test VALUES" +
                    "(DEFAULT,?,?,NOW(),NOW())")
                .bind(0, String.format("integration-test%d@mail.com", it))
                .bind(1, "******")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .reduce(Math::addExact)
            .doOnNext(it -> assertThat(it).isEqualTo(10))
            .then(Mono.from(connection.createStatement("SELECT email FROM test").execute()))
            .flatMapMany(result -> result.map((row, metadata) -> row.get(0, String.class)))
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(IntStream.range(0, 10)
                .mapToObj(i -> String.format("integration-test%d@mail.com", i))
                .collect(Collectors.toList()))));
    }

    /**
     * See <a href="https://github.com/mirromutth/r2dbc-mysql/issues/69">Issue 69</a> .
     */
    @Test
    void consumePortion() {
        complete(connection -> Mono.from(connection.createStatement("CREATE TEMPORARY TABLE test" +
                "(id INT PRIMARY KEY AUTO_INCREMENT,value INT)").execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("INSERT INTO test(`value`) VALUES (1),(2),(3),(4),(5)")
                .execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("SELECT value FROM test WHERE id > ?")
                .bind(0, 0)
                .execute()))
            .flatMapMany(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE))).take(3)
            .concatWith(Mono.from(connection.createStatement("SELECT value FROM test WHERE id > ?")
                    .bind(0, 0)
                    .execute())
                .flatMapMany(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE))).take(2))
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(1, 2, 3, 1, 2))));
    }

    /**
     * <a href="https://github.com/mirromutth/r2dbc-mysql/issues/73">Issue 73</a> .
     */
    @Test
    void ignoreResult() {
        String tdl = "CREATE TEMPORARY TABLE test(id INT PRIMARY KEY AUTO_INCREMENT,value INT)";
        List<Integer> values = new ArrayList<>();
        complete(connection -> Mono.from(connection.createStatement(tdl).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .then(Mono.from(connection.createStatement("INSERT INTO test(`value`) " +
                "VALUES (1),(2),(3),(4),(5)").execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(Flux.merge(
                Flux.from(connection.createStatement("SELECT value FROM test WHERE id > ?")
                        .bind(0, 0).execute())
                    .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
                    .doOnNext(values::add),
                connection.createStatement("BAD GRAMMAR").execute()
            ).onErrorResume(ignored -> Flux.empty()))
            .thenMany(connection.createStatement("SELECT value FROM test ORDER BY id DESC LIMIT ?,?")
                .bind(0, 2)
                .bind(1, 5)
                .execute())
            .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.TYPE)))
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Arrays.asList(3, 2, 1))));
        assertThat(values).isEqualTo(Arrays.asList(1, 2, 3, 4, 5));
    }

    /**
     * See <a href="https://github.com/mirromutth/r2dbc-mysql/issues/90">Issue 90</a> .
     */
    @Test
    void foundRows() {
        int value = 10;
        complete(connection -> Flux.from(connection.createStatement("CREATE TEMPORARY TABLE test" +
                    "(id INT PRIMARY KEY AUTO_INCREMENT,value INT)")
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test VALUES(DEFAULT,?)")
                .bind(0, value).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("UPDATE test SET value=? WHERE id=?")
                .bind(0, value).bind(1, 1).execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .reduce(Math::addExact)
            .doOnNext(it -> assertThat(it).isEqualTo(1)));
    }

    /**
     * See also:
     *
     * <ul><li><a href="https://github.com/mirromutth/r2dbc-mysql/issues/156">Issue 156</a></li><li>
     * <a href="https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html">INSERT-ON-DUPLICATE</a></li>
     * <li><a href="https://github.com/mirromutth/r2dbc-mysql/issues/90">Issue 90</a></li></ul>
     * <p>
     * The {@code Capability.FOUND_ROWS} has been enabled by default, the first number of affected rows is
     * 1, thesecond one is 2, and the third is 1 (found/touched 1, changed 0).
     */
    @Test
    void insertOnDuplicate() {
        complete(connection -> Flux.from(connection.createStatement("CREATE TEMPORARY TABLE test" +
                    "(id INT PRIMARY KEY,value INT)")
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .thenMany(connection.createStatement("INSERT INTO test VALUES(?,?) " +
                    "ON DUPLICATE KEY UPDATE value=?")
                .bind(0, 1)
                .bind(1, 10)
                .bind(2, 20)
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(it -> assertThat(it).isOne())
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, 1)
                .execute())
            .flatMap(QueryIntegrationTestSupport::extractFirstInteger)
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Collections.singletonList(10)))
            .thenMany(connection.createStatement("INSERT INTO test VALUES(?,?) " +
                    "ON DUPLICATE KEY UPDATE value=?")
                .bind(0, 1)
                .bind(1, 10)
                .bind(2, 20)
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(it -> assertThat(it).isEqualTo(2))
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, 1)
                .execute())
            .flatMap(QueryIntegrationTestSupport::extractFirstInteger)
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Collections.singletonList(20)))
            .thenMany(connection.createStatement("INSERT INTO test VALUES(?,?) " +
                    "ON DUPLICATE KEY UPDATE value=?")
                .bind(0, 1)
                .bind(1, 10)
                .bind(2, 20)
                .execute())
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(it -> assertThat(it).isOne())
            .thenMany(connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, 1)
                .execute())
            .flatMap(QueryIntegrationTestSupport::extractFirstInteger)
            .collectList()
            .doOnNext(it -> assertThat(it).isEqualTo(Collections.singletonList(20))));
    }

    /**
     * ref: <a href="https://github.com/asyncer-io/r2dbc-mysql/issues/91">Issue 91</a>
     */
    @DisabledIf("envIsLessThanMySql578OrMariaDb102")
    @Test
    void testUnionQueryWithJsonColumnDecodedAsString() {
        complete(connection ->
            Flux.from(connection.createStatement(
                        "CREATE TEMPORARY TABLE test1 (id INT PRIMARY KEY AUTO_INCREMENT, value JSON)")
                    .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .thenMany(connection.createStatement("INSERT INTO test1 VALUES(DEFAULT, ?)")
                    .bind(0, "{\"id\":1,\"name\":\"iron man\"}")
                    .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .doOnNext(it -> assertThat(it).isEqualTo(1))
                .thenMany(connection.createStatement(
                        "CREATE TEMPORARY TABLE test2 (id INT PRIMARY KEY AUTO_INCREMENT, value JSON)")
                    .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .thenMany(connection.createStatement("INSERT INTO test2 VALUES(DEFAULT, ?)")
                    .bind(0,
                        "[{\"id\":2,\"name\":\"bat man\"},{\"id\":3,\"name\":\"super man\"}]")
                    .execute())
                .flatMap(IntegrationTestSupport::extractRowsUpdated)
                .doOnNext(it -> assertThat(it).isEqualTo(1))
                .thenMany(
                    connection.createStatement("SELECT value FROM test1 UNION SELECT value FROM test2")
                        .execute())
                .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class))
                    .map(QueryIntegrationTestSupport::parseJson)
                    .collectList()
                    .doOnNext(it -> assertThat(it).isEqualTo(
                        Arrays.asList(
                            parseJson("{\"id\": 1, \"name\": \"iron man\"}"),
                            parseJson(
                                "[{\"id\": 2, \"name\": \"bat man\"}, {\"id\": 3, \"name\": \"super man\"}]"
                            )
                        )
                    )))
        );
    }

    @Test
    @DisabledIf("envIsLessThanMySql574OrMariaDb1011")
    void setStatementTimeoutTest() {
        final String sql = "SELECT COUNT(*) " +
                           "FROM information_schema.tables a cross join " +
                           "information_schema.tables b cross join " +
                           "information_schema.tables c cross join " +
                           "information_schema.tables d cross join " +
                           "information_schema.tables e";

        timeout(connection -> connection.setStatementTimeout(Duration.ofMillis(500))
                .then(Mono.from(connection.createStatement(sql).execute()))
                .flatMapMany(result -> Mono.from(result.map((row, metadata) -> row.get(0, String.class))))
                .collectList()
                .doOnNext(System.out::println)
        );
    }

    private static JsonNode parseJson(String json) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Flux<Integer> extractFirstInteger(MySqlResult result) {
        return Flux.from(result.map((row, metadata) -> row.get(0, Integer.class)));
    }

    @SuppressWarnings("unchecked")
    private static <T> Flux<Optional<T>> extractOptionalField(MySqlResult result, Type type) {
        if (type instanceof Class<?>) {
            return Flux.from(result.map((row, metadata) -> Optional.ofNullable(row.get(0, (Class<T>) type))));
        }
        return Flux.from(result.map((row, metadata) ->
            Optional.ofNullable(((MySqlRow) row).get(0, (ParameterizedType) type))));
    }

    private static Mono<Void> testTimeDuration(MySqlConnection connection, Duration origin, LocalTime time) {
        return Mono.from(connection.createStatement("INSERT INTO test VALUES(DEFAULT,?)")
                .bind(0, origin)
                .returnGeneratedValues("id")
                .execute())
            .flatMapMany(QueryIntegrationTestSupport::extractFirstInteger)
            .concatMap(id -> connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, id)
                .execute())
            .<Optional<LocalTime>>flatMap(r -> extractOptionalField(r, LocalTime.class))
            .map(Optional::get)
            .doOnNext(t -> assertThat(t).isEqualTo(time))
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0")
                .execute()))
            .flatMap(QueryIntegrationTestSupport::extractRowsUpdated)
            .then();
    }

    private static Flux<Tuple2<Duration, LocalTime>> timeDurationCases() {
        Duration negativeOne = Duration.ofSeconds(-1);
        Duration negativeOneDay = Duration.ofSeconds(-TimeUnit.DAYS.toSeconds(1) - 1);
        Duration oneDayOneSecond = Duration.ofSeconds(TimeUnit.DAYS.toSeconds(1) + 1);
        LocalTime lastSecond = LocalTime.of(23, 59, 59);
        LocalTime firstSecond = LocalTime.of(0, 0, 1);

        return Flux.just(
            Tuples.of(negativeOne, lastSecond),
            Tuples.of(negativeOneDay, lastSecond),
            Tuples.of(oneDayOneSecond, firstSecond)
        );
    }

    private static Flux<Tuple2<Duration, LocalTime>> timeDuration6Cases() {
        long seconds = TimeUnit.HOURS.toSeconds(8) + TimeUnit.MINUTES.toSeconds(5) + 45;
        Duration one = Duration.ofSeconds(0, -1000);
        Duration aDuration = Duration.ofSeconds(seconds, 45_610_000);
        Duration oneDay = Duration.ofSeconds(-TimeUnit.DAYS.toSeconds(1), -1000);
        Duration oneDayOne = Duration.ofSeconds(TimeUnit.DAYS.toSeconds(1), 1000);
        LocalTime aTime = LocalTime.of(8, 5, 45, 45_610_000);
        LocalTime lastMicro = LocalTime.of(23, 59, 59, 999_999_000);
        LocalTime firstMicro = LocalTime.of(0, 0, 0, 1000);

        return Flux.just(
            Tuples.of(one, lastMicro),
            Tuples.of(oneDay, lastMicro),
            Tuples.of(oneDayOne, firstMicro), Tuples.of(aDuration, aTime)
        );
    }

    private static <T> List<Optional<T>> convertOptional(T[] values) {
        return Arrays.stream(values).map(Optional::ofNullable).collect(Collectors.toList());
    }

    private static <T> Mono<Void> testOne(MySqlConnection connection, Type type, boolean selectValue,
        @Nullable T value) {
        MySqlStatement insert = connection.createStatement("INSERT INTO test VALUES(DEFAULT,?)");

        if (value == null) {
            if (type instanceof Class<?>) {
                insert.bindNull(0, (Class<?>) type);
            } else {
                insert.bindNull(0, (Class<?>) ((ParameterizedType) type).getRawType());
            }
        } else {
            if (value instanceof ByteBuffer) {
                insert.bind(0, ((ByteBuffer) value).slice());
            } else {
                insert.bind(0, value);
            }
        }

        return Mono.from(insert.returnGeneratedValues("id").execute())
            .flatMapMany(QueryIntegrationTestSupport::extractFirstInteger)
            .collectList()
            .map(ids -> {
                assertThat(ids).hasSize(1).first().isNotNull();
                return ids.get(0);
            })
            .flatMap(id -> Mono.from(connection.createStatement("SELECT value FROM test WHERE id=?")
                .bind(0, id)
                .execute()))
            .<Optional<T>>flatMapMany(r -> extractOptionalField(r, type))
            .collectList()
            .map(list -> {
                assertThat(list).hasSize(1);
                return list.get(0);
            })
            .doOnNext(data -> {
                if (data.isPresent()) {
                    T t = data.get();
                    Class<?> clazz = t.getClass();
                    if (clazz.isArray()) {
                        if (clazz == byte[].class) {
                            assertThat(value).isInstanceOfSatisfying(byte[].class,
                                it -> assertThat(it).isEqualTo(t));
                        } else {
                            assertThat(value).isInstanceOfSatisfying(Object[].class,
                                it -> assertThat(it).isEqualTo(t));
                        }
                    } else {
                        assertThat(value).isEqualTo(t);
                    }
                } else {
                    assertThat(value).isNull();
                }
            })
            .as(it -> {
                if (selectValue) {
                    MySqlStatement valueSelect;

                    if (value == null) {
                        valueSelect = connection.createStatement("SELECT value FROM test " +
                            "WHERE value IS NULL");
                    } else {
                        valueSelect = connection.createStatement("SELECT value FROM test WHERE value=?");

                        if (value instanceof ByteBuffer) {
                            valueSelect.bind(0, ((ByteBuffer) value).slice());
                        } else {
                            valueSelect.bind(0, value);
                        }
                    }

                    return it.then(Mono.from(valueSelect.execute()))
                        .<Optional<T>>flatMapMany(r -> extractOptionalField(r, type))
                        .collectList()
                        .map(list -> {
                            assertThat(list).hasSize(1);
                            return list.get(0);
                        })
                        .doOnNext(data -> {
                            if (data.isPresent()) {
                                T t = data.get();
                                Class<?> clazz = t.getClass();
                                if (clazz.isArray()) {
                                    if (clazz == byte[].class) {
                                        assertThat(value).isInstanceOfSatisfying(byte[].class,
                                            item -> assertThat(item).isEqualTo(t));
                                    } else {
                                        assertThat(value).isInstanceOfSatisfying(Object[].class,
                                            item -> assertThat(item).isEqualTo(t));
                                    }
                                } else {
                                    assertThat(value).isEqualTo(t);
                                }
                            } else {
                                assertThat(value).isNull();
                            }
                        });
                }
                return it;
            })
            .then(Mono.from(connection.createStatement("DELETE FROM test WHERE id>0").execute()))
            .flatMap(IntegrationTestSupport::extractRowsUpdated)
            .doOnNext(u -> assertThat(u).isEqualTo(1))
            .then();
    }

    enum EnumData {

        ONE,
        TWO,
        THREE;

        @Override
        public final String toString() {
            throw new IllegalStateException("Special enum class, can not to string");
        }
    }
}
