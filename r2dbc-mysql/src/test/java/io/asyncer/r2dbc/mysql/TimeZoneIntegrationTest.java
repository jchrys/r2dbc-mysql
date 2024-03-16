package io.asyncer.r2dbc.mysql;

import com.zaxxer.hikari.HikariDataSource;
import io.asyncer.r2dbc.mysql.api.MySqlResult;
import io.asyncer.r2dbc.mysql.internal.util.TestContainerExtension;
import io.asyncer.r2dbc.mysql.internal.util.TestServerUtil;
import org.assertj.core.data.TemporalUnitOffset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuple4;
import reactor.util.function.Tuple6;
import reactor.util.function.Tuple8;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Integration tests for aligning time zone configuration options with jdbc.
 */
@ExtendWith(TestContainerExtension.class)
@Isolated
class TimeZoneIntegrationTest {

    // Earlier versions did not support microseconds, so it is almost always within 1 second, extending to
    // 2 seconds due to network reasons
    private static final TemporalUnitOffset TINY_WITHIN = within(2, ChronoUnit.SECONDS);

    private static TimeZone defaultTimeZone;

    @BeforeAll
    static void setUpTimeZone() {
        defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+9:30"));
    }

    @AfterAll
    static void tearDownTimeZone() {
        TimeZone.setDefault(defaultTimeZone);
    }

    @BeforeEach
    void setUp() {
        String tdl = "CREATE TABLE IF NOT EXISTS test_time_zone ("
            + "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,"
            + "data1 DATETIME" + dateTimeSuffix(false) + " NOT NULL,"
            + "data2 TIMESTAMP" + dateTimeSuffix(false) + " NOT NULL)";

        MySqlConnectionFactory.from(configuration(Function.identity())).create()
            .flatMapMany(connection -> connection.createStatement(tdl)
                .execute()
                .flatMap(MySqlResult::getRowsUpdated)
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .then(connection.close()))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @AfterEach
    void tearDown() {
        MySqlConnectionFactory.from(configuration(Function.identity())).create()
            .flatMapMany(connection -> connection.createStatement("DROP TABLE IF EXISTS test_time_zone")
                .execute()
                .flatMap(MySqlResult::getRowsUpdated)
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .then(connection.close()))
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource
    void alignDateTimeFunction(boolean instants, String timeZone, boolean force) {
        String selectQuery = "SELECT CURRENT_TIMESTAMP" + dateTimeSuffix(true) +
            ", NOW" + dateTimeSuffix(true) +
            ", CURRENT_TIME" + dateTimeSuffix(true) +
            ", CURRENT_DATE()";
        MySqlConnectionConfiguration config = configuration(builder -> builder
            .preserveInstants(instants)
            .connectionTimeZone(timeZone)
            .forceConnectionTimeZoneToSession(force));
        JdbcTemplate jdbc = jdbc(config);

        Tuple4<
            Tuple3<LocalDateTime, ZonedDateTime, OffsetDateTime>,
            Tuple3<LocalDateTime, ZonedDateTime, OffsetDateTime>,
            Tuple3<LocalTime, OffsetTime, Duration>,
            LocalDate
            > expectedTuples = jdbc.query(selectQuery, (rs, ignored) -> Tuples.of(
            Tuples.of(
                requireNonNull(rs.getObject(1, LocalDateTime.class)),
                requireNonNull(rs.getObject(1, ZonedDateTime.class)),
                requireNonNull(rs.getObject(1, OffsetDateTime.class))
            ),
            Tuples.of(
                requireNonNull(rs.getObject(2, LocalDateTime.class)),
                requireNonNull(rs.getObject(2, ZonedDateTime.class)),
                requireNonNull(rs.getObject(2, OffsetDateTime.class))
            ),
            Tuples.of(
                rs.getObject(3, LocalTime.class),
                rs.getObject(3, OffsetTime.class),
                rs.getObject(3, Duration.class)
            ),
            rs.getObject(4, LocalDate.class)
        )).get(0);

        MySqlConnectionFactory.from(config).create()
            .flatMapMany(connection -> connection.createStatement(selectQuery)
                .execute()
                .flatMap(result -> result.map((row, metadata) -> Tuples.of(
                    Tuples.of(
                        requireNonNull(row.get(0, LocalDateTime.class)),
                        requireNonNull(row.get(0, ZonedDateTime.class)),
                        requireNonNull(row.get(0, OffsetDateTime.class)),
                        requireNonNull(row.get(0, Instant.class))
                    ),
                    Tuples.of(
                        requireNonNull(row.get(1, LocalDateTime.class)),
                        requireNonNull(row.get(1, ZonedDateTime.class)),
                        requireNonNull(row.get(1, OffsetDateTime.class)),
                        requireNonNull(row.get(1, Instant.class))
                    ),
                    Tuples.of(
                        requireNonNull(row.get(2, LocalTime.class)),
                        requireNonNull(row.get(2, OffsetTime.class)),
                        requireNonNull(row.get(2, Duration.class))
                    ),
                    requireNonNull(row.get(3, LocalDate.class))
                )))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .assertNext(data -> {
                assertDateTimeTuples(data.getT1(), expectedTuples.getT1());
                assertDateTimeTuples(data.getT2(), expectedTuples.getT2());

                assertThat(data.getT3().getT1()).isCloseTo(expectedTuples.getT3().getT1(), TINY_WITHIN);
                assertThat(data.getT3().getT2().getOffset())
                    .isEqualTo(expectedTuples.getT3().getT2().getOffset());
                assertThat(data.getT3().getT2()).isCloseTo(expectedTuples.getT3().getT2(), TINY_WITHIN);
                assertThat(data.getT3().getT3()).isCloseTo(expectedTuples.getT3().getT3(), Duration.ofSeconds(2));

                // If the test case is run close to UTC midnight, it may fail, just run it again
                assertThat(data.getT4()).isEqualTo(expectedTuples.getT4());
            })
            .verifyComplete();

        requireNonNull((HikariDataSource) jdbc.getDataSource()).close();
    }

    @ParameterizedTest
    @MethodSource
    void alignSendAndReceiveTimeZoneOption(boolean instants, String timeZone, boolean force, Temporal now) {
        String insertQuery = "INSERT INTO test_time_zone VALUES (DEFAULT, ?, ?)";
        String selectQuery = "SELECT data1, data2 FROM test_time_zone";
        MySqlConnectionConfiguration config = configuration(builder -> builder
            .preserveInstants(instants)
            .connectionTimeZone(timeZone)
            .forceConnectionTimeZoneToSession(force));
        JdbcTemplate jdbc = jdbc(config);

        assertThat(jdbc.update(insertQuery, now, now)).isOne();
        Tuple6<LocalDateTime, LocalDateTime, ZonedDateTime, ZonedDateTime, OffsetDateTime,
            OffsetDateTime> expectedTuples = jdbc.query(selectQuery, (rs, ignored) -> Tuples.of(
            requireNonNull(rs.getObject(1, LocalDateTime.class)),
            requireNonNull(rs.getObject(2, LocalDateTime.class)),
            requireNonNull(rs.getObject(1, ZonedDateTime.class)),
            requireNonNull(rs.getObject(2, ZonedDateTime.class)),
            requireNonNull(rs.getObject(1, OffsetDateTime.class)),
            requireNonNull(rs.getObject(2, OffsetDateTime.class))
        )).get(0);
        Consumer<Tuple8<LocalDateTime, LocalDateTime, ZonedDateTime, ZonedDateTime, OffsetDateTime,
            OffsetDateTime, Instant, Instant>> assertion = actual -> {
            assertThat(actual.getT1()).isCloseTo(expectedTuples.getT1(), TINY_WITHIN);
            assertThat(actual.getT2()).isCloseTo(expectedTuples.getT2(), TINY_WITHIN);
            assertThat(actual.getT3().getZone().normalized())
                .isEqualTo(expectedTuples.getT3().getZone().normalized());
            assertThat(actual.getT3()).isCloseTo(expectedTuples.getT3(), TINY_WITHIN);
            assertThat(actual.getT4().getZone().normalized())
                .isEqualTo(expectedTuples.getT4().getZone().normalized());
            assertThat(actual.getT4()).isCloseTo(expectedTuples.getT4(), TINY_WITHIN);
            assertThat(actual.getT5().getOffset()).isEqualTo(expectedTuples.getT5().getOffset());
            assertThat(actual.getT5()).isCloseTo(expectedTuples.getT5(), TINY_WITHIN);
            assertThat(actual.getT6().getOffset()).isEqualTo(expectedTuples.getT6().getOffset());
            assertThat(actual.getT6()).isCloseTo(expectedTuples.getT6(), TINY_WITHIN);
            assertThat(actual.getT7()).isCloseTo(expectedTuples.getT3().toInstant(), TINY_WITHIN);
            assertThat(actual.getT8()).isCloseTo(expectedTuples.getT4().toInstant(), TINY_WITHIN);
            assertThat(actual.getT7()).isCloseTo(expectedTuples.getT5().toInstant(), TINY_WITHIN);
            assertThat(actual.getT8()).isCloseTo(expectedTuples.getT6().toInstant(), TINY_WITHIN);
        };

        MySqlConnectionFactory.from(config).create()
            .flatMapMany(connection -> connection.createStatement(insertQuery)
                .bind(0, now)
                .bind(1, now)
                .execute()
                .flatMap(MySqlResult::getRowsUpdated)
                .thenMany(connection.createStatement(selectQuery).execute())
                .flatMap(result -> result.map(r -> Tuples.of(
                    requireNonNull(r.get(0, LocalDateTime.class)),
                    requireNonNull(r.get(1, LocalDateTime.class)),
                    requireNonNull(r.get(0, ZonedDateTime.class)),
                    requireNonNull(r.get(1, ZonedDateTime.class)),
                    requireNonNull(r.get(0, OffsetDateTime.class)),
                    requireNonNull(r.get(1, OffsetDateTime.class)),
                    requireNonNull(r.get(0, Instant.class)),
                    requireNonNull(r.get(1, Instant.class))
                )))
                .onErrorResume(e -> connection.close().then(Mono.error(e)))
                .concatWith(connection.close().then(Mono.empty())))
            .as(StepVerifier::create)
            .assertNext(assertion)
            .assertNext(assertion)
            .verifyComplete();

        requireNonNull((HikariDataSource) jdbc.getDataSource()).close();
    }

    static Stream<Arguments> alignDateTimeFunction() {
        return Stream.of(
            Arguments.of(false, "LOCAL", false),
            Arguments.of(false, "LOCAL", true),
            Arguments.of(true, "LOCAL", false),
            Arguments.of(true, "LOCAL", true),
            Arguments.of(false, "SERVER", false),
            Arguments.of(false, "SERVER", true),
            Arguments.of(true, "SERVER", false),
            Arguments.of(true, "SERVER", true),
            Arguments.of(false, "GMT+2", false),
            Arguments.of(false, "GMT+3", true),
            Arguments.of(true, "GMT+4", false),
            Arguments.of(true, "GMT+5", true)
        );
    }

    static Stream<Arguments> alignSendAndReceiveTimeZoneOption() {
        ZonedDateTime dateTime = ZonedDateTime.now();

        return Stream.of(dateTime).flatMap(now -> Stream.of(
            now.toLocalDateTime(),
            now,
            now.toOffsetDateTime(),
            now.withZoneSameInstant(ZoneOffset.ofHours(2)).toLocalDateTime(),
            now.withZoneSameInstant(ZoneOffset.ofHours(3)),
            now.withZoneSameInstant(ZoneOffset.ofHours(4)).toOffsetDateTime(),
            now.withZoneSameLocal(ZoneOffset.ofHours(5)).toLocalDateTime(),
            now.withZoneSameLocal(ZoneOffset.ofHours(6)),
            now.withZoneSameLocal(ZoneOffset.ofHours(7)).toOffsetDateTime()
        )).flatMap(temporal -> Stream.of(
            Arguments.of(false, "LOCAL", false, temporal),
            Arguments.of(false, "LOCAL", true, temporal),
            Arguments.of(true, "LOCAL", false, temporal),
            Arguments.of(true, "LOCAL", true, temporal),
            Arguments.of(false, "SERVER", false, temporal),
            Arguments.of(false, "SERVER", true, temporal),
            Arguments.of(true, "SERVER", false, temporal),
            Arguments.of(true, "SERVER", true, temporal),
            Arguments.of(false, "GMT+1", false, temporal),
            Arguments.of(false, "GMT+1", true, temporal),
            Arguments.of(true, "GMT+1", false, temporal),
            Arguments.of(true, "GMT+1", true, temporal)
        ));
    }

    private static MySqlConnectionConfiguration configuration(
        Function<MySqlConnectionConfiguration.Builder, MySqlConnectionConfiguration.Builder> customizer
    ) {

        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder()
            .host(TestServerUtil.getHost())
            .port(TestServerUtil.getPort())
            .user(TestServerUtil.getUsername())
            .password(TestServerUtil.getPassword())
            .database(TestServerUtil.getDatabase());

        return customizer.apply(builder).build();
    }

    private static JdbcTemplate jdbc(MySqlConnectionConfiguration config) {
        HikariDataSource source = new HikariDataSource();

        source.setJdbcUrl(String.format("jdbc:mysql://%s:%d/%s", config.getDomain(),
            config.getPort(), config.getDatabase()));
        source.setUsername(config.getUser());
        source.setPassword(Optional.ofNullable(config.getPassword())
            .map(Object::toString).orElse(null));
        source.setMaximumPoolSize(1);
        source.setConnectionTimeout(Optional.ofNullable(config.getConnectTimeout())
            .map(Duration::toMillis).orElse(0L));

        source.addDataSourceProperty("preserveInstants", config.isPreserveInstants());
        source.addDataSourceProperty("connectionTimeZone", config.getConnectionTimeZone());
        source.addDataSourceProperty("forceConnectionTimeZoneToSession",
            config.isForceConnectionTimeZoneToSession());

        return new JdbcTemplate(source);
    }

    private static String dateTimeSuffix(boolean function) {
        return isMicrosecondSupported() ? "(6)" : function ? "()" : "";
    }

    private static boolean isMicrosecondSupported() {
        final ServerVersion ver = TestServerUtil.getServerVersion();

        return TestServerUtil.isMariaDb() ||
            ver.isGreaterThanOrEqualTo(ServerVersion.create(5, 6, 0));
    }

    private static void assertDateTimeTuples(
        Tuple4<LocalDateTime, ZonedDateTime, OffsetDateTime, Instant> actual,
        Tuple3<LocalDateTime, ZonedDateTime, OffsetDateTime> expected
    ) {
        assertThat(actual.getT1()).isCloseTo(expected.getT1(), TINY_WITHIN);
        assertThat(actual.getT2().getZone().normalized())
            .isEqualTo(expected.getT2().getZone().normalized());
        assertThat(actual.getT2()).isCloseTo(expected.getT2(), TINY_WITHIN);
        assertThat(actual.getT3().getOffset())
            .isEqualTo(expected.getT3().getOffset());
        assertThat(actual.getT3()).isCloseTo(expected.getT3(), TINY_WITHIN);
        assertThat(actual.getT4())
            .isCloseTo(expected.getT2().toInstant(), TINY_WITHIN);
        assertThat(actual.getT4())
            .isCloseTo(expected.getT3().toInstant(), TINY_WITHIN);
    }
}
