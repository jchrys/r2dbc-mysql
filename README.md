# Reactive Relational Database Connectivity MySQL Implementation

![Maven Central](https://img.shields.io/maven-central/v/io.asyncer/r2dbc-mysql?color=blue)
![LICENSE](https://img.shields.io/github/license/asyncer-io/r2dbc-mysql)

This project contains the [MySQL][m] implementation of the [R2DBC SPI](https://github.com/r2dbc/r2dbc-spi).
This implementation is not intended to be used directly, but rather to be
used as the backing implementation for a humane client library to
delegate to. See [R2DBC Homepage](https://r2dbc.io).

See [R2DBC MySQL wiki](https://github.com/asyncer-io/r2dbc-mysql/wiki) for more information.

## Spring-framework and R2DBC-SPI Compatibility

Refer to the table below to determine the appropriate version of r2dbc-mysql for your project.

| spring-boot-starter-data-r2dbc | spring-data-r2dbc | r2dbc-spi     | r2dbc-mysql(recommended)     |
|--------------------------------|-------------------|---------------|------------------------------|
| 3.0.* and above                | 3.0.* and above   | 1.0.0.RELEASE | io.asyncer:r2dbc-mysql:1.3.1 |
| 2.7.*                          | 1.5.*             | 0.9.1.RELEASE | io.asyncer:r2dbc-mysql:0.9.7 |
| 2.6.* and below                | 1.4.* and below   | 0.8.6.RELEASE | dev.miku:r2dbc-mysql:0.8.2   |

## Supported Features

This driver provides the following features:

- [x] Unix domain socket.
- [x] Compression protocols, including zstd and zlib.
- [x] Execution of simple or batch statements without bindings.
- [x] Execution of prepared statements with bindings.
- [x] Reactive LOB types (e.g. BLOB, CLOB)
- [x] All charsets from MySQL, like `utf8mb4_0900_ai_ci`, `latin1_general_ci`, `utf32_unicode_520_ci`, etc.
- [x] All authentication types for MySQL, like `caching_sha2_password`, `mysql_native_password`, etc.
- [x] General exceptions with error code and standard SQL state mappings.
- [x] Secure connection with verification (SSL/TLS), auto-select TLS version for community and enterprise editions.
- [x] SSL tunnel for proxy protocol of MySQL.
- [x] Transactions with savepoint.
- [x] Native ping can be sent via `Connection.validate(ValidationDepth.REMOTE)` and the lightweight ping syntax `/* ping */ ...`.
- [x] Extensible, e.g. extend built-in `Codec`(s).
- [x] MariaDB `RETURNING` clause.

## Version compatibility / Integration tests states

![MySQL 5.5 status](https://img.shields.io/badge/MySQL%205.5-pass-blue)
![MySQL 5.6 status](https://img.shields.io/badge/MySQL%205.6-pass-blue)
![MySQL 5.7 status](https://img.shields.io/badge/MySQL%205.7-pass-blue)
![MySQL 8.0 status](https://img.shields.io/badge/MySQL%208.0-pass-blue)
![MySQL 8.1 status](https://img.shields.io/badge/MySQL%208.1-pass-blue)
![MySQL 8.2 status](https://img.shields.io/badge/MySQL%208.2-pass-blue)
![MySQL 8.3 status](https://img.shields.io/badge/MySQL%208.3-pass-blue)
![MySQL 8.4 status](https://img.shields.io/badge/MySQL%208.4-pass-blue)
![MySQL 9.0 status](https://img.shields.io/badge/MySQL%209.0-pass-blue)
![MariaDB 10.6 status](https://img.shields.io/badge/MariaDB%2010.6-pass-blue)
![MariaDB 10.11 status](https://img.shields.io/badge/MariaDB%2010.11-pass-blue)

In fact, it supports lower versions, in the theory, such as 4.1, 4.0, etc.

However, Docker-certified images do not have these versions lower than 5.5.0, so tests are not integrated on these versions.

## Maven

```xml

<dependency>
  <groupId>io.asyncer</groupId>
  <artifactId>r2dbc-mysql</artifactId>
  <version>1.3.1</version>
</dependency>
```

## Gradle

### Groovy DSL

```groovy
dependencies {
    implementation 'io.asyncer:r2dbc-mysql:1.3.1'
}
```

### Kotlin DSL

```kotlin
dependencies {
    // Maybe should to use `compile` instead of `implementation` on the lower version of Gradle.
    implementation("io.asyncer:r2dbc-mysql:1.3.1")
}
```

## Getting Started

Here is a quick teaser of how to use R2DBC MySQL in Java:

```java
// Notice: the query string must be URL encoded
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbcs:mysql://root:database-password-in-here@127.0.0.1:3306/r2dbc");

// Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

See [Getting Started](https://github.com/asyncer-io/r2dbc-mysql/wiki/getting-started) and [Configuration Options](https://github.com/asyncer-io/r2dbc-mysql/wiki/Configuration-Options) wiki for more information.

### Pooling

See [r2dbc-pool](https://github.com/r2dbc/r2dbc-pool).

### Usage

```java
connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .execute(); // return a Publisher include one Result
```

See [Usage](https://github.com/asyncer-io/r2dbc-mysql/wiki/usage) wiki for more information.

## Reporting Issues

The R2DBC MySQL Implementation uses GitHub as issue tracking system to record bugs and feature requests.
If you want to raise an issue, please follow the recommendations below:

- Before log a bug, please search the [issue tracker](https://github.com/asyncer-io/r2dbc-mysql/issues) to see if someone has already reported the problem.
- If the issue doesn't already exist, [create a new issue](https://github.com/asyncer-io/r2dbc-mysql/issues/new).
- Please provide as much information as possible with the issue report, we like to know the version of R2DBC MySQL that you are using and JVM version.
- If you need to paste code, or include a stack trace use Markdown **&#96;&#96;&#96;** escapes before and after your text.
- If possible try to create a test-case or project that replicates the issue. Attach a link to your code or a compressed file containing your code.

## Before use

- The MySQL data fields encoded by index-based natively, get fields by an index will have **better** performance than get by column name.
- Each `Result` should be used (call `getRowsUpdated` or `map`/`flatMap`, even table definition), can **NOT** just ignore any `Result`, otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)
- The MySQL server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this driver does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Should not turn-on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.
- If `Statement` bound `returnGeneratedValues`, the `Result` of the `Statement` can be called both of `getRowsUpdated` and `map`/`flatMap`.
  - If server is MariaDB 10.5.1 and above: the statement will attempt to use `RETURNING` clause, zero arguments will make the statement like `... RETURNING *`.
  - Otherwise: `returnGeneratedValues` can only be called with one or zero arguments, and `map`/`flatMap` will emit the last inserted id.
- The MySQL may be not support well for searching rows by a binary field, like `BIT` and `JSON`
  - `BIT`: cannot select 'BIT(64)' with value greater than 'Long.MAX_VALUE' (or equivalent in binary)
  - `JSON`: different MySQL may have different serialization formats, e.g. MariaDB and MySQL
- MySQL 8.0+ disables `@@global.local_infile` by default, make sure `@@local_infile` is `ON` before enable `allowLoadLocalInfileInPath` of the driver. e.g. run `SET GLOBAL local_infile=ON`, or set it in `mysql.cnf`.

## License

This project is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgements

## Contributors

<a href="https://github.com/asyncer-io/r2dbc-mysql/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=asyncer-io/r2dbc-mysql" />
</a>

Thanks a lot for your support!

## Supports

- [R2DBC Team](https://r2dbc.io) - Thanks for their support by sharing all relevant resources around R2DBC
  projects.

[m]: https://www.mysql.com
