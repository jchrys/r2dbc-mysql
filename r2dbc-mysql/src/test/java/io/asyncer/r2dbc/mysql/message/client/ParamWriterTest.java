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

package io.asyncer.r2dbc.mysql.message.client;

import io.asyncer.r2dbc.mysql.Query;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 * Unit tests for {@link ParamWriter}.
 */
class ParamWriterTest {

    private static final int SIZE = 10;

    @Test
    void badFollowString() {
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeNull());
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeBinary(true));
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeBinary(false));
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeHex(new byte[0]));
        assertThatIllegalStateException().isThrownBy(() -> stringWriter().writeHex(ByteBuffer.allocate(0)));
    }

    @Test
    void stringFollowString() {
        ParamWriter writer = stringWriter();
        writer.write("abc");
        writer.write('1');
        writer.write("define", 2, 3);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'0abc1fin'");
    }

    @Test
    void numericFollowString() {
        ParamWriter writer = stringWriter();
        writer.writeInt(1);
        writer.writeLong(2);
        writer.writeFloat(3.4f);
        writer.writeDouble(5.6);
        writer.writeBigInteger(BigInteger.valueOf(7));
        writer.writeBigDecimal(BigDecimal.valueOf(8.9));
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'0123.45.678.9'");
    }

    @Test
    void badFollowNull() {
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeNull());
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeBinary(true));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeBinary(false));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeHex(new byte[0]));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeHex(ByteBuffer.allocate(0)));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().append(' '));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().append("123"));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().append("123", 1, 2));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().write(' '));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().write("123"));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().write("123", 1, 2));
        assertThatIllegalStateException().isThrownBy(() -> nullWriter().writeDouble(0.1));
    }

    @Test
    void appendPart() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.append("define", 2, 5);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'fin'");
    }

    @Test
    void writePart() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.write("define", 2, 3);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'fin'");
    }

    @Test
    void appendNull() {
        assertThat(ParameterWriterHelper.toSql(ParameterWriterHelper.get(false, parameterOnly(1)).append(null)))
            .isEqualTo("'null'");
        assertThat(ParameterWriterHelper.toSql(ParameterWriterHelper.get(false, parameterOnly(1))
            .append(null, 1, 3)))
            .isEqualTo("'ul'");
    }

    @Test
    void writeNull() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.write((String) null);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'null'");

        writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.write((String) null, 1, 2);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'ul'");

        writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.write((char[]) null);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'null'");

        writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.write((char[]) null, 1, 2);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'ul'");
    }

    @Test
    void publishSuccess() {
        MockMySqlParameter[] values = new MockMySqlParameter[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockMySqlParameter(true);
        }

        Flux.from(ParamWriter.publish(false, parameterOnly(SIZE), Flux.fromArray(values)))
            .as(StepVerifier::create)
            .expectNext(new String(new char[SIZE]).replace("\0", "''"))
            .verifyComplete();

        assertThat(values).extracting(MockMySqlParameter::refCnt).containsOnly(0);
    }

    @Test
    void publishPartially() {
        MockMySqlParameter[] values = new MockMySqlParameter[SIZE];

        int i = 0;

        for (; i < SIZE >>> 1; ++i) {
            values[i] = new MockMySqlParameter(true);
        }

        for (; i < SIZE; ++i) {
            values[i] = new MockMySqlParameter(false);
        }

        Flux.from(ParamWriter.publish(false, parameterOnly(SIZE), Flux.fromArray(values)))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertThat(values).extracting(MockMySqlParameter::refCnt).containsOnly(0);
    }

    @Test
    void publishNothing() {
        MockMySqlParameter[] values = new MockMySqlParameter[SIZE];

        for (int i = 0; i < SIZE; ++i) {
            values[i] = new MockMySqlParameter(false);
        }

        Flux.from(ParamWriter.publish(false, parameterOnly(SIZE), Flux.fromArray(values)))
            .as(StepVerifier::create)
            .verifyError(MockException.class);

        assertThat(values).extracting(MockMySqlParameter::refCnt).containsOnly(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "abc",
        "a'b'c",
        "a\nb\rc",
        "a\"b\"c",
        "a\\b\\c",
        "a\0b\0c",
        "a\u00a5b\u20a9c",
        "a\032b\032c",
    })
    void noBackslashEscapes(String value) {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(true, parameterOnly(1));
        writer.write(value);
        assertThat(ParameterWriterHelper.toSql(writer)).isEqualTo("'" + value.replaceAll("'", "''") + "'");
    }

    private static Query parameterOnly(int parameters) {
        char[] chars = new char[parameters];
        Arrays.fill(chars, '?');

        return Query.parse(new String(chars));
    }

    private static ParamWriter stringWriter() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.write('0');
        return writer;
    }

    private static ParamWriter nullWriter() {
        ParamWriter writer = (ParamWriter) ParameterWriterHelper.get(false, parameterOnly(1));
        writer.writeNull();
        return writer;
    }
}
