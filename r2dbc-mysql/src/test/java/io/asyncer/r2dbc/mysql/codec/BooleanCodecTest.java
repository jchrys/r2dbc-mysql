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

package io.asyncer.r2dbc.mysql.codec;

import io.asyncer.r2dbc.mysql.ConnectionContextTest;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BooleanCodec}.
 */
class BooleanCodecTest implements CodecTestSupport<Boolean> {

    private final Boolean[] booleans = { Boolean.TRUE, Boolean.FALSE };

    @Override
    public BooleanCodec getCodec() {
        return BooleanCodec.INSTANCE;
    }

    @Override
    public Boolean[] originParameters() {
        return booleans;
    }

    @Override
    public Object[] stringifyParameters() {
        return Arrays.stream(booleans).map(it -> it ? "1" : "0").toArray();
    }

    @Override
    public ByteBuf[] binaryParameters(Charset charset) {
        return Arrays.stream(booleans)
            .map(it -> Unpooled.wrappedBuffer(it ? new byte[] { 1 } : new byte[] { 0 }))
            .toArray(ByteBuf[]::new);
    }

    @Override
    public ByteBuf sized(ByteBuf value) {
        return value;
    }

    @Test
    void decodeString() {
        Codec<Boolean> codec = getCodec();
        Charset c = ConnectionContextTest.mock().getClientCollation().getCharset();
        Decoding d1 = new Decoding(Unpooled.copiedBuffer("true", c), "true", MySqlType.VARCHAR);
        Decoding d2 = new Decoding(Unpooled.copiedBuffer("false", c), "false", MySqlType.VARCHAR);
        Decoding d3 = new Decoding(Unpooled.copiedBuffer("1", c), "1", MySqlType.VARCHAR);
        Decoding d4 = new Decoding(Unpooled.copiedBuffer("0", c), "0", MySqlType.VARCHAR);

        assertThat(codec.decode(d1.content(), d1.metadata(), Boolean.class, false, ConnectionContextTest.mock()))
            .as("Decode failed, %s", d1)
            .isEqualTo(true);

        assertThat(codec.decode(d2.content(), d2.metadata(), Boolean.class, false, ConnectionContextTest.mock()))
            .as("Decode failed, %s", d2)
            .isEqualTo(false);

        assertThat(codec.decode(d3.content(), d3.metadata(), Boolean.class, false, ConnectionContextTest.mock()))
            .as("Decode failed, %s", d3)
            .isEqualTo(true);

        assertThat(codec.decode(d4.content(), d4.metadata(), Boolean.class, false, ConnectionContextTest.mock()))
            .as("Decode failed, %s", d4)
            .isEqualTo(false);
    }
}
