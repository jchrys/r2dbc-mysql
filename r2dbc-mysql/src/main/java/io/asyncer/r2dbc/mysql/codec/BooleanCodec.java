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

import java.math.BigInteger;

import io.asyncer.r2dbc.mysql.MySqlParameter;
import io.asyncer.r2dbc.mysql.ParameterWriter;
import io.asyncer.r2dbc.mysql.api.MySqlReadableMetadata;
import io.asyncer.r2dbc.mysql.constant.MySqlType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import reactor.core.publisher.Mono;

/**
 * Codec for BIT, can convert to {@code boolean} if precision is 1.
 */
final class BooleanCodec extends AbstractPrimitiveCodec<Boolean> {

    static final BooleanCodec INSTANCE = new BooleanCodec();

    private BooleanCodec() {
        super(Boolean.TYPE, Boolean.class);
    }

    @Override
    public Boolean decode(ByteBuf value, MySqlReadableMetadata metadata, Class<?> target, boolean binary,
        CodecContext context) {
        MySqlType dataType = metadata.getType();

        if (dataType == MySqlType.VARCHAR) {
            if (!value.isReadable()) {
                return createFromLong(0);
            }

            String s = value.toString(metadata.getCharCollation(context).getCharset());

            if (s.equalsIgnoreCase("Y") || s.equalsIgnoreCase("yes") ||
            s.equalsIgnoreCase("T") || s.equalsIgnoreCase("true")) {
                return createFromLong(1);
            } else if (s.equalsIgnoreCase("N") || s.equalsIgnoreCase("no") ||
            s.equalsIgnoreCase("F") || s.equalsIgnoreCase("false")) {
                return createFromLong(0);
            } else if (s.matches("-?\\d*\\.\\d*") || s.matches("-?\\d*\\.\\d+[eE]-?\\d+")
            || s.matches("-?\\d*[eE]-?\\d+")) {
                return createFromDouble(Double.parseDouble(s));
            } else if (s.matches("-?\\d+")) {
                if (!CodecUtils.isGreaterThanLongMax(s)) {
                    return createFromLong(CodecUtils.parseLong(value));
                }
                return createFromBigInteger(new BigInteger(s));
            }
            throw new R2dbcNonTransientResourceException("The value '" + s + "' of type '" + dataType +
            "' cannot be encoded into a Boolean.", "22018");
        }

        return binary || dataType == MySqlType.BIT ? value.readBoolean() : value.readByte() != '0';
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Boolean;
    }

    @Override
    public MySqlParameter encode(Object value, CodecContext context) {
        return (Boolean) value ? BooleanMySqlParameter.TRUE : BooleanMySqlParameter.FALSE;
    }

    @Override
    public boolean doCanDecode(MySqlReadableMetadata metadata) {
        MySqlType type = metadata.getType();
        return ((type == MySqlType.BIT || type == MySqlType.TINYINT) &&
        Integer.valueOf(1).equals(metadata.getPrecision())) || type == MySqlType.VARCHAR;
    }

    public Boolean createFromLong(long l) {
        return (l == -1 || l > 0);
    }

    public Boolean createFromDouble(double d) {
        return (d == -1.0d || d > 0);
    }

    public Boolean createFromBigInteger(BigInteger b) {
        return b.compareTo(BigInteger.valueOf(0)) > 0 || b.compareTo(BigInteger.valueOf(-1)) == 0;
    }

    private static final class BooleanMySqlParameter extends AbstractMySqlParameter {

        private static final BooleanMySqlParameter TRUE = new BooleanMySqlParameter(true);

        private static final BooleanMySqlParameter FALSE = new BooleanMySqlParameter(false);

        private final boolean value;

        private BooleanMySqlParameter(boolean value) {
            this.value = value;
        }

        @Override
        public Mono<ByteBuf> publishBinary(final ByteBufAllocator allocator) {
            return Mono.fromSupplier(() -> allocator.buffer(Byte.BYTES).writeByte(value ? 1 : 0));
        }

        @Override
        public Mono<Void> publishText(ParameterWriter writer) {
            return Mono.fromRunnable(() -> writer.writeInt(value ? 1 : 0));
        }

        @Override
        public MySqlType getType() {
            // Note: BIT will least 2-bytes in binary parameter (var integer size and content),
            // so use TINYINT will encode to buffer faster and shorter.
            return MySqlType.TINYINT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BooleanMySqlParameter)) {
                return false;
            }

            BooleanMySqlParameter that = (BooleanMySqlParameter) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (value ? 1 : 0);
        }

        @Override
        public String toString() {
            return Boolean.toString(value);
        }
    }
}
