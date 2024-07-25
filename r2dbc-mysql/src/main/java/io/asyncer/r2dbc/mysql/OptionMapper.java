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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

/**
 * An utility data parser for {@link Option}.
 *
 * @see MySqlConnectionFactoryProvider using this utility.
 * @since 0.8.2
 */
final class OptionMapper {

    private final ConnectionFactoryOptions options;

    OptionMapper(ConnectionFactoryOptions options) {
        this.options = options;
    }

    Source<Object> requires(Option<?> option) {
        return Source.of(options.getRequiredValue(option));
    }

    Source<Object> optional(Option<?> option) {
        return Source.of(options.getValue(option));
    }
}

final class Source<T> {

    private static final Source<Object> NIL = new Source<>(null);

    @Nullable
    private final T value;

    private Source(@Nullable T value) {
        this.value = value;
    }

    boolean to(Consumer<? super T> consumer) {
        if (value == null) {
            return false;
        }

        consumer.accept(value);
        return true;
    }

    <R> Source<R> as(Class<R> type) {
        if (value == null) {
            return nilSource();
        }

        if (type.isInstance(value)) {
            return new Source<>(type.cast(value));
        } else if (value instanceof String) {
            try {
                Class<?> impl = Class.forName((String) value);

                if (type.isAssignableFrom(impl)) {
                    return new Source<>(type.cast(impl.getDeclaredConstructor().newInstance()));
                }
                // Otherwise, not an implementation, convert failed.
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
            }
        }

        throw new IllegalArgumentException(toMessage(value, type.getName()));
    }

    <R> Source<R> as(Class<R> type, Function<String, R> mapping) {
        if (value == null) {
            return nilSource();
        }

        if (type.isInstance(value)) {
            return new Source<>(type.cast(value));
        } else if (value instanceof String) {
            // Type cast for check mapping result.
            return new Source<>(type.cast(mapping.apply((String) value)));
        }

        throw new IllegalArgumentException(toMessage(value, type.getTypeName()));
    }

    <R> Source<R[]> asArray(Class<R[]> arrayType, Function<String, R> mapper,
        Function<String, String[]> splitter, IntFunction<R[]> generator) {
        if (value == null) {
            return nilSource();
        }

        if (arrayType.isInstance(value)) {
            return new Source<>(arrayType.cast(value));
        } else if (value instanceof String[]) {
            return new Source<>(mapArray((String[]) value, mapper, generator));
        } else if (value instanceof String) {
            String[] strings = splitter.apply((String) value);

            if (arrayType.isInstance(strings)) {
                return new Source<>(arrayType.cast(strings));
            }

            return new Source<>(mapArray(strings, mapper, generator));
        } else if (value instanceof Collection<?>) {
            @SuppressWarnings("unchecked")
            Class<R> type = (Class<R>) arrayType.getComponentType();
            R[] array = ((Collection<?>) value).stream().map(e -> {
                if (type.isInstance(e)) {
                    return type.cast(e);
                } else {
                    return mapper.apply(e.toString());
                }
            }).toArray(generator);

            return new Source<>(array);
        }

        throw new IllegalArgumentException(toMessage(value, arrayType.getTypeName()));
    }

    Source<Boolean> asBoolean() {
        if (value == null) {
            return nilSource();
        }

        if (value instanceof Boolean) {
            return new Source<>((Boolean) value);
        } else if (value instanceof String) {
            return new Source<>(Boolean.parseBoolean((String) value));
        }

        throw new IllegalArgumentException(toMessage(value, "Boolean"));
    }

    Source<Integer> asInt() {
        if (value == null) {
            return nilSource();
        }

        if (value instanceof Integer) {
            // Reduce the cost of re-boxed.
            return new Source<>((Integer) value);
        } else if (value instanceof Number) {
            return new Source<>(((Number) value).intValue());
        } else if (value instanceof String) {
            return new Source<>(Integer.parseInt((String) value));
        }

        throw new IllegalArgumentException(toMessage(value, "Integer"));
    }

    Source<CharSequence> asPassword() {
        if (value == null) {
            return nilSource();
        }

        if (value instanceof CharSequence) {
            return new Source<>((CharSequence) value);
        }

        throw new IllegalArgumentException(toMessage("REDACTED", "CharSequence"));
    }

    Source<String> asString() {
        if (value == null) {
            return nilSource();
        }

        if (value instanceof String) {
            return new Source<>((String) value);
        }

        throw new IllegalArgumentException(toMessage(value, "String"));
    }

    @SuppressWarnings("unchecked")
    void prepare(Runnable client, Runnable server, Consumer<Predicate<String>> preferred) {
        if (value == null) {
            return;
        }

        if (value instanceof Boolean) {
            if ((Boolean) value) {
                server.run();
            } else {
                client.run();
            }
            return;
        } else if (value instanceof Predicate<?>) {
            preferred.accept((Predicate<String>) value);
            return;
        } else if (value instanceof String) {
            String stringify = (String) value;

            if ("true".equalsIgnoreCase(stringify)) {
                server.run();
                return;
            } else if ("false".equalsIgnoreCase(stringify)) {
                client.run();
                return;
            }

            try {
                Class<?> impl = Class.forName(stringify);

                if (Predicate.class.isAssignableFrom(impl)) {
                    preferred.accept((Predicate<String>) impl.getDeclaredConstructor().newInstance());
                    return;
                }
                // Otherwise, not an implementation, convert failed.
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot instantiate '" + value + "'", e);
            }
        }

        throw new IllegalArgumentException(toMessage(value, "Boolean or Predicate<String>"));
    }

    static Source<Object> of(@Nullable Object value) {
        if (value == null) {
            return NIL;
        }

        return new Source<>(value);
    }

    @SuppressWarnings("unchecked")
    private static <T> Source<T> nilSource() {
        return (Source<T>) NIL;
    }

    private static String toMessage(Object value, String type) {
        return "Cannot convert value " + value + " to " + type;
    }

    private static <O> O[] mapArray(String[] input, Function<String, O> mapper, IntFunction<O[]> generator) {
        O[] output = generator.apply(input.length);

        for (int i = 0; i < input.length; i++) {
            output[i] = mapper.apply(input[i]);
        }

        return output;
    }
}
