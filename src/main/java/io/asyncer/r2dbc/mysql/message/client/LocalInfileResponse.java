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

package io.asyncer.r2dbc.mysql.message.client;

import io.asyncer.r2dbc.mysql.ConnectionContext;
import io.asyncer.r2dbc.mysql.internal.util.NettyBufferUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.requireNonNull;

/**
 * A message considers as a chunk of a local in-file data.
 */
public final class LocalInfileResponse implements SubsequenceClientMessage {

    private final int envelopeId;

    private final String path;

    private final SynchronousSink<?> errorSink;

    public LocalInfileResponse(int envelopeId, String path, SynchronousSink<?> errorSink) {
        requireNonNull(path, "path must not be null");

        this.envelopeId = envelopeId;
        this.path = path;
        this.errorSink = errorSink;
    }

    @Override
    public Flux<ByteBuf> encode(ByteBufAllocator allocator, ConnectionContext context) {
        return Flux.defer(() -> {
            AtomicReference<Throwable> error = new AtomicReference<>();
            Path path = Paths.get(this.path);

            return NettyBufferUtils.readFile(path, allocator, context.getLocalInfileBufferSize())
                .onErrorComplete(e -> {
                    error.set(e);
                    return true;
                })
                .concatWith(Flux.just(allocator.buffer(0, 0)))
                .doAfterTerminate(() -> {
                    Throwable e = error.getAndSet(null);

                    if (e != null) {
                        errorSink.error(e);
                    }
                });
        });
    }

    @Override
    public int getEnvelopeId() {
        return envelopeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LocalInfileResponse)) {
            return false;
        }

        LocalInfileResponse that = (LocalInfileResponse) o;

        return envelopeId == that.envelopeId && path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return 31 * envelopeId + path.hashCode();
    }

    @Override
    public String toString() {
        return "LocalInfileResponse{envelopeId=" + envelopeId +
            ", path='" + path + "'}";
    }
}
