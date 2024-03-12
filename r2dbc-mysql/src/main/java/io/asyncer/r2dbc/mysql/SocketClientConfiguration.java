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

package io.asyncer.r2dbc.mysql;

import org.jetbrains.annotations.Nullable;
import reactor.netty.resources.LoopResources;

import java.time.Duration;
import java.util.Objects;

/**
 * A general-purpose configuration for a socket client. The client can be a TCP client or a Unix Domain Socket client.
 */
final class SocketClientConfiguration {

    @Nullable
    private final Duration connectTimeout;

    @Nullable
    private final LoopResources loopResources;

    SocketClientConfiguration(@Nullable Duration connectTimeout, @Nullable LoopResources loopResources) {
        this.connectTimeout = connectTimeout;
        this.loopResources = loopResources;
    }

    @Nullable
    Duration getConnectTimeout() {
        return connectTimeout;
    }

    @Nullable
    LoopResources getLoopResources() {
        return loopResources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SocketClientConfiguration)) {
            return false;
        }

        SocketClientConfiguration that = (SocketClientConfiguration) o;

        return Objects.equals(connectTimeout, that.connectTimeout) && Objects.equals(loopResources, that.loopResources);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(connectTimeout) + Objects.hashCode(loopResources);
    }

    @Override
    public String toString() {
        return "Client{connectTimeout=" + connectTimeout + ", loopResources=" + loopResources + '}';
    }

    static final class Builder {

        @Nullable
        private Duration connectTimeout;

        @Nullable
        private LoopResources loopResources;

        void connectTimeout(@Nullable Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        void loopResources(@Nullable LoopResources loopResources) {
            this.loopResources = loopResources;
        }

        SocketClientConfiguration build() {
            return new SocketClientConfiguration(connectTimeout, loopResources);
        }
    }
}
