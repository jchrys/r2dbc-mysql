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

/**
 * A configuration for a Unix Domain Socket.
 */
final class UnixDomainSocketConfiguration implements SocketConfiguration {

    private final String path;

    UnixDomainSocketConfiguration(String path) {
        this.path = path;
    }

    String getPath() {
        return this.path;
    }

    @Override
    public ConnectionStrategy strategy(MySqlConnectionConfiguration configuration) {
        return new UnixDomainSocketConnectionStrategy(this, configuration);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UnixDomainSocketConfiguration)) {
            return false;
        }

        UnixDomainSocketConfiguration that = (UnixDomainSocketConfiguration) o;

        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return "UnixDomainSocket{path='" + path + "'}";
    }

    static final class Builder {

        private String path;

        void path(String path) {
            this.path = path;
        }

        UnixDomainSocketConfiguration build() {
            return new UnixDomainSocketConfiguration(path);
        }
    }
}
