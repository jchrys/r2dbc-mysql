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

import java.util.Objects;

/**
 * A value object representing a user with an optional password.
 */
final class Credential {

    private final String user;

    @Nullable
    private final CharSequence password;

    Credential(String user, @Nullable CharSequence password) {
        this.user = user;
        this.password = password;
    }

    String getUser() {
        return user;
    }

    @Nullable
    CharSequence getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Credential)) {
            return false;
        }

        Credential that = (Credential) o;

        return user.equals(that.user) && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return 31 * user.hashCode() + Objects.hashCode(password);
    }

    @Override
    public String toString() {
        return "Credential{user=" + user + ", password=REDACTED}";
    }
}
