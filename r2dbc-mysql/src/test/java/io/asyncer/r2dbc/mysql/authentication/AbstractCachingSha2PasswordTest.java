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

package io.asyncer.r2dbc.mysql.authentication;

import io.asyncer.r2dbc.mysql.MySqlConnectionConfiguration;

import java.util.function.Function;

/**
 * Base class for caching_sha2_password authentication tests.
 * <p>
 * Uses MySQL 8.0+ with caching_sha2_password as the default authentication plugin (MySQL 8.0 default).
 * All tests extending this class share the same MySQL 8.0 container instance.
 * <p>
 * This authentication method supports RSA public key encryption.
 */
public abstract class AbstractCachingSha2PasswordTest extends AbstractAuthenticationTest {

    private static final String MYSQL_VERSION = "8.0.35";
    private static final String AUTH_PLUGIN = "caching_sha2_password";

    protected AbstractCachingSha2PasswordTest() {
        super();
    }

    protected AbstractCachingSha2PasswordTest(
            Function<MySqlConnectionConfiguration.Builder,
                     MySqlConnectionConfiguration.Builder> customizer) {
        super(customizer);
    }

    @Override
    protected String getMySqlVersion() {
        return MYSQL_VERSION;
    }

    @Override
    protected String getAuthenticationPlugin() {
        return AUTH_PLUGIN;
    }
}
