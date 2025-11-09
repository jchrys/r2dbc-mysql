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
 * Base class for mysql_native_password authentication tests.
 * <p>
 * Uses MySQL 5.7 with mysql_native_password as the default authentication plugin.
 * All tests extending this class share the same MySQL 5.7 container instance.
 */
public abstract class AbstractMySqlNativePasswordTest extends AbstractAuthenticationTest {

    private static final String MYSQL_VERSION = "5.7.44";
    private static final String AUTH_PLUGIN = "mysql_native_password";

    protected AbstractMySqlNativePasswordTest() {
        super();
    }

    protected AbstractMySqlNativePasswordTest(
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
