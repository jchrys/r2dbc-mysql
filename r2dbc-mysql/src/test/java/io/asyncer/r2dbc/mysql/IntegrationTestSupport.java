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

import java.util.function.Function;

/**
 * Base class for integration tests that provides connection factory and test utilities.
 * <p>
 * This class extends {@link AbstractMySqlContainerHolder} which manages the TestContainer lifecycle
 * and provides both R2DBC and JDBC connection access.
 */
abstract class IntegrationTestSupport extends AbstractMySqlContainerHolder {

    /**
     * Constructor that accepts a custom configuration.
     *
     * @param configuration the R2DBC connection configuration
     */
    IntegrationTestSupport(MySqlConnectionConfiguration configuration) {
        super(builder ->
            // Override all builder properties with the provided configuration
            builder.host(configuration.getDomain())
                .port(configuration.getPort())
                .user(configuration.getUser())
                .password(configuration.getPassword() != null ?
                    new String(configuration.getPassword()) : null)
                .database(configuration.getDatabase())
                .connectTimeout(configuration.getConnectTimeout())
        );
    }

    /**
     * Default constructor using base configuration.
     */
    IntegrationTestSupport() {
        super();
    }

    /**
     * Constructor with configuration customizer.
     *
     * @param customizer function to customize the connection configuration builder
     */
    IntegrationTestSupport(
        Function<MySqlConnectionConfiguration.Builder, MySqlConnectionConfiguration.Builder> customizer
    ) {
        super(customizer);
    }
}
