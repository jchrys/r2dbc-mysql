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

import io.asyncer.r2dbc.mysql.constant.SslMode;
import io.asyncer.r2dbc.mysql.internal.util.InternalArrays;
import io.netty.handler.ssl.SslContextBuilder;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.HostnameVerifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static io.asyncer.r2dbc.mysql.internal.util.AssertUtils.require;
import static io.asyncer.r2dbc.mysql.internal.util.InternalArrays.EMPTY_STRINGS;

/**
 * A configuration of MySQL SSL connection.
 */
public final class MySqlSslConfiguration {

    private static final MySqlSslConfiguration DISABLED = new MySqlSslConfiguration(SslMode.DISABLED,
        EMPTY_STRINGS, null, null, null, null, null, null);

    private final SslMode sslMode;

    private final String[] tlsVersion;

    @Nullable
    private final HostnameVerifier sslHostnameVerifier;

    @Nullable
    private final String sslCa;

    @Nullable
    private final String sslKey;

    @Nullable
    private final CharSequence sslKeyPassword;

    @Nullable
    private final String sslCert;

    @Nullable
    private final Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

    private MySqlSslConfiguration(SslMode sslMode, String[] tlsVersion,
        @Nullable HostnameVerifier sslHostnameVerifier, @Nullable String sslCa, @Nullable String sslKey,
        @Nullable CharSequence sslKeyPassword, @Nullable String sslCert,
        @Nullable Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer) {
        this.sslMode = sslMode;
        this.tlsVersion = tlsVersion;
        this.sslHostnameVerifier = sslHostnameVerifier;
        this.sslCa = sslCa;
        this.sslKey = sslKey;
        this.sslKeyPassword = sslKeyPassword;
        this.sslCert = sslCert;
        this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
    }

    public SslMode getSslMode() {
        return sslMode;
    }

    public String[] getTlsVersion() {
        return tlsVersion;
    }

    @Nullable
    public HostnameVerifier getSslHostnameVerifier() {
        return sslHostnameVerifier;
    }

    @Nullable
    public String getSslCa() {
        return sslCa;
    }

    @Nullable
    public String getSslKey() {
        return sslKey;
    }

    @Nullable
    public CharSequence getSslKeyPassword() {
        return sslKeyPassword;
    }

    @Nullable
    public String getSslCert() {
        return sslCert;
    }

    /**
     * Customizes a {@link SslContextBuilder} that customizer was specified by configuration, or do nothing if the
     * customizer was not set.
     *
     * @param builder the {@link SslContextBuilder}.
     * @return the {@code builder}.
     */
    public SslContextBuilder customizeSslContext(SslContextBuilder builder) {
        if (sslContextBuilderCustomizer == null) {
            return builder;
        }

        return sslContextBuilderCustomizer.apply(builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlSslConfiguration)) {
            return false;
        }
        MySqlSslConfiguration that = (MySqlSslConfiguration) o;
        return sslMode == that.sslMode &&
            Arrays.equals(tlsVersion, that.tlsVersion) &&
            Objects.equals(sslHostnameVerifier, that.sslHostnameVerifier) &&
            Objects.equals(sslCa, that.sslCa) &&
            Objects.equals(sslKey, that.sslKey) &&
            Objects.equals(sslKeyPassword, that.sslKeyPassword) &&
            Objects.equals(sslCert, that.sslCert) &&
            Objects.equals(sslContextBuilderCustomizer, that.sslContextBuilderCustomizer);
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(sslMode, sslHostnameVerifier, sslCa, sslKey, sslKeyPassword, sslCert,
            sslContextBuilderCustomizer);
        return 31 * hash + Arrays.hashCode(tlsVersion);
    }

    @Override
    public String toString() {
        if (sslMode == SslMode.DISABLED) {
            return "MySqlSslConfiguration{sslMode=DISABLED}";
        }

        return "MySqlSslConfiguration{sslMode=" + sslMode + ", tlsVersion=" + Arrays.toString(tlsVersion) +
            ", sslHostnameVerifier=" + sslHostnameVerifier + ", sslCa='" + sslCa + "', sslKey='" + sslKey +
            "', sslKeyPassword=REDACTED, sslCert='" + sslCert + "', sslContextBuilderCustomizer=" +
            sslContextBuilderCustomizer + '}';
    }

    static MySqlSslConfiguration disabled() {
        return DISABLED;
    }

    static final class Builder {

        @Nullable
        private SslMode sslMode;

        private String[] tlsVersions = InternalArrays.EMPTY_STRINGS;

        @Nullable
        private HostnameVerifier sslHostnameVerifier;

        @Nullable
        private String sslCa;

        @Nullable
        private String sslKey;

        @Nullable
        private CharSequence sslKeyPassword;

        @Nullable
        private String sslCert;

        @Nullable
        private Function<SslContextBuilder, SslContextBuilder> sslContextBuilderCustomizer;

        void sslMode(SslMode sslMode) {
            this.sslMode = sslMode;
        }

        void tlsVersions(String[] tlsVersions) {
            int size = tlsVersions.length;

            if (size > 0) {
                String[] versions = new String[size];
                System.arraycopy(tlsVersions, 0, versions, 0, size);
                this.tlsVersions = versions;
            } else {
                this.tlsVersions = EMPTY_STRINGS;
            }
        }

        void sslHostnameVerifier(HostnameVerifier sslHostnameVerifier) {
            this.sslHostnameVerifier = sslHostnameVerifier;
        }

        void sslCa(@Nullable String sslCa) {
            this.sslCa = sslCa;
        }

        void sslCert(@Nullable String sslCert) {
            this.sslCert = sslCert;
        }

        void sslKey(@Nullable String sslKey) {
            this.sslKey = sslKey;
        }

        void sslKeyPassword(@Nullable CharSequence sslKeyPassword) {
            this.sslKeyPassword = sslKeyPassword;
        }

        void sslContextBuilderCustomizer(Function<SslContextBuilder, SslContextBuilder> customizer) {
            this.sslContextBuilderCustomizer = customizer;
        }

        MySqlSslConfiguration build(boolean preferred) {
            SslMode sslMode = this.sslMode;

            if (sslMode == null) {
                sslMode = preferred ? SslMode.PREFERRED : SslMode.DISABLED;
            }

            if (sslMode == SslMode.DISABLED) {
                return DISABLED;
            }

            require((sslCert == null && sslKey == null) || (sslCert != null && sslKey != null),
                "sslCert and sslKey must be both null or both non-null");

            return new MySqlSslConfiguration(sslMode, tlsVersions, sslHostnameVerifier, sslCa, sslKey,
                sslKeyPassword, sslCert, sslContextBuilderCustomizer);
        }
    }
}
