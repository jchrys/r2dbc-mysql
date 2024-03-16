package io.asyncer.r2dbc.mysql.internal.util;

import io.asyncer.r2dbc.mysql.ServerVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestContainerExtension.class)
class TestServerUtilTest {

    @Test
    void serverVersionTest() {
        final boolean useTestContainer = TestContainerExtension.useTestContainer;
        final String versionString = TestContainerExtension.dbVersion;
        Assumptions.assumeTrue(useTestContainer && containsPatchVersion(versionString));
        Assertions.assertEquals(ServerVersion.parse(versionString), TestServerUtil.getServerVersion());
    }

    @Test
    void serverTypeTest() {
        final boolean useTestContainer = TestContainerExtension.useTestContainer;
        final String dbType = TestContainerExtension.dbType;
        Assumptions.assumeTrue(useTestContainer);
        Assertions.assertEquals("mariadb".equalsIgnoreCase(dbType), TestServerUtil.isMariaDb());
    }

    private static boolean containsPatchVersion(final String versionString) {
        return versionString.indexOf('.') != versionString.lastIndexOf('.');
    }

}
