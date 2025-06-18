/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */

package org.apache.cassandra.tools.nodetool;

import java.net.InetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.audit.AuditUsersCacheService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.tools.ToolRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@code nodetool setauditlogrolefilter}, {@code nodetool disableauditlogrolefilter},
 * and {@code nodetool getauditlogrolefilter}.
 *
 * The style, helper methods, and assertions intentionally mirror {@link GetAuditLogTest}
 * so the suite blends naturally with the existing nodetool integration tests.
 */
public class GetAuditRoleFilterTest extends CQLTester
{

    private static EmbeddedCassandraService embedded;

    public GetAuditRoleFilterTest(){}

    private static final String ROLE = "app_user";
    private static final Double FILTER_PERCENTAGE = 75.5;
    private static final String ACCOUNT_TYPE = "SERVICE";


    @BeforeClass
    public static void setup() throws Exception
    {
        prepareServer();
        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        embedded = new EmbeddedCassandraService();
        embedded.start();
        Thread.sleep(5000);

        AuditUsersCacheService.instance.setup();
        AuditUsersCacheService.instance.initialize();

        try (Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                                      .withoutJMXReporting()
                                      .withPort(DatabaseDescriptor.getNativeTransportPort()).build())
        {
            try (Session session = cluster.connect())
            {
                session.execute("TRUNCATE "+  SchemaConstants.DISTRIBUTED_KEYSPACE_NAME+ "."+ SystemDistributedKeyspace.AUDIT_USER);
            }
        }

        startJMXServer();
    }

    @Before
    public void beforeTest()
    {
        disableAuditRoleFilter(true, ROLE);
        String output = getAuditRoleFilter();
        assertThat(output)
            .contains("No audit log filters found.");
    }

    @After
    public void afterTest()
    {
        disableAuditRoleFilter(true, ROLE);
    }

    @Test
    public void addFilterAndGetItBackTest()
    {
        addAuditRoleFilter(ROLE, "SERVICE", 75.5);

        String output = getAuditRoleFilter();

        assertThat(output)
            .contains(ROLE)
            .contains(ACCOUNT_TYPE)
            .contains(String.valueOf(FILTER_PERCENTAGE))
            .doesNotContain("No audit log filters found.");
    }

    @Test
    public void addMultipleFiltersAndGetThemBack()
    {
        String role2 = ROLE + 's';
        String accountType2 = ACCOUNT_TYPE + 'S';
        Double filterPercent2 = FILTER_PERCENTAGE + 1;

        addAuditRoleFilter(ROLE, ACCOUNT_TYPE, FILTER_PERCENTAGE);
        addAuditRoleFilter(role2, accountType2, filterPercent2);
        String output = getAuditRoleFilter(ROLE, role2);

        assertThat(output)
            .contains(ROLE)
            .contains(ACCOUNT_TYPE)
            .contains(String.valueOf(FILTER_PERCENTAGE))
            .contains(role2)
            .contains(accountType2)
            .contains(String.valueOf(FILTER_PERCENTAGE + 1))
            .doesNotContain("No audit log filters found.");

        disableAuditRoleFilter(false, ROLE, role2);

        output = getAuditRoleFilter(ROLE, role2);

        assertThat(output)
            .contains(ROLE)
            .contains(ACCOUNT_TYPE)
            .contains(String.valueOf(0.0))
            .contains(role2)
            .contains(accountType2)
            .contains(String.valueOf( 0.0))
            .doesNotContain("No audit log filters found.");

        disableAuditRoleFilter(true, ROLE, role2);

        output = getAuditRoleFilter(ROLE, role2);
        assertThat(output)
            .contains("No audit log filters found.");
    }

    @Test
    public void disableFilterWithoutDeleteTest()
    {
        addAuditRoleFilter(ROLE, "SERVICE", 80.0);

        ToolRunner.invokeNodetool("disableauditlogrolefilter","--refresh", ROLE)
                  .assertOnCleanExit();

        String output = getAuditRoleFilter();
        assertThat(output)
            .contains(ROLE)
            .contains("0.0");
    }

    @Test
    public void disableFilterWithDeleteTest()
    {
        addAuditRoleFilter(ROLE, "SERVICE", 60.0);

        ToolRunner.invokeNodetool("disableauditlogrolefilter", "--refresh", "--delete", ROLE)
                  .assertOnCleanExit();

        String output = getAuditRoleFilter();
        assertThat(output.trim()).isEqualTo("No audit log filters found.");
    }

    @Test
    public void addFilterInvalidRoleTest()
    {
        String err = ToolRunner.invokeNodetool("setauditlogrolefilter",
                                               "--refresh",
                                               "--role", "INVALID_ROLE",
                                               "--account-type", "SERVICE",
                                               "--filter-percent", "25.0")
                               .getStdout();

        assertThat(err).contains("Role must only contain lowercase letters and underscores");
    }

    @Test
    public void addFilterInvalidAccountTypeTest()
    {
        String err = ToolRunner.invokeNodetool("setauditlogrolefilter",
                                               "--refresh",
                                               "--role", ROLE,
                                               "--account-type", "Service",
                                               "--filter-percent", "25.0")
                               .getStdout();

        assertThat(err).contains("Account type must only contain capitals letters");
    }

    @Test
    public void addFilterInvalidPercentTest()
    {
        String err = ToolRunner.invokeNodetool("setauditlogrolefilter",
                                               "--refresh",
                                               "--role", ROLE,
                                               "--account-type", "SERVICE",
                                               "--filter-percent", "150.0")
                               .getStdout();

        assertThat(err).contains("Filter percent must be between 0.0 and 100.0");
    }

    @Test
    public void disableFilterNoRolesSpecifiedTest()
    {
        String stdout = ToolRunner.invokeNodetool("disableauditlogrolefilter")
                                  .getStdout();

        assertThat(stdout.trim()).contains("nodetool: Roles must be specified");
    }

    private void addAuditRoleFilter(String role, String accountType, double filterPercent)
    {
        ToolRunner.invokeNodetool("setauditlogrolefilter",
                                  "--refresh",
                                  "--role", role,
                                  "--account-type", accountType,
                                  "--filter-percent", String.valueOf(filterPercent))
                  .assertOnCleanExit();
    }

    private String disableAuditRoleFilter(Boolean delete, String... roles)
    {
        int idx = 2 + (delete ? 1 : 0);
        int len = idx + roles.length;
        String[] cmd = new String[len];
        if (delete){
            cmd[2] = "--delete";
        }
        cmd[0] = "disableauditlogrolefilter";
        cmd[1] = "--refresh";

        System.arraycopy(roles, 0, cmd, idx, roles.length);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(cmd);
        tool.assertOnCleanExit();
        return tool.getStdout();
    }

    private String getAuditRoleFilter(String... roles)
    {
        String[] cmd = new String[2 + roles.length];
        cmd[0] = "getauditlogrolefilter";
        cmd[1] = "--refresh";
        System.arraycopy(roles, 0, cmd, 2, roles.length);

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(cmd);
        tool.assertOnCleanExit();
        return tool.getStdout();
    }
}
