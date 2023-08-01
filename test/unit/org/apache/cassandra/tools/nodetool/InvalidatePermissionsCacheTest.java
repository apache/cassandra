/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.apache.cassandra.auth.AuthTestUtils.getRolePermissionsReadCount;
import static org.assertj.core.api.Assertions.assertThat;

public class InvalidatePermissionsCacheTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());
        AuthCacheService.initializeAndRegisterCaches();

        List<IResource> resources = Arrays.asList(
                DataResource.root(),
                DataResource.keyspace(KEYSPACE),
                DataResource.allTables(KEYSPACE),
                DataResource.table(KEYSPACE, "t1"),
                RoleResource.root(),
                RoleResource.role("role_x"),
                FunctionResource.root(),
                FunctionResource.keyspace(KEYSPACE),
                // Particular function is excluded from here and covered by a separate test because in order to grant
                // permissions we need to have a function registered. However, the function cannot be registered via
                // CQLTester.createFunction from static contex. That's why we initialize it in a separate test case.
                JMXResource.root(),
                JMXResource.mbean("org.apache.cassandra.auth:type=*"));

        IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
        for (IResource resource : resources)
        {
            Set<Permission> permissions = resource.applicablePermissions();
            authorizer.grant(AuthenticatedUser.SYSTEM_USER, permissions, resource, ROLE_A);
            authorizer.grant(AuthenticatedUser.SYSTEM_USER, permissions, resource, ROLE_B);
        }

        requireNetwork();
        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "invalidatepermissionscache");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool invalidatepermissionscache - Invalidate the permissions cache\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] invalidatepermissionscache\n" +
                        "                [--all-functions] [--all-keyspaces] [--all-mbeans] [--all-roles]\n" +
                        "                [--all-tables] [--function <function>]\n" +
                        "                [--functions-in-keyspace <functions-in-keyspace>]\n" +
                        "                [--keyspace <keyspace>] [--mbean <mbean>] [--role <role>]\n" +
                        "                [--table <table>] [--] [<role>]\n" +
                        "\n" +
                        "OPTIONS\n" +
                        "        --all-functions\n" +
                        "            Invalidate permissions for 'ALL FUNCTIONS'\n" +
                        "\n" +
                        "        --all-keyspaces\n" +
                        "            Invalidate permissions for 'ALL KEYSPACES'\n" +
                        "\n" +
                        "        --all-mbeans\n" +
                        "            Invalidate permissions for 'ALL MBEANS'\n" +
                        "\n" +
                        "        --all-roles\n" +
                        "            Invalidate permissions for 'ALL ROLES'\n" +
                        "\n" +
                        "        --all-tables\n" +
                        "            Invalidate permissions for 'ALL TABLES'\n" +
                        "\n" +
                        "        --function <function>\n" +
                        "            Function to invalidate permissions for (you must specify\n" +
                        "            --functions-in-keyspace for using this option; function format:\n" +
                        "            name[arg1^..^agrN], for example: foo[Int32Type^DoubleType])\n" +
                        "\n" +
                        "        --functions-in-keyspace <functions-in-keyspace>\n" +
                        "            Keyspace to invalidate permissions for\n" +
                        "\n" +
                        "        -h <host>, --host <host>\n" +
                        "            Node hostname or ip address\n" +
                        "\n" +
                        "        --keyspace <keyspace>\n" +
                        "            Keyspace to invalidate permissions for\n" +
                        "\n" +
                        "        --mbean <mbean>\n" +
                        "            MBean to invalidate permissions for\n" +
                        "\n" +
                        "        -p <port>, --port <port>\n" +
                        "            Remote jmx agent port number\n" +
                        "\n" +
                        "        -pp, --print-port\n" +
                        "            Operate in 4.0 mode with hosts disambiguated by port number\n" +
                        "\n" +
                        "        -pw <password>, --password <password>\n" +
                        "            Remote jmx agent password\n" +
                        "\n" +
                        "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" +
                        "            Path to the JMX password file\n" +
                        "\n" +
                        "        --role <role>\n" +
                        "            Role to invalidate permissions for\n" +
                        "\n" +
                        "        --table <table>\n" +
                        "            Table to invalidate permissions for (you must specify --keyspace for\n" +
                        "            using this option)\n" +
                        "\n" +
                        "        -u <username>, --username <username>\n" +
                        "            Remote jmx agent username\n" +
                        "\n" +
                        "        --\n" +
                        "            This option can be used to separate command-line options from the\n" +
                        "            list of argument, (useful when arguments might be mistaken for\n" +
                        "            command-line options\n" +
                        "\n" +
                        "        [<role>]\n" +
                        "            A role for which permissions to specified resources need to be\n" +
                        "            invalidated\n" +
                        "\n" +
                        "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testInvalidatePermissionsWithIncorrectParameters()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "--all-keyspaces");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("No resource options allowed without a <role> being specified"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "role1");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("No resource options specified"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "role1", "--invalid-option");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("A single <role> is only supported / you have a typo in the resource options spelling"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "role1", "--all-tables");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("--all-tables option should be passed along with --keyspace option"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "role1", "--table", "t1");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("--table option should be passed along with --keyspace option"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "role1", "--function", "f[Int32Type]");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("--function option should be passed along with --functions-in-keyspace option"));
        assertThat(tool.getCleanedStderr()).isEmpty();

        tool = ToolRunner.invokeNodetool("invalidatepermissionscache", "role1", "--functions-in-keyspace",
                KEYSPACE, "--function", "f[x]");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout())
                .isEqualTo(wrapByDefaultNodetoolMessage("An error was encountered when looking up function definition: Unable to find abstract-type class 'org.apache.cassandra.db.marshal.x'"));
        assertThat(tool.getCleanedStderr()).isEmpty();
    }

    @Test
    public void testInvalidatePermissionsForEveryResourceExceptFunction()
    {
        assertInvalidation(DataResource.root(), Collections.singletonList("--all-keyspaces"));
        assertInvalidation(DataResource.keyspace(KEYSPACE), Arrays.asList("--keyspace", KEYSPACE));
        assertInvalidation(DataResource.allTables(KEYSPACE), Arrays.asList("--keyspace", KEYSPACE, "--all-tables"));
        assertInvalidation(DataResource.table(KEYSPACE, "t1"),
                Arrays.asList("--keyspace", KEYSPACE, "--table", "t1"));
        assertInvalidation(RoleResource.root(), Collections.singletonList("--all-roles"));
        assertInvalidation(RoleResource.role("role_x"), Arrays.asList("--role", "role_x"));
        assertInvalidation(FunctionResource.root(), Collections.singletonList("--all-functions"));
        assertInvalidation(FunctionResource.keyspace(KEYSPACE), Arrays.asList("--functions-in-keyspace", KEYSPACE));
        assertInvalidation(JMXResource.root(), Collections.singletonList("--all-mbeans"));
        assertInvalidation(JMXResource.mbean("org.apache.cassandra.auth:type=*"),
                Arrays.asList("--mbean", "org.apache.cassandra.auth:type=*"));
    }

    @Test
    public void testInvalidatePermissionsForFunction() throws Throwable
    {
        String keyspaceAndFunctionName = createFunction(KEYSPACE, "int",
                " CREATE FUNCTION %s (val int)" +
                        " CALLED ON NULL INPUT" +
                        " RETURNS int" +
                        " LANGUAGE java" +
                        " AS 'return val;'");
        String functionName = StringUtils.split(keyspaceAndFunctionName, ".")[1];

        FunctionResource resource = FunctionResource.function(KEYSPACE, functionName, Collections.singletonList(Int32Type.instance));
        Set<Permission> permissions = resource.applicablePermissions();
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER, permissions, resource, ROLE_A);
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER, permissions, resource, ROLE_B);

        assertInvalidation(resource,
                Arrays.asList("--functions-in-keyspace", KEYSPACE, "--function", functionName + "[Int32Type]"));
    }

    private void assertInvalidation(IResource resource, List<String> options)
    {
        Set<Permission> dataPermissions = resource.applicablePermissions();

        AuthenticatedUser role = new AuthenticatedUser(ROLE_A.getRoleName());

        // cache permission
        role.getPermissions(resource);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure permission is cached
        assertThat(role.getPermissions(resource)).isEqualTo(dataPermissions);
        assertThat(originalReadsCount).isEqualTo(getRolePermissionsReadCount());

        // invalidate permission
        List<String> args = new ArrayList<>();
        args.add("invalidatepermissionscache");
        args.add(ROLE_A.getRoleName());
        args.addAll(options);
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(args);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure permission is reloaded
        assertThat(role.getPermissions(resource)).isEqualTo(dataPermissions);
        assertThat(originalReadsCount).isLessThan(getRolePermissionsReadCount());
    }

    @Test
    public void testInvalidatePermissionsForAllRoles()
    {
        DataResource rootDataResource = DataResource.root();
        Set<Permission> dataPermissions = rootDataResource.applicablePermissions();

        AuthenticatedUser roleA = new AuthenticatedUser(ROLE_A.getRoleName());
        AuthenticatedUser roleB = new AuthenticatedUser(ROLE_B.getRoleName());

        // cache permissions
        roleA.getPermissions(rootDataResource);
        roleB.getPermissions(rootDataResource);
        long originalReadsCount = getRolePermissionsReadCount();

        // enure permissions are cached
        assertThat(roleA.getPermissions(rootDataResource)).isEqualTo(dataPermissions);
        assertThat(roleB.getPermissions(rootDataResource)).isEqualTo(dataPermissions);
        assertThat(originalReadsCount).isEqualTo(getRolePermissionsReadCount());

        // invalidate both permissions
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("invalidatepermissionscache");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        // ensure permission for roleA is reloaded
        assertThat(roleA.getPermissions(rootDataResource)).isEqualTo(dataPermissions);
        long readsCountAfterFirstReLoad = getRolePermissionsReadCount();
        assertThat(originalReadsCount).isLessThan(readsCountAfterFirstReLoad);

        // ensure permission for roleB is reloaded
        assertThat(roleB.getPermissions(rootDataResource)).isEqualTo(dataPermissions);
        long readsCountAfterSecondReLoad = getRolePermissionsReadCount();
        assertThat(readsCountAfterFirstReLoad).isLessThan(readsCountAfterSecondReLoad);
    }

    private String wrapByDefaultNodetoolMessage(String s)
    {
        return "nodetool: " + s + "\nSee 'nodetool help' or 'nodetool help <command>'.\n";
    }
}
