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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class UpdateCIDRGroupTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
        AuthCacheService.initializeAndRegisterCaches();

        startJMXServer();
    }

    @Before
    public void cleanCidrGroups() throws Throwable
    {
        execute(format("TRUNCATE %s.%s", AUTH_KEYSPACE_NAME, CIDR_GROUPS));
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testUpdateCidrGroupDoc()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "updatecidrgroup");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool updatecidrgroup - Insert/Update a cidr group\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] updatecidrgroup [--]\n" +
                      "                [<cidrGroup> <cidr> ...]\n" +
                      "\n" +
                      "OPTIONS\n" +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
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
                      "        -u <username>, --username <username>\n" +
                      "            Remote jmx agent username\n" +
                      "\n" +
                      "        --\n" +
                      "            This option can be used to separate command-line options from the\n" +
                      "            list of argument, (useful when arguments might be mistaken for\n" +
                      "            command-line options\n" +
                      "\n" +
                      "        [<cidrGroup> <cidr> ...]\n" +
                      "            Requires a cidr group name, followed by one or more CIDRs separated\n" +
                      "            by space\n" +
                      "\n" +
                      "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testUpdateCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("listcidrgroups");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test1", "10.11.12.0/24");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test2", "11.11.12.0/24", "12.11.0.0/18");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("listcidrgroups");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("test1");
        assertThat(tool.getStdout()).contains("test2");

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test1");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("10.11.12.0/24");

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test2");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("11.11.12.0/24");
        assertThat(tool.getStdout()).contains("12.11.0.0/18");
    }

    @Test
    public void testUpdateSameCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("listcidrgroups");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test", "10.11.12.0/24");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("10.11.12.0/24");

        tool = ToolRunner.invokeNodetool("updatecidrgroup", "test", "11.11.12.0/24");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("11.11.12.0/24");
    }

    @Test
    public void testUpdateInvalidCidrGroup()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("updatecidrgroup", "test", "10.11.12.0/33");
        assertThat(tool.getStderr()).contains("is not a valid CIDR String");

        tool = ToolRunner.invokeNodetool("listcidrgroups", "test");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();
    }
}
