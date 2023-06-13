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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.auth.AuthTestUtils.ROLE_A;
import static org.apache.cassandra.auth.AuthTestUtils.ROLE_B;
import static org.assertj.core.api.Assertions.assertThat;

public class VerifyTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.prepareServer();
        AuthTestUtils.LocalCassandraRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        SchemaLoader.setupAuth(roleManager,
                               new AuthTestUtils.LocalPasswordAuthenticator(),
                               new AuthTestUtils.LocalCassandraAuthorizer(),
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer(),
                               new AuthTestUtils.LocalCassandraCIDRAuthorizer());

        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_A, AuthTestUtils.getLoginRoleOptions());
        roleManager.createRole(AuthenticatedUser.SYSTEM_USER, ROLE_B, AuthTestUtils.getLoginRoleOptions());

        startJMXServer();
    }

    /**
     * We calcify the help file as last seen as a "trigger" to notify a developer that, upon addition of a new flag or
     * functionality to this tool option, they will need to update help output and/or documentation as necessary.
     */
    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "verify");
        tool.assertOnCleanExit();

        String help =
        "NAME\n" +
        "        nodetool verify - Verify (check data checksum for) one or more tables\n" +
        "\n" +
        "SYNOPSIS\n" +
        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
        "                [(-u <username> | --username <username>)] verify\n" +
        "                [(-c | --check-version)] [(-d | --dfp)] [(-e | --extended-verify)]\n" +
        "                [(-f | --force)] [(-q | --quick)] [(-r | --rsc)] [(-t | --check-tokens)]\n" +
        "                [--] [<keyspace> <tables>...]\n" +
        "\n" +

        "OPTIONS\n" +
        "        -c, --check-version\n" +
        "            Also check that all sstables are the latest version\n" +
        "\n" +
        "        -d, --dfp\n" +
        "            Invoke the disk failure policy if a corrupt sstable is found\n" +
        "\n" +
        "        -e, --extended-verify\n" +
        "            Verify each cell data, beyond simply checking sstable checksums\n" +
        "\n" +
        "        -f, --force\n" +
        "            Override disabling of verify tool - see CASSANDRA-9947 for caveats\n" +
        "\n" +
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
        "        -q, --quick\n" +
        "            Do a quick check - avoid reading all data to verify checksums\n" +
        "\n" +
        "        -r, --rsc\n" +
        "            Mutate the repair status on corrupt sstables\n" +
        "\n" +
        "        -t, --check-tokens\n" +
        "            Verify that all tokens in sstables are owned by this node\n" +
        "\n" +
        "        -u <username>, --username <username>\n" +
        "            Remote jmx agent username\n" +
        "\n" +
        "        --\n" +
        "            This option can be used to separate command-line options from the\n" +
        "            list of argument, (useful when arguments might be mistaken for\n" +
        "            command-line options\n" +
        "\n" +
        "        [<keyspace> <tables>...]\n" +
        "            The keyspace followed by one or many tables\n" +
        "\n\n";

        assertThat(tool.getStdout()).isEqualTo(help);
    }
}
