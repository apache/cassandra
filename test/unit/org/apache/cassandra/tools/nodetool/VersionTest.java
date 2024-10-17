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

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class VersionTest extends CQLTester
{

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testHelp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "version");
        tool.assertOnExitCode();

        String help =   "NAME\n" +
                        "        nodetool version - Print cassandra version\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] version [(-v | --verbose)]\n" +
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
                        "        -v, --verbose\n" +
                        "            Include additional information\n" +
                        "\n" +
                        "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testBasic()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("version");
        tool.assertOnExitCode();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("ReleaseVersion:\\s+\\S+");
    }

    @Test
    public void testVOption()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("version", "-v");
        tool.assertOnExitCode();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("ReleaseVersion:\\s+\\S+");
        assertThat(stdout).containsPattern("BuildDate:\\s+\\S+");
        assertThat(stdout).containsPattern("GitSHA:\\s+\\S+");
        assertThat(stdout).containsPattern("JVM vendor/version:\\s+\\S+");
    }
}
