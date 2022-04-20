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

import org.apache.commons.lang3.StringUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class DataPathsTest extends CQLTester
{
    private static final String SUBCOMMAND = "datapaths";
    
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testHelp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", SUBCOMMAND);
        tool.assertOnCleanExit();
        
        String help = "NAME\n" +
                      "        nodetool datapaths - Print all directories where data of tables are\n" +
                      "        stored\n" +
                      '\n' +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] datapaths\n" +
                      "                [(-F <format> | --format <format>)] [--] [<keyspace.table>...]\n" +
                      '\n' +
                      "OPTIONS\n" +
                      "        -F <format>, --format <format>\n" +
                      "            Output format (json, yaml)\n" +
                      '\n' +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
                      '\n' +
                      "        -p <port>, --port <port>\n" +
                      "            Remote jmx agent port number\n" +
                      '\n' +
                      "        -pp, --print-port\n" +
                      "            Operate in 4.0 mode with hosts disambiguated by port number\n" +
                      '\n' +
                      "        -pw <password>, --password <password>\n" +
                      "            Remote jmx agent password\n" +
                      '\n' +
                      "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" +
                      "            Path to the JMX password file\n" +
                      '\n' +
                      "        -u <username>, --username <username>\n" +
                      "            Remote jmx agent username\n" +
                      '\n' +
                      "        --\n" +
                      "            This option can be used to separate command-line options from the\n" +
                      "            list of argument, (useful when arguments might be mistaken for\n" +
                      "            command-line options\n" +
                      '\n' +
                      "        [<keyspace.table>...]\n" +
                      "            List of table (or keyspace) names\n" +
                      '\n' +
                      '\n';
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testAllOutput()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND);
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Keyspace:")).isGreaterThan(1);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tTable:")).isGreaterThan(1);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tPaths:")).isGreaterThan(1);
    }

    @Test
    public void testSelectedKeyspace()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "system_traces");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_traces");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Keyspace:")).isEqualTo(1);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tTable:")).isGreaterThan(1);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tPaths:")).isGreaterThan(1);
    }

    @Test
    public void testSelectedMultipleKeyspaces()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "system_traces", "system_auth");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_traces");
        assertThat(tool.getStdout()).contains("Keyspace: system_auth");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Keyspace:")).isEqualTo(2);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tTable:")).isGreaterThan(1);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tPaths:")).isGreaterThan(1);
    }

    @Test
    public void testSelectedTable()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "system_auth.roles");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_auth");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Keyspace:")).isEqualTo(1);
        assertThat(tool.getStdout()).contains("Table: roles");
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tTable:")).isEqualTo(1);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tPaths:")).isEqualTo(1);
    }

    @Test
    public void testSelectedMultipleTables()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "system_auth.roles", "system_auth.role_members");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_auth");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Keyspace:")).isEqualTo(1);
        assertThat(tool.getStdout()).contains("Table: roles");
        assertThat(tool.getStdout()).contains("Table: role_members");
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tTable:")).isEqualTo(2);
        assertThat(StringUtils.countMatches(tool.getStdout(), "\tPaths:")).isEqualTo(2);
    }

    @Test
    public void testFormatArgJson()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "--format", "json");
        tool.assertOnCleanExit();
    }

    @Test
    public void testFormatArgYaml()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "--format", "yaml");
        tool.assertOnCleanExit();
    }

    @Test
    public void testFormatArgBad()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(SUBCOMMAND, "--format", "bad");
        assertThat(tool.getStdout()).contains("arguments for -F are yaml and json only.");
    }
}
