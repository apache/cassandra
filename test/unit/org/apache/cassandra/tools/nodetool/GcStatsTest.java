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
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.tools.nodetool.stats.GcStatsHolder;
import org.apache.cassandra.utils.JsonUtils;
import org.yaml.snakeyaml.Yaml;

import static java.lang.Double.parseDouble;
import static java.util.Arrays.asList;
import static org.apache.cassandra.tools.nodetool.stats.GcStatsHolder.MAX_DIRECT_MEMORY;
import static org.apache.cassandra.tools.nodetool.stats.GcStatsHolder.RESERVED_DIRECT_MEMORY;
import static org.apache.cassandra.tools.nodetool.stats.GcStatsHolder.ALLOCATED_DIRECT_MEMORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class GcStatsTest extends CQLTester
{
    @BeforeClass
    public static void setUp() throws Exception
    {
        requireNetwork();
        startJMXServer();
        GCInspector.register();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeNodetool("help", "gcstats");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool gcstats - Print GC Statistics\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] gcstats\n" +
                      "                [(-F <format> | --format <format>)] [(-H | --human-readable)]\n" +
                      "\n" +
                      "OPTIONS\n" +
                      "        -F <format>, --format <format>\n" +
                      "            Output format (json, yaml, table)\n" +
                      "\n" +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
                      "\n" +
                      "        -H, --human-readable\n" +
                      "            Display gcstats with human-readable units\n" +
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
                      "\n";

        assertThat(tool.getStdout().trim()).isEqualTo(help.trim());
    }

    @Test
    public void testDefaultGcStatsOutput()
    {
        ToolResult tool = ToolRunner.invokeNodetool("gcstats");
        tool.assertOnCleanExit();
        String output = tool.getStdout();
        for (String value : GcStatsHolder.columnDescriptionMap.values())
            assertThat(output).contains(value);
    }

    @Test
    public void testJsonGcStatsOutput()
    {
        asList("-F", "--format").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("gcstats", arg, "json");
            tool.assertOnCleanExit();
            String json = tool.getStdout();
            assertThatCode(() -> JsonUtils.JSON_OBJECT_MAPPER.readTree(json)).doesNotThrowAnyException();

            for (String key : GcStatsHolder.columnDescriptionMap.keySet())
                assertThat(json).contains(key);
        });
    }

    @Test
    public void testYamlGcStatsOutput()
    {
        asList("-F", "--format").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("gcstats", arg, "yaml");
            tool.assertOnCleanExit();
            String yamlOutput = tool.getStdout();
            Yaml yaml = new Yaml();
            assertThatCode(() -> yaml.load(yamlOutput)).doesNotThrowAnyException();

            for (String key : GcStatsHolder.columnDescriptionMap.keySet())
                assertThat(yamlOutput).containsPattern(key);
        });
    }

    @Test
    public void testInvalidFormatOption()
    {
        ToolResult tool = ToolRunner.invokeNodetool("gcstats", "-F", "invalid_format");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("arguments for -F are json, yaml, table only.");
    }

    @Test
    public void testWithoutNoOption()
    {
        ToolResult tool = ToolRunner.invokeNodetool("gcstats");
        tool.assertOnCleanExit();

        for (String value : GcStatsHolder.columnDescriptionMap.values())
            assertThat(tool.getStdout()).contains(value);
    }

    @Test
    public void testWithHumanReadableOption()
    {
        ToolResult tool = ToolRunner.invokeNodetool("gcstats", "--human-readable", "-F", "table");
        tool.assertOnCleanExit();
        String gcStatsOutput = tool.getStdout();

        for (String value : GcStatsHolder.columnDescriptionMap.values())
            assertThat(tool.getStdout()).contains(value);

        String total = StringUtils.substringBetween(gcStatsOutput, GcStatsHolder.columnDescriptionMap.get(ALLOCATED_DIRECT_MEMORY), "\n").trim();
        assertThat(parseDouble(total.split(" ")[0])).isGreaterThan(0);
        String max = StringUtils.substringBetween(gcStatsOutput, GcStatsHolder.columnDescriptionMap.get(MAX_DIRECT_MEMORY), "\n").trim();
        assertThat(parseDouble(max.split(" ")[0])).isGreaterThan(0);
        String reserved = StringUtils.substringBetween(gcStatsOutput, GcStatsHolder.columnDescriptionMap.get(RESERVED_DIRECT_MEMORY), "\n").trim();
        assertThat(parseDouble(reserved.split(" ")[0])).isGreaterThan(0);
    }
}