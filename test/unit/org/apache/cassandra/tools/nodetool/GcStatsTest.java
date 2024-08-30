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

import java.util.Arrays;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

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
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "gcstats");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool gcstats - Print GC Statistics\n" +
                        "\n" +
                        "SYNOPSIS\n" +
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] gcstats\n" +
                        "                [(-F <format> | --format <format>)]\n" +
                        "\n" +
                        "OPTIONS\n" +
                        "        -F <format>, --format <format>\n" +
                        "            Output format (json, yaml)\n" +
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
                        "        -u <username>, --username <username>\n" +
                        "            Remote jmx agent username\n" +
                        "\n";

        assertThat(tool.getStdout().trim()).isEqualTo(help.trim());
    }

    @Test
    public void testDefaultGcStatsOutput()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gcstats");
        tool.assertOnCleanExit();
        String output = tool.getStdout();
        assertThat(output).contains("Interval (ms)");
        assertThat(output).contains("Max GC Elapsed (ms)");
        assertThat(output).contains("Total GC Elapsed (ms)");
        assertThat(output).contains("GC Reclaimed (MB)");
        assertThat(output).contains("Collections");
        assertThat(output).contains("Direct Memory Bytes");
    }

    @Test
    public void testJsonGcStatsOutput()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gcstats", arg, "json");
            tool.assertOnCleanExit();
            String json = tool.getStdout();
            assertThatCode(() -> JsonUtils.JSON_OBJECT_MAPPER.readTree(json)).doesNotThrowAnyException();
            assertThat(json).containsPattern("\"interval_ms\"");
            assertThat(json).containsPattern("\"stdev_gc_elapsed_ms\"");
            assertThat(json).containsPattern("\"collections\"");
            assertThat(json).containsPattern("\"max_gc_elapsed_ms\"");
            assertThat(json).containsPattern("\"gc_reclaimed_mb\"");
            assertThat(json).containsPattern("\"total_gc_elapsed_ms\"");
            assertThat(json).containsPattern("\"direct_memory_bytes\"");
        });
    }

    @Test
    public void testYamlGcStatsOutput()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gcstats", arg, "yaml");
            tool.assertOnCleanExit();
            String yamlOutput = tool.getStdout();
            Yaml yaml = new Yaml();
            assertThatCode(() -> yaml.load(yamlOutput)).doesNotThrowAnyException();
            assertThat(yamlOutput).containsPattern("interval_ms:");
            assertThat(yamlOutput).containsPattern("stdev_gc_elapsed_ms:");
            assertThat(yamlOutput).containsPattern("collections:");
            assertThat(yamlOutput).containsPattern("max_gc_elapsed_ms:");
            assertThat(yamlOutput).containsPattern("gc_reclaimed_mb:");
            assertThat(yamlOutput).containsPattern("total_gc_elapsed_ms:");
            assertThat(yamlOutput).containsPattern("direct_memory_bytes:");
        });
    }

    @Test
    public void testInvalidFormatOption() throws Exception
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gcstats", "-F", "invalid_format");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("arguments for -F are json, yaml only.");
    }
}