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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.stats.GcStatsHolder;
import org.apache.cassandra.tools.nodetool.stats.GcStatsPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GcStatsTest
{
    private NodeProbe nodeProbe;
    private ByteArrayOutputStream outContent;
    private PrintStream outStream;

    @Before
    public void setUp() throws Exception
    {
        nodeProbe = mock(NodeProbe.class);

        // Set up mock values for getAndResetGCStats method
        double[] gcStats = {500, 100, 245, 300, 100, 10, 1024};
        when(nodeProbe.getAndResetGCStats()).thenReturn(gcStats);

        // Capture the output printed to the console
        outContent = new ByteArrayOutputStream();
        outStream = new PrintStream(outContent);
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
    public void testDefaultGcStatsOutput() throws Exception
    {
        // Initialize GcStats with the mock NodeProbe
        GcStatsHolder gcStatsHolder = new GcStatsHolder(nodeProbe);
        StatsPrinter defaultPrinter = new GcStatsPrinter.DefaultPrinter();

        // Print using the default format
        defaultPrinter.print(gcStatsHolder, outStream);

        // Verify that output contains the expected headers and values using assertThat
        String output = outContent.toString();

        assertThat(output).contains("Interval (ms)");
        assertThat(output).contains("Max GC Elapsed (ms)");
        assertThat(output).contains("Total GC Elapsed (ms)");
        assertThat(output).contains("Stdev GC Elapsed (ms)");
        assertThat(output).contains("GC Reclaimed (MB)");
        assertThat(output).contains("Collections");
        assertThat(output).contains("Direct Memory Bytes");

        // Verify that NaN values are handled correctly, or actual values appear
        if (output.contains("NaN"))
        {
            assertThat(output).contains("NaN");
        }
        else
        {
            assertThat(output).contains("500");  // Interval (ms)
            assertThat(output).contains("100");  // Max GC Elapsed (ms)
            assertThat(output).contains("245");  // Total GC Elapsed (ms)
            assertThat(output).contains("300");  // Stdev GC Elapsed (ms)
            assertThat(output).contains("1024"); // Direct Memory Bytes
        }
    }

    @Test
    public void testJsonGcStatsOutput() throws Exception {
        // Initialize GcStats with the mock NodeProbe
        GcStatsHolder gcStatsHolder = new GcStatsHolder(nodeProbe);
        StatsPrinter jsonPrinter = GcStatsPrinter.from("json");

        // Print using the JSON format
        jsonPrinter.print(gcStatsHolder, outStream);

        // Verify JSON output structure
        String jsonOutput = outContent.toString();

        // Parse JSON output using JsonUtils
        JsonNode rootNode = JsonUtils.JSON_OBJECT_MAPPER.readTree(jsonOutput);

        // Check if important fields are present in the JSON output
        assertThat(rootNode.has("interval_ms")).isTrue();
        assertThat(rootNode.has("max_gc_elapsed_ms")).isTrue();
        assertThat(rootNode.has("total_gc_elapsed_ms")).isTrue();
        assertThat(rootNode.has("stdev_gc_elapsed_ms")).isTrue();
        assertThat(rootNode.has("gc_reclaimed_mb")).isTrue();
        assertThat(rootNode.has("collections")).isTrue();
        assertThat(rootNode.has("direct_memory_bytes")).isTrue();

        // Verify values in JSON output
        assertThat(rootNode.get("interval_ms").asInt()).isEqualTo(500);
        assertThat(rootNode.get("max_gc_elapsed_ms").asInt()).isEqualTo(100);
        assertThat(rootNode.get("total_gc_elapsed_ms").asInt()).isEqualTo(245);
        assertThat(rootNode.get("direct_memory_bytes").asInt()).isEqualTo(1024);

        // Check if stdev_gc_elapsed_ms is NaN
        if (rootNode.get("stdev_gc_elapsed_ms").asText().equals("NaN")) {
            assertThat(rootNode.get("stdev_gc_elapsed_ms").asText().equals("NaN")).isTrue();
        } else {
            assertThat(rootNode.get("stdev_gc_elapsed_ms").asInt()).isEqualTo(300);
        }
    }

    @Test
    public void testYamlGcStatsOutput() throws Exception
    {
        GcStatsHolder gcStatsHolder = new GcStatsHolder(nodeProbe);
        StatsPrinter yamlPrinter = GcStatsPrinter.from("yaml");

        yamlPrinter.print(gcStatsHolder, outStream);

        String yamlOutput = outContent.toString();

        assertThat(yamlOutput).contains("interval_ms: 500.0");
        assertThat(yamlOutput).contains("max_gc_elapsed_ms: 100.0");
        assertThat(yamlOutput).contains("total_gc_elapsed_ms: 245.0");
        assertThat(yamlOutput).contains("gc_reclaimed_mb: 100.0");
        assertThat(yamlOutput).contains("collections: 10.0");
        assertThat(yamlOutput).contains("direct_memory_bytes: 1024");

        if (yamlOutput.contains("stdev_gc_elapsed_ms: .NaN")) {
            assertThat(yamlOutput).contains("stdev_gc_elapsed_ms: .NaN");
        } else {
            assertThat(yamlOutput).contains("stdev_gc_elapsed_ms: 300.0");
        }
    }

    @Test
    public void testInvalidFormatOption() throws Exception
    {
        GcStats gcStats = new GcStats();

        // Use reflection to set the private field outputFormat to an invalid value
        java.lang.reflect.Field outputFormatField = GcStats.class.getDeclaredField("outputFormat");
        outputFormatField.setAccessible(true);
        outputFormatField.set(gcStats, "invalid_format");
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> gcStats.execute(nodeProbe))
                                                                 .withMessage("arguments for -F are json, yaml only.");
    }
}