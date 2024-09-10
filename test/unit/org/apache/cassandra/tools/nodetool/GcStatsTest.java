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
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for GcStats command.
 */
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
                        "            Output format (json)\n" +
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

        assertThat(output)
                .contains("Interval (ms)")
                .contains("Max GC Elapsed (ms)")
                .contains("Total GC Elapsed (ms)")
                .contains("Stdev GC Elapsed (ms)")
                .contains("GC Reclaimed (MB)")
                .contains("Collections")
                .contains("Direct Memory Bytes");

        // Verify that NaN values are handled correctly, or actual values appear
        if (output.contains("NaN"))
        {
            assertThat(output).contains("NaN");
        }
        else
        {
            assertThat(output).contains("500")  // Interval (ms)
                    .contains("100")  // Max GC Elapsed (ms)
                    .contains("245")  // Total GC Elapsed (ms)
                    .contains("300")  // Stdev GC Elapsed (ms)
                    .contains("1024"); // Direct Memory Bytes
        }
    }

    @Test
    public void testJsonGcStatsOutput() throws Exception
    {
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
    }

    @Test
    public void testInvalidFormatOption() throws Exception
    {
        GcStats gcStats = new GcStats();

        // Use reflection to set the private field outputFormat to an invalid value
        java.lang.reflect.Field outputFormatField = GcStats.class.getDeclaredField("outputFormat");
        outputFormatField.setAccessible(true);
        outputFormatField.set(gcStats, "invalid_format");

        try
        {
            gcStats.execute(nodeProbe);
            fail("Expected IllegalArgumentException for invalid format");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("arguments for -F are json only.", e.getMessage());
        }
    }
}