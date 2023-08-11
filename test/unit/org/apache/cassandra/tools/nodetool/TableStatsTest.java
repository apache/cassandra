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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.JsonUtils;
import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TableStatsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "tablestats");
        tool.assertOnCleanExit();

        String help =   "NAME\n" +
                        "        nodetool tablestats - Print statistics on tables\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                        "                [(-u <username> | --username <username>)] tablestats\n" +
                        "                [(-F <format> | --format <format>)] [(-H | --human-readable)] [-i]\n" +
                        "                [(-l | --sstable-location-check)] [(-s <sort_key> | --sort <sort_key>)]\n" +
                        "                [(-t <top> | --top <top>)] [--] [<keyspace.table>...]\n" +
                        "\n" + 
                        "OPTIONS\n" +
                        "        -F <format>, --format <format>\n" +
                        "            Output format (json, yaml)\n" + 
                        "\n" + 
                        "        -h <host>, --host <host>\n" + 
                        "            Node hostname or ip address\n" + 
                        "\n" + 
                        "        -H, --human-readable\n" + 
                        "            Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB\n" + 
                        "\n" + 
                        "        -i\n" + 
                        "            Ignore the list of tables and display the remaining tables\n" +
                        "\n" +
                        "        -l, --sstable-location-check\n" +
                        "            Check whether or not the SSTables are in the correct location.\n" +
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
                        "        -s <sort_key>, --sort <sort_key>\n" + 
                        "            Sort tables by specified sort key\n" + 
                        "            (average_live_cells_per_slice_last_five_minutes,\n" + 
                        "            average_tombstones_per_slice_last_five_minutes,\n" + 
                        "            bloom_filter_false_positives, bloom_filter_false_ratio,\n" + 
                        "            bloom_filter_off_heap_memory_used, bloom_filter_space_used,\n" + 
                        "            compacted_partition_maximum_bytes, compacted_partition_mean_bytes,\n" + 
                        "            compacted_partition_minimum_bytes,\n" + 
                        "            compression_metadata_off_heap_memory_used, full_name,\n" +
                        "            index_summary_off_heap_memory_used, local_read_count,\n" +
                        "            local_read_latency_ms, local_write_latency_ms,\n" + 
                        "            maximum_live_cells_per_slice_last_five_minutes,\n" + 
                        "            maximum_tombstones_per_slice_last_five_minutes, memtable_cell_count,\n" + 
                        "            memtable_data_size, memtable_off_heap_memory_used,\n" + 
                        "            memtable_switch_count, number_of_partitions_estimate,\n" + 
                        "            off_heap_memory_used_total, pending_flushes, percent_repaired,\n" + 
                        "            read_latency, reads, space_used_by_snapshots_total, space_used_live,\n" + 
                        "            space_used_total, sstable_compression_ratio, sstable_count,\n" + 
                        "            table_name, write_latency, writes, max_sstable_size,\n" +
                        "            local_read_write_ratio, twcs_max_duration)\n" +
                        "\n" + 
                        "        -t <top>, --top <top>\n" + 
                        "            Show only the top K tables for the sort key (specify the number K of\n" + 
                        "            tables to be shown\n" + 
                        "\n" + 
                        "        -u <username>, --username <username>\n" + 
                        "            Remote jmx agent username\n" + 
                        "\n" + 
                        "        --\n" + 
                        "            This option can be used to separate command-line options from the\n" + 
                        "            list of argument, (useful when arguments might be mistaken for\n" + 
                        "            command-line options\n" + 
                        "\n" + 
                        "        [<keyspace.table>...]\n" + 
                        "            List of tables (or keyspace) names\n" + 
                        "\n" + 
                        "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testTableStats()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);

        tool = ToolRunner.invokeNodetool("tablestats", "system_distributed");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_distributed");
        assertThat(tool.getStdout()).doesNotContain("Keyspace : system_schema");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);
    }

    @Test
    public void testTableIgnoreArg()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-i", "system_schema.aggregates");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Keyspace: system_schema");
        assertThat(tool.getStdout()).doesNotContain("Table: system_schema.aggregates");
        assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isGreaterThan(1);
    }

    @Test
    public void testHumanReadableArg()
    {
        Arrays.asList("-H", "--human-readable").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg);
            tool.assertOnCleanExit();
            assertThat(tool.getStdout()).contains(" KiB");
        });
    }

    @Test
    public void testSortArg()
    {
        Pattern regExp = Pattern.compile("((?m)Table: .*$)");

        Arrays.asList("-s", "--sort").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "table_name");
            Matcher m = regExp.matcher(tool.getStdout());
            ArrayList<String> orig = new ArrayList<>();
            while (m.find())
                orig.add(m.group(1));

            tool = ToolRunner.invokeNodetool("tablestats", arg, "sstable_count");
            tool.assertOnCleanExit();
            m = regExp.matcher(tool.getStdout());
            ArrayList<String> sorted = new ArrayList<>();
            while (m.find())
                sorted.add(m.group(1));

            assertThat(sorted).isNotEqualTo(orig);
            Collections.sort(orig);
            Collections.sort(sorted);
            assertThat(sorted).isEqualTo(orig);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "wrongSort");
        assertThat(tool.getStdout()).contains("argument for sort must be one of");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
    }

    @Test
    public void testTopArg()
    {
        Arrays.asList("-t", "--top").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", arg, "1");
            tool.assertOnCleanExit();
            assertThat(StringUtils.countMatches(tool.getStdout(), "Table:")).isEqualTo(1);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", "-t", "-1");
        tool.assertCleanStdErr();
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("argument for top must be a positive integer");
    }

    @Test
    public void testSSTableLocationCheckArg()
    {
        Arrays.asList("-l", "--sstable-location-check").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "system.local");
            tool.assertOnCleanExit();
            assertThat(StringUtils.countMatches(tool.getStdout(), "SSTables in correct location: ")).isEqualTo(1);
        });

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", "system.local");
        tool.assertCleanStdErr();
        assertThat(tool.getStdout()).doesNotContain("SSTables in correct location: ");
    }

    @Test
    public void testFormatJson()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "json");
            tool.assertOnCleanExit();
            String json = tool.getStdout();
            assertThatCode(() -> JsonUtils.JSON_OBJECT_MAPPER.readTree(json)).doesNotThrowAnyException();
            assertThat(json).containsPattern("\"sstable_count\"\\s*:\\s*[0-9]+")
                            .containsPattern("\"old_sstable_count\"\\s*:\\s*[0-9]+");
        });
    }

    @Test
    public void testFormatYaml()
    {
        Arrays.asList("-F", "--format").forEach(arg -> {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "yaml");
            tool.assertOnCleanExit();
            String yaml = tool.getStdout();
            assertThatCode(() -> new Yaml().load(yaml)).doesNotThrowAnyException();
            assertThat(yaml).containsPattern("sstable_count:\\s*[0-9]+")
                            .containsPattern("old_sstable_count:\\s*[0-9]+");
        });
    }
}
