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

package org.apache.cassandra.tools.nodetool.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class NodetoolTableStatsTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer();
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeNodetool("help", "tablestats");
        String help =   "NAME\n" +
                        "        nodetool tablestats - Print statistics on tables\n" + 
                        "\n" + 
                        "SYNOPSIS\n" + 
                        "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" + 
                        "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" + 
                        "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" + 
                        "                [(-u <username> | --username <username>)] tablestats\n" + 
                        "                [(-F <format> | --format <format>)] [(-H | --human-readable)] [-i]\n" + 
                        "                [(-s <sort_key> | --sort <sort_key>)] [(-t <top> | --top <top>)] [--]\n" + 
                        "                [<keyspace.table>...]\n" + 
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
                        "            compression_metadata_off_heap_memory_used, dropped_mutations,\n" + 
                        "            full_name, index_summary_off_heap_memory_used, local_read_count,\n" + 
                        "            local_read_latency_ms, local_write_latency_ms,\n" + 
                        "            maximum_live_cells_per_slice_last_five_minutes,\n" + 
                        "            maximum_tombstones_per_slice_last_five_minutes, memtable_cell_count,\n" + 
                        "            memtable_data_size, memtable_off_heap_memory_used,\n" + 
                        "            memtable_switch_count, number_of_partitions_estimate,\n" + 
                        "            off_heap_memory_used_total, pending_flushes, percent_repaired,\n" + 
                        "            read_latency, reads, space_used_by_snapshots_total, space_used_live,\n" + 
                        "            space_used_total, sstable_compression_ratio, sstable_count,\n" + 
                        "            table_name, write_latency, writes)\n" + 
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
        Assertions.assertThat(tool.getStdout()).isEqualTo(help);
        tool.assertOnCleanExit();
    }

    @Test
    public void testTableStats()
    {
        ToolResult tool = ToolRunner.invokeNodetool("tablestats");

        assertThat(tool.getStdout(), CoreMatchers.containsString("Keyspace : system_schema"));
        assertTrue(StringUtils.countMatches(tool.getStdout(), "Table:") > 1);
        tool.assertOnCleanExit();

        tool = ToolRunner.invokeNodetool("tablestats", "system_distributed");
        assertThat(tool.getStdout(), CoreMatchers.containsString("Keyspace : system_distributed"));
        assertThat(tool.getStdout(), CoreMatchers.not(CoreMatchers.containsString("Keyspace : system_schema")));
        assertTrue(StringUtils.countMatches(tool.getStdout(), "Table:") > 1);
        tool.assertOnCleanExit();
    }

    @Test
    public void testTableIgnoreArg()
    {
        ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-i", "system_schema.aggregates");

        assertThat(tool.getStdout(), CoreMatchers.containsString("Keyspace : system_schema"));
        assertThat(tool.getStdout(), CoreMatchers.not(CoreMatchers.containsString("Table: system_schema.aggregates")));
        assertTrue(StringUtils.countMatches(tool.getStdout(), "Table:") > 1);
        tool.assertOnCleanExit();
    }

    @Test
    public void testHumanReadableArg()
    {
        Arrays.asList("-H", "--human-readable").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg);
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsString(" KiB"));
            assertTrue(String.format("Expected empty stderr for option [%s] but found: %s",
                                     arg,
                                     tool.getCleanedStderr()),
                       tool.getCleanedStderr().isEmpty());
            assertEquals(String.format("Expected exit code 0 for option [%s] but found: %s", arg, tool.getExitCode()),
                         0,
                         tool.getExitCode());
        });
    }

    @Test
    public void testSortArg()
    {
        Pattern regExp = Pattern.compile("((?m)Table: .*$)");

        Arrays.asList("-s", "--sort").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("tablestats", arg, "table_name");
            Matcher m = regExp.matcher(tool.getStdout());
            ArrayList<String> orig = new ArrayList<>();
            while (m.find())
                orig.add(m.group(1));

            tool = ToolRunner.invokeNodetool("tablestats", arg, "sstable_count");
            m = regExp.matcher(tool.getStdout());
            ArrayList<String> sorted = new ArrayList<>();
            while (m.find())
                sorted.add(m.group(1));

            assertNotEquals("Arg: [" + arg + "]", orig, sorted);
            Collections.sort(orig);
            Collections.sort(sorted);
            assertEquals("Arg: [" + arg + "]", orig, sorted);
            assertTrue("Arg: [" + arg + "]", tool.getCleanedStderr().isEmpty());
            assertEquals(0, tool.getExitCode());
        });

        ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "wrongSort");
        assertThat(tool.getStdout(), CoreMatchers.containsString("argument for sort must be one of"));
        tool.assertCleanStdErr();
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testTopArg()
    {
        Arrays.asList("-t", "--top").forEach(arg -> {
            ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", arg, "1");
            assertEquals("Arg: [" + arg + "]", StringUtils.countMatches(tool.getStdout(), "Table:"), 1);
            assertTrue("Arg: [" + arg + "]", tool.getCleanedStderr().isEmpty());
            assertEquals("Arg: [" + arg + "]", 0, tool.getExitCode());
        });

        ToolResult tool = ToolRunner.invokeNodetool("tablestats", "-s", "table_name", "-t", "-1");
        assertThat(tool.getStdout(), CoreMatchers.containsString("argument for top must be a positive integer"));
        tool.assertCleanStdErr();
        assertEquals(1, tool.getExitCode());
    }
}
