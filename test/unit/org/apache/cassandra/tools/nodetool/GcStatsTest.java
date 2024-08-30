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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.tools.ToolRunner;

import static org.apache.cassandra.tools.nodetool.GcStats.COLLECTIONS;
import static org.apache.cassandra.tools.nodetool.GcStats.INTERVAL;
import static org.apache.cassandra.tools.nodetool.GcStats.MAX_DIRECT_MEMORY;
import static org.apache.cassandra.tools.nodetool.GcStats.MAX_GC;
import static org.apache.cassandra.tools.nodetool.GcStats.RECLAIMED_GC;
import static org.apache.cassandra.tools.nodetool.GcStats.RESERVED_DIRECT_MEMORY;
import static org.apache.cassandra.tools.nodetool.GcStats.STDEV_GC;
import static org.apache.cassandra.tools.nodetool.GcStats.TOTAL_DIRECT_MEMORY;
import static org.apache.cassandra.tools.nodetool.GcStats.TOTAL_GC;
import static org.assertj.core.api.Assertions.assertThat;

public class GcStatsTest extends CQLTester
{
    GCInspector gcInspector;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Before
    public void before()
    {
        gcInspector = new GCInspector();
    }

    @Test
    @SuppressWarnings("SingleCharacterStringConcatenation")
    public void testHelp()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "gcstats");
        tool.assertOnCleanExit();
        String help = "NAME\n" +
                      "        nodetool gcstats - Print GC Statistics\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] gcstats\n" +
                      "                [(-H | --human-readable)]\n" +
                      "\n" +
                      "OPTIONS\n" +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
                      "\n" +
                      "        -H, --human-readable\n" +
                      "            Display gcstats in the table format for human-readable\n" +
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
                      "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testWithoutAnyOption()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gcstats");
        tool.assertOnCleanExit();
        testBasicOutput(tool.getStdout());
    }

    @Test
    public void testWithHOption()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("gcstats", "--human-readable");
        tool.assertOnCleanExit();
        String gcStatsOutput = tool.getStdout();
        testBasicOutput(gcStatsOutput);

        assertThat(gcStatsOutput).contains(MAX_DIRECT_MEMORY);
        assertThat(gcStatsOutput).contains(RESERVED_DIRECT_MEMORY);

        String total = StringUtils.substringBetween(gcStatsOutput, TOTAL_DIRECT_MEMORY, "\n").trim();
        assertThat(Long.parseLong(total)).isGreaterThan(0);
        String max = StringUtils.substringBetween(gcStatsOutput, MAX_DIRECT_MEMORY, "\n").trim();
        assertThat(Long.parseLong(max)).isGreaterThan(0);
        String reserved = StringUtils.substringBetween(gcStatsOutput, RESERVED_DIRECT_MEMORY, "\n").trim();
        assertThat(Long.parseLong(reserved)).isGreaterThan(0);
    }

    private void testBasicOutput(final String output)
    {
        assertThat(output).contains(INTERVAL);
        assertThat(output).contains(MAX_GC);
        assertThat(output).contains(TOTAL_GC);
        assertThat(output).contains(STDEV_GC);
        assertThat(output).contains(RECLAIMED_GC);
        assertThat(output).contains(COLLECTIONS);
        assertThat(output).contains(TOTAL_DIRECT_MEMORY);
    }
}
