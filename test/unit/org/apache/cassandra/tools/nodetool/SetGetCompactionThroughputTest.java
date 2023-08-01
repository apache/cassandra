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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.apache.cassandra.tools.ToolRunner.ToolResult;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@code nodetool setcompactionthroughput} and {@code nodetool getcompactionthroughput}.
 */
public class SetGetCompactionThroughputTest extends CQLTester
{
    private static final int MAX_INT_CONFIG_VALUE_IN_MBIT = Integer.MAX_VALUE - 1;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testNull()
    {
        assertSetInvalidThroughput(null, "Required parameters are missing: compaction_throughput");
    }

    @Test
    public void testPositive()
    {
        assertSetGetValidThroughput(7);
    }

    @Test
    public void testMaxValue()
    {
        assertSetGetValidThroughput(MAX_INT_CONFIG_VALUE_IN_MBIT);
    }

    @Test
    public void testUpperBound()
    {
        assertSetInvalidThroughputMib(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void testZero()
    {
        assertSetGetValidThroughput(0);
    }

    @Test
    public void testUnparseable()
    {
        assertSetInvalidThroughput("1.2", "compaction_throughput: can not convert \"1.2\" to a Integer");
        assertSetInvalidThroughput("value", "compaction_throughput: can not convert \"value\" to a Integer");
        assertSetInvalidThroughput();
        assertPreciseMibFlagNeeded();
    }

    private static void assertSetGetValidThroughput(int throughput)
    {
        ToolResult tool = invokeNodetool("setcompactionthroughput", String.valueOf(throughput));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughput(throughput);
        assertGetThroughputDouble(throughput);
    }

    private static void assertSetInvalidThroughput(String throughput, String expectedErrorMessage)
    {
        ToolResult tool = throughput == null ? invokeNodetool("setcompactionthroughput")
                                             : invokeNodetool("setcompactionthroughput", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains(expectedErrorMessage);
    }

    private static void assertSetInvalidThroughput()
    {
        DatabaseDescriptor.setCompactionThroughputBytesPerSec(500);
        ToolResult tool = invokeNodetool("getstreamthroughput");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("Use the -d flag to quiet this error and get the exact throughput in megabits/s");
    }

    private static void assertSetInvalidThroughputMib(String throughput)
    {
        ToolResult tool = invokeNodetool("setcompactionthroughput", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("compaction_throughput: 2147483647 is too large; it should be less than" +
                                              " 2147483647 in MiB/s");
    }

    private static void assertPreciseMibFlagNeeded()
    {
        DatabaseDescriptor.setCompactionThroughputBytesPerSec(15);
        ToolResult tool = invokeNodetool("getcompactionthroughput");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("Use the -d flag to quiet this error and get the exact throughput in MiB/s");
    }

    private static void assertGetThroughput(int expected)
    {
        ToolResult tool = invokeNodetool("getcompactionthroughput");
        tool.assertOnCleanExit();

        if (expected > 0)
            assertThat(tool.getStdout()).contains("Current compaction throughput: " + expected + " MB/s");
        else
            assertThat(tool.getStdout()).contains("Current compaction throughput: 0 MB/s");
    }

    private static void assertGetThroughputDouble(double expected)
    {
        ToolResult tool = invokeNodetool("getcompactionthroughput", "-d");
        tool.assertOnCleanExit();

        if (expected > 0)
            assertThat(tool.getStdout()).contains("Current compaction throughput: " + expected + " MiB/s");
        else
            assertThat(tool.getStdout()).contains("Current compaction throughput: 0.0 MiB/s");
    }
}
