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

import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import static org.apache.cassandra.tools.ToolRunner.ToolResult;
import static org.apache.cassandra.tools.ToolRunner.invokeNodetool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;

/**
 * Tests for entire SSTable {@code nodetool setstreamthroughput} and {@code nodetool getstreamthroughput}.
 */
public class SetGetEntireSSTableStreamThroughputTest extends CQLTester
{
    private static final int MAX_INT_CONFIG_VALUE_MIB = Integer.MAX_VALUE - 1;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testNull()
    {
        assertSetInvalidThroughput(null, "Required parameters are missing: stream_throughput");
    }

    @Test
    public void testPositive()
    {
        assertSetGetValidThroughput(7, 7 * StreamRateLimiter.BYTES_PER_MEBIBYTE);
    }

    @Test
    public void testMaxValue()
    {
        assertSetGetValidThroughput(MAX_INT_CONFIG_VALUE_MIB, MAX_INT_CONFIG_VALUE_MIB * StreamRateLimiter.BYTES_PER_MEBIBYTE);
    }

    @Test
    public void testUpperBound()
    {
        assertSetInvalidEntireSStableThroughputMib(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void testZero()
    {
        assertSetGetValidThroughput(0, Double.MAX_VALUE);
    }

    @Test
    public void testUnparseable()
    {
        assertSetInvalidThroughput("1.2", "stream_throughput: can not convert \"1.2\" to a int");
        assertSetInvalidThroughput("value", "stream_throughput: can not convert \"value\" to a int");
    }

    private static void assertSetGetValidThroughput(int throughput)
    {
        ToolResult tool = invokeNodetool("setstreamthroughput", "-e", String.valueOf(throughput));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughput(throughput);
    }

    private static void assertSetGetValidThroughput(int throughput, double rateInBytes)
    {
        assertSetGetValidThroughput(throughput);

        assertThat(StreamRateLimiter.getEntireSSTableRateLimiterRateInBytes()).isEqualTo(rateInBytes, withPrecision(0.01));
    }

    private static void assertSetInvalidEntireSStableThroughputMib(String throughput)
    {
        ToolResult tool = invokeNodetool("setstreamthroughput", "-e", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("entire_sstable_stream_throughput_outbound: 2147483647 is too large; it " +
                                              "should be less than 2147483647 in MiB/s");
    }

    private static void assertSetInvalidThroughput(String throughput, String expectedErrorMessage)
    {
        ToolResult tool = throughput == null ? invokeNodetool("setstreamthroughput", "-e")
                                             : invokeNodetool("setstreamthroughput", "-e", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains(expectedErrorMessage);
    }

    private static void assertGetThroughput(double expected)
    {
        ToolResult tool = invokeNodetool("getstreamthroughput", "-e");
        tool.assertOnCleanExit();

        if (expected > 0)
            assertThat(tool.getStdout()).contains("Current entire SSTable stream throughput: " + expected + " MiB/s");
        else
            assertThat(tool.getStdout()).contains("Current entire SSTable stream throughput: unlimited");
    }
}
