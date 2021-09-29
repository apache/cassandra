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
 * Tests for {@code nodetool setinterdcstreamthroughput} and {@code nodetool getinterdcstreamthroughput}.
 */
public class SetGetInterDCStreamThroughputTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
    }

    @Test
    public void testNull()
    {
        assertSetInvalidThroughput(null, "Required parameters are missing: inter_dc_stream_throughput");
    }

    @Test
    public void testPositive()
    {
        assertSetGetValidThroughput(7, 7 * StreamRateLimiter.BYTES_PER_MEGABIT);
    }

    @Test
    public void testMaxValue()
    {
        assertSetGetValidThroughput(Integer.MAX_VALUE, Integer.MAX_VALUE * StreamRateLimiter.BYTES_PER_MEGABIT);
    }

    @Test
    public void testZero()
    {
        assertSetGetValidThroughput(0, Double.MAX_VALUE);
    }

    @Test
    public void testNegative()
    {
        assertSetGetValidThroughput(-7, Double.MAX_VALUE);
    }

    @Test
    public void testUnparseable()
    {
        assertSetInvalidThroughput("1.2", "inter_dc_stream_throughput: can not convert \"1.2\" to a int");
        assertSetInvalidThroughput("value", "inter_dc_stream_throughput: can not convert \"value\" to a int");
    }

    private static void assertSetGetValidThroughput(int throughput, double rateInBytes)
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", String.valueOf(throughput));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughput(throughput);

        assertThat(StreamRateLimiter.getInterDCRateLimiterRateInBytes()).isEqualTo(rateInBytes, withPrecision(0.01));
    }

    private static void assertSetInvalidThroughput(String throughput, String expectedErrorMessage)
    {
        ToolResult tool = throughput == null ? invokeNodetool("setinterdcstreamthroughput")
                                             : invokeNodetool("setinterdcstreamthroughput", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains(expectedErrorMessage);
    }

    private static void assertGetThroughput(int expected)
    {
        ToolResult tool = invokeNodetool("getinterdcstreamthroughput");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Current inter-datacenter stream throughput: " + expected + " Mb/s");
    }
}
