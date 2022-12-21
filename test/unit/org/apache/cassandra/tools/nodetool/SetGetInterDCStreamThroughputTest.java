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

import org.apache.cassandra.config.DataRateSpec;
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
    private static final int MAX_INT_CONFIG_VALUE_IN_MBIT = Integer.MAX_VALUE - 1;
    private static final double BYTES_PER_MEGABIT = 125_000;
    private static final int MAX_INT_CONFIG_VALUE_MIB = (int) (MAX_INT_CONFIG_VALUE_IN_MBIT * BYTES_PER_MEGABIT) / 1024 / 1024;
    private static final double INTEGER_MAX_VALUE_MEGABITS_IN_BYTES = DataRateSpec.LongBytesPerSecondBound
                                                                      .megabitsPerSecondInBytesPerSecond(MAX_INT_CONFIG_VALUE_IN_MBIT)
                                                                      .toBytesPerSecond();
    private static final double MEBIBYTES_PER_MEGABIT = 0.11920928955078125;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
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
        assertSetGetValidThroughput(7, 7 * BYTES_PER_MEGABIT);
        assertSetGetValidThroughputMiB(7, 7 * StreamRateLimiter.BYTES_PER_MEBIBYTE);
        assertSetMbitGetMibValidThroughput(7, 7 * BYTES_PER_MEGABIT);
    }

    @Test
    public void testSmallPositive()
    {
        // As part of CASSANDRA-15234 we had to do some tweaks with precision. This test has to ensure no regressions
        // happen, hopefully. Internally data rate parameters values and rate limitter are set in double. Users can set
        // and get only integers
        assertSetGetValidThroughput(1, 1 * BYTES_PER_MEGABIT);
        assertSetGetValidThroughputMiB(1, 1 * StreamRateLimiter.BYTES_PER_MEBIBYTE);
        assertSetMbitGetMibValidThroughput(1, 1 * BYTES_PER_MEGABIT);
    }

    @Test
    public void testMaxValue()
    {
        assertSetGetValidThroughput(MAX_INT_CONFIG_VALUE_IN_MBIT, INTEGER_MAX_VALUE_MEGABITS_IN_BYTES);
        assertSetGetValidThroughputMiB(MAX_INT_CONFIG_VALUE_MIB, MAX_INT_CONFIG_VALUE_MIB * StreamRateLimiter.BYTES_PER_MEBIBYTE);
        assertSetMbitGetMibValidThroughput(MAX_INT_CONFIG_VALUE_IN_MBIT, INTEGER_MAX_VALUE_MEGABITS_IN_BYTES);
    }

    @Test
    public void testUpperBound()
    {
        assertSetInvalidThroughputMib(String.valueOf(Integer.MAX_VALUE));
        assertSetInvalidThroughputMbit(String.valueOf(Integer.MAX_VALUE));
    }

    @Test
    public void testZero()
    {
        assertSetGetValidThroughput(0, Double.MAX_VALUE);
        assertSetGetValidThroughputMiB(0, Double.MAX_VALUE);
        assertSetMbitGetMibValidThroughput(0, Double.MAX_VALUE);
    }

    @Test
    public void testUnparseable()
    {
        assertSetInvalidThroughput("1.2", "inter_dc_stream_throughput: can not convert \"1.2\" to a int");
        assertSetInvalidThroughput("value", "inter_dc_stream_throughput: can not convert \"value\" to a int");
        assertSetGetMoreFlagsIsInvalid();
        assertDFlagNeeded();
    }

    private static void assertSetGetValidThroughput(int throughput, double rateInBytes)
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", String.valueOf(throughput));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughput(throughput);

        assertThat(StreamRateLimiter.getInterDCRateLimiterRateInBytes()).isEqualTo(rateInBytes, withPrecision(0.04));
    }

    private static void assertDFlagNeeded()
    {
        ToolResult tool = invokeNodetool("setstreamthroughput", "-m", String.valueOf(1));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        tool = invokeNodetool("getstreamthroughput");
        assertThat(tool.getExitCode()).isEqualTo(2);
        assertThat(tool.getStderr()).contains("Use the -d flag to quiet this error and get the exact throughput in megabits/s");
    }

    private static void assertSetGetValidThroughputMiB(int throughput, double rateInBytes)
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", "-m", String.valueOf(throughput));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughputMiB(throughput);

        assertThat(StreamRateLimiter.getInterDCRateLimiterRateInBytes()).isEqualTo(rateInBytes, withPrecision(0.01));
    }

    private static void assertSetMbitGetMibValidThroughput(int throughput, double rateInBytes)
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", String.valueOf(throughput));
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEmpty();

        assertGetThroughputMiB(throughput * MEBIBYTES_PER_MEGABIT);

        assertThat(StreamRateLimiter.getInterDCRateLimiterRateInBytes()).isEqualTo(rateInBytes, withPrecision(0.01));
    }

    private static void assertSetInvalidThroughput(String throughput, String expectedErrorMessage)
    {
        ToolResult tool = throughput == null ? invokeNodetool("setinterdcstreamthroughput")
                                             : invokeNodetool("setinterdcstreamthroughput", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains(expectedErrorMessage);
    }

    private static void assertSetInvalidThroughputMib(String throughput)
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", "-m", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("inter_dc_stream_throughput_outbound: 2147483647 is too large; it should be" +
                                              " less than 2147483647 in megabits/s");
    }

    private static void assertSetInvalidThroughputMbit(String throughput)
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", throughput);
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("Invalid data rate: 2147483647 megabits per second; stream_throughput_outbound" +
                                              " and inter_dc_stream_throughput_outbound should be between 0 and 2147483646 in " +
                                              "megabits per second");
    }

    private static void assertSetGetMoreFlagsIsInvalid()
    {
        ToolResult tool = invokeNodetool("setinterdcstreamthroughput", "-m", "5", "-e", "5");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("You cannot use -e and -m at the same time");

        tool = invokeNodetool("getinterdcstreamthroughput", "-m", "-e");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("You cannot use more than one flag with this command");

        tool = invokeNodetool("getinterdcstreamthroughput", "-m", "-d");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("You cannot use more than one flag with this command");

        tool = invokeNodetool("getinterdcstreamthroughput", "-d", "-e");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("You cannot use more than one flag with this command");

        tool = invokeNodetool("getinterdcstreamthroughput", "-m", "-e", "-d");
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("You cannot use more than one flag with this command");
    }

    private static void assertGetThroughput(int expected)
    {
        ToolResult tool = invokeNodetool("getinterdcstreamthroughput");
        tool.assertOnCleanExit();

        if (expected > 0)
            assertThat(tool.getStdout()).contains("Current inter-datacenter stream throughput: " + expected + " Mb/s");
        else
            assertThat(tool.getStdout()).contains("Current inter-datacenter stream throughput: unlimited");
    }

    private static void assertGetThroughputMiB(double expected)
    {
        ToolResult tool = invokeNodetool("getinterdcstreamthroughput", "-m");
        tool.assertOnCleanExit();

        if (expected > 0)
            assertThat(tool.getStdout()).contains("Current inter-datacenter stream throughput: " + expected + " MiB/s");
        else
            assertThat(tool.getStdout()).contains("Current inter-datacenter stream throughput: unlimited");
    }
}
