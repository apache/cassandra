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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.tools.NodeProbe;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Suite.class)
@Suite.SuiteClasses({ SetAutoRepairConfigTest.NoParamTests.class, SetAutoRepairConfigTest.RepairTypeParamTests.class,
                      SetAutoRepairConfigTest.RepairTypeAndArgsParamsTests.class })
public class SetAutoRepairConfigTest
{
    protected static AutoRepairConfig config;

    protected static SetAutoRepairConfig cmd;

    public static void before(NodeProbe probeMock, PrintStream outMock)
    {
        config = new AutoRepairConfig(true);
        when(probeMock.getAutoRepairConfig()).thenReturn(config);
        cmd = new SetAutoRepairConfig();
        cmd.out = outMock;
    }

    public static class NoParamTests
    {
        @Mock
        private static NodeProbe probe;

        @Mock
        private static PrintStream out;

        @Before
        public void setUp()
        {
            MockitoAnnotations.initMocks(this);
            before(probe, out);
        }

        @Test
        public void testHistoryDeleteHostsClearBufferInSec()
        {
            cmd.args = ImmutableList.of("history_clear_delete_hosts_buffer_interval", "1s");

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairHistoryClearDeleteHostsBufferDuration("1s");

            // test scenario when auto repair is disabled
            when(probe.getAutoRepairConfig()).thenReturn(new AutoRepairConfig(false));

            cmd.execute(probe);

            // test new calls are not made when auto repair is disabled
            verify(probe, times(1)).setAutoRepairHistoryClearDeleteHostsBufferDuration("1s");
        }

        @Test
        public void testRepairMaxRetries()
        {
            cmd.args = ImmutableList.of("repair_max_retries", "2");

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairMaxRetriesCount(2);
        }


        @Test
        public void testRetryBackoffInSec()
        {
            cmd.args = ImmutableList.of("repair_retry_backoff", "3s");

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairRetryBackoff("3s");
        }
    }

    @RunWith(Parameterized.class)
    public static class RepairTypeParamTests
    {
        @Mock
        private static NodeProbe probe;

        @Mock
        private static PrintStream out;

        @Parameterized.Parameter
        public AutoRepairConfig.RepairType repairType;

        @Parameterized.Parameters(name = "repairType={0}")
        public static Object[] data()
        {
            return AutoRepairConfig.RepairType.values();
        }

        private static InetAddressAndPort localEndpoint;
        private static InetAddressAndPort otherEndpoint;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            before(probe, out);
            localEndpoint = InetAddressAndPort.getByName("127.0.0.1:7000");
            otherEndpoint = localEndpoint.withPort(localEndpoint.getPort() + 1);
        }

        @Test(expected = IllegalArgumentException.class)
        public void testNoArgs()
        {
            cmd.repairType = repairType;
            cmd.execute(probe);
        }

        @Test
        public void testRepairSchedulingDisabled()
        {
            when(probe.getAutoRepairConfig()).thenReturn(new AutoRepairConfig(false));
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("threads", "1");

            cmd.execute(probe);

            verify(out, times(1)).println("Auto-repair is not enabled");
            verify(probe, times(0)).setRepairThreads(repairType, 1);
        }

        @Test
        public void testRepairTypeDisabled()
        {
            config.setAutoRepairEnabled(repairType, false);
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("number_of_repair_threads", "1");

            cmd.execute(probe);

            verify(probe, times(1)).setRepairThreads(repairType, 1);
        }


        @Test
        public void testV2FlagMissing()
        {
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("threads", "1");

            try
            {
                cmd.execute(probe);

                fail("expected IllegalArgumentException");
            }
            catch (IllegalArgumentException e)
            {
                // expected
            }

            verify(probe, times(0)).setRepairThreads(repairType, 0);
        }

        @Test(expected = IllegalArgumentException.class)
        public void testInvalidParamType()
        {
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("unknown_type", "1");

            cmd.execute(probe);
        }


        @Test
        public void testPriorityHosts()
        {
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("priority_hosts", String.join(",", localEndpoint.toString().substring(1), otherEndpoint.toString().substring(1)));

            cmd.execute(probe);

            verify(probe, times(1)).setRepairPriorityForHosts(repairType, ImmutableSet.of(localEndpoint, otherEndpoint));
        }

        @Test
        public void testForceRepairHosts()
        {
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("forcerepair_hosts", String.join(",", localEndpoint.toString().substring(1), otherEndpoint.toString().substring(1)));

            cmd.execute(probe);

            verify(probe, times(1)).setForceRepairForHosts(repairType, ImmutableSet.of(localEndpoint, otherEndpoint));
        }
    }

    @RunWith(Parameterized.class)
    public static class RepairTypeAndArgsParamsTests
    {
        @Parameterized.Parameter
        public AutoRepairConfig.RepairType repairType;

        @Parameterized.Parameter(1)
        public String paramType;

        @Parameterized.Parameter(2)
        public String paramVal;

        @Parameterized.Parameter(3)
        public Consumer<AutoRepairConfig.RepairType> verifyFunc;

        @Parameterized.Parameters(name = "repairType={0},paramType={1}")
        public static Collection<Object[]> testCases()
        {
            return Stream.of(
            forEachRepairType("enabled", "true", (type) -> verify(probe, times(1)).setAutoRepairEnabled(type, true)),
            forEachRepairType("number_of_repair_threads", "1", (type) -> verify(probe, times(1)).setRepairThreads(type, 1)),
            forEachRepairType("number_of_subranges", "2", (type) -> verify(probe, times(1)).setRepairSubRangeNum(type, 2)),
            forEachRepairType("min_repair_interval", "3h", (type) -> verify(probe, times(1)).setRepairMinInterval(type, "3h")),
            forEachRepairType("sstable_upper_threshold", "4", (type) -> verify(probe, times(1)).setRepairSSTableCountHigherThreshold(type, 4)),
            forEachRepairType("table_max_repair_time", "5s", (type) -> verify(probe, times(1)).setAutoRepairTableMaxRepairTime(type, "5s")),
            forEachRepairType("repair_primary_token_range_only", "true", (type) -> verify(probe, times(1)).setPrimaryTokenRangeOnly(type, true)),
            forEachRepairType("parallel_repair_count", "6", (type) -> verify(probe, times(1)).setParallelRepairCountInGroup(type, 6)),
            forEachRepairType("parallel_repair_percentage", "7", (type) -> verify(probe, times(1)).setParallelRepairPercentageInGroup(type, 7)),
            forEachRepairType("mv_repair_enabled", "true", (type) -> verify(probe, times(1)).setMVRepairEnabled(type, true)),
            forEachRepairType("ignore_dcs", "dc1,dc2", (type) -> verify(probe, times(1)).setAutoRepairIgnoreDCs(type, ImmutableSet.of("dc1", "dc2")))
            ).flatMap(Function.identity()).collect(Collectors.toList());
        }

        private static Stream<Object[]> forEachRepairType(String paramType, String paramVal, Consumer<AutoRepairConfig.RepairType> verifyFunc)
        {
            Object[][] testCases = new Object[AutoRepairConfig.RepairType.values().length][4];
            for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
            {
                testCases[repairType.ordinal()] = new Object[]{ repairType, paramType, paramVal, verifyFunc };
            }

            return Arrays.stream(testCases);
        }

        @Mock
        private static NodeProbe probe;

        @Mock
        private static PrintStream out;

        @Before
        public void setUp()
        {
            MockitoAnnotations.initMocks(this);
            before(probe, out);
        }

        @Test
        public void test()
        {
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of(paramType, paramVal);

            cmd.execute(probe);

            verifyFunc.accept(repairType);

            // test scenario when auto repair is disabled
            when(probe.getAutoRepairConfig()).thenReturn(new AutoRepairConfig(false));

            cmd.execute(probe);

            // test new calls are not made when auto repair is disabled
            verifyFunc.accept(repairType);
        }
    }
}
