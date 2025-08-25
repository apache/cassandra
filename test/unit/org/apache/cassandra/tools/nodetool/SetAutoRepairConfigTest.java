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

/**
 * Unit tests for {@link org.apache.cassandra.tools.nodetool.SetAutoRepairConfig}
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ SetAutoRepairConfigTest.NoParamTests.class, SetAutoRepairConfigTest.RepairTypeParamTests.class,
                      SetAutoRepairConfigTest.RepairTypeAndArgsParamsTests.class })
public class SetAutoRepairConfigTest
{
    protected static SetAutoRepairConfig cmd;

    public static void before(NodeProbe probeMock, PrintStream outMock)
    {
        when(probeMock.isAutoRepairDisabled()).thenReturn(false);
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
            when(probe.isAutoRepairDisabled()).thenReturn(true);

            cmd.execute(probe);

            // test new calls are not made when auto repair is disabled
            verify(probe, times(1)).setAutoRepairHistoryClearDeleteHostsBufferDuration("1s");
        }

        @Test
        public void testStartScheduler()
        {
            cmd.args = ImmutableList.of("start_scheduler", "false");

            cmd.execute(probe);

            verify(probe, times(0)).startAutoRepairScheduler();

            cmd.args = ImmutableList.of("start_scheduler", "true");

            cmd.execute(probe);

            verify(probe, times(1)).startAutoRepairScheduler();
        }

        @Test
        public void testMinRepairDuration()
        {
            cmd.args = ImmutableList.of("min_repair_task_duration", "4s");

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairMinRepairTaskDuration("4s");
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
            cmd.repairTypeStr = repairType.name();
            cmd.execute(probe);
        }

        @Test
        public void testRepairSchedulingDisabled()
        {
            when(probe.isAutoRepairDisabled()).thenReturn(true);
            cmd.repairTypeStr = repairType.name();
            cmd.args = ImmutableList.of("threads", "1");

            cmd.execute(probe);

            verify(out, times(1)).println("Auto-repair is not enabled");
            verify(probe, times(0)).setAutoRepairThreads(repairType.name(), 1);
        }

        @Test
        public void testRepairTypeDisabled()
        {
            cmd.repairTypeStr = repairType.name();
            cmd.args = ImmutableList.of("number_of_repair_threads", "1");

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairThreads(repairType.name(), 1);
        }

        @Test
        public void testV2FlagMissing()
        {
            cmd.repairTypeStr = repairType.name();
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

            verify(probe, times(0)).setAutoRepairThreads(repairType.name(), 0);
        }

        @Test(expected = IllegalArgumentException.class)
        public void testInvalidParamType()
        {
            cmd.repairTypeStr = repairType.name();
            cmd.args = ImmutableList.of("unknown_type", "1");

            cmd.execute(probe);
        }

        @Test
        public void testPriorityHosts()
        {
            String commaSeparatedHostSet = String.join(",", localEndpoint.toString().substring(1), otherEndpoint.toString().substring(1));
            cmd.repairTypeStr = repairType.name();
            cmd.args = ImmutableList.of("priority_hosts", commaSeparatedHostSet);

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairPriorityForHosts(repairType.name(), commaSeparatedHostSet);
        }

        @Test
        public void testForceRepairHosts()
        {
            String commaSeparatedHostSet = String.join(",", localEndpoint.toString().substring(1), otherEndpoint.toString().substring(1));
            cmd.repairTypeStr = repairType.name();
            cmd.args = ImmutableList.of("forcerepair_hosts", commaSeparatedHostSet);

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairForceRepairForHosts(repairType.name(), commaSeparatedHostSet);
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
            forEachRepairType("enabled", "true", (type) -> verify(probe, times(1)).setAutoRepairEnabled(type.name(), true)),
            forEachRepairType("number_of_repair_threads", "1", (type) -> verify(probe, times(1)).setAutoRepairThreads(type.name(), 1)),
            forEachRepairType("min_repair_interval", "3h", (type) -> verify(probe, times(1)).setAutoRepairMinInterval(type.name(), "3h")),
            forEachRepairType("sstable_upper_threshold", "4", (type) -> verify(probe, times(1)).setAutoRepairSSTableCountHigherThreshold(type.name(), 4)),
            forEachRepairType("table_max_repair_time", "5s", (type) -> verify(probe, times(1)).setAutoRepairTableMaxRepairTime(type.name(), "5s")),
            forEachRepairType("repair_primary_token_range_only", "true", (type) -> verify(probe, times(1)).setAutoRepairPrimaryTokenRangeOnly(type.name(), true)),
            forEachRepairType("parallel_repair_count", "6", (type) -> verify(probe, times(1)).setAutoRepairParallelRepairCount(type.name(), 6)),
            forEachRepairType("parallel_repair_percentage", "7", (type) -> verify(probe, times(1)).setAutoRepairParallelRepairPercentage(type.name(), 7)),
            forEachRepairType("materialized_view_repair_enabled", "true", (type) -> verify(probe, times(1)).setAutoRepairMaterializedViewRepairEnabled(type.name(), true)),
            forEachRepairType("ignore_dcs", "dc1,dc2", (type) -> verify(probe, times(1)).setAutoRepairIgnoreDCs(type.name(), ImmutableSet.of("dc1", "dc2"))),
            forEachRepairType("token_range_splitter.max_bytes_per_schedule", "500GiB", (type) -> verify(probe, times(1)).setAutoRepairTokenRangeSplitterParameter(type.name(), "max_bytes_per_schedule", "500GiB")),
            forEachRepairType("repair_max_retries", "3", (type) -> verify(probe, times(1)).setAutoRepairMaxRetriesCount(type.name(), 3)),
            forEachRepairType("repair_retry_backoff", "60s", (type) -> verify(probe, times(1)).setAutoRepairRetryBackoff(type.name(), "60s"))
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
            cmd.repairTypeStr = repairType.name();
            cmd.args = ImmutableList.of(paramType, paramVal);

            cmd.execute(probe);

            verifyFunc.accept(repairType);

            // test scenario when auto repair is disabled
            when(probe.isAutoRepairDisabled()).thenReturn(true);

            cmd.execute(probe);

            // test new calls are not made when auto repair is disabled
            verifyFunc.accept(repairType);
        }
    }
}
