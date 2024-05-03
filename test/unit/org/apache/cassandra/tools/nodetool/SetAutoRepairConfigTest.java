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
import org.apache.commons.math3.util.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
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
            cmd.v2 = true;
        }

        @Test
        public void testHistoryDeleteHostsClearBufferInSec()
        {
            cmd.v2 = true;
            cmd.args = ImmutableList.of("historydeletehostsclearbufferinsec", "1");

            cmd.execute(probe);

            verify(probe, times(1)).setAutoRepairHistoryClearDeleteHostsBufferInSecV2(1);

            // test scenario when auto repair is disabled
            when(probe.getAutoRepairConfig()).thenReturn(new AutoRepairConfig(false));

            cmd.execute(probe);

            // test new calls are not made when auto repair is disabled
            verify(probe, times(1)).setAutoRepairHistoryClearDeleteHostsBufferInSecV2(1);
        }

        @Test
        public void testLegacy()
        {
            cmd.v2 = false;
            cmd.args = ImmutableList.of("threads", "1");
            when(probe.isAutoRepairEnabled()).thenReturn(true);

            cmd.execute(probe);

            verify(probe, times(1)).setRepairThreads(1);
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
        public RepairType repairType;

        @Parameterized.Parameters(name = "repairType={0}")
        public static Object[] data()
        {
            return RepairType.values();
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
            cmd.v2 = true;
            cmd.args = ImmutableList.of("threads", "1");

            cmd.execute(probe);

            verify(out, times(1)).println("Auto-repair is not enabled");
            verify(probe, times(0)).setRepairThreads(repairType, 1);
        }

        @Test
        public void testRepairTypeDisabled()
        {
            config.setAutoRepairEnabled(repairType, false);
            cmd.v2 = true;
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("threads", "1");

            cmd.execute(probe);

            verify(probe, times(1)).setRepairThreads(repairType, 1);
        }


        @Test
        public void testV2FlagMissing()
        {
            cmd.v2 = false;
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
            cmd.v2 = true;
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("unknown_type", "1");

            cmd.execute(probe);
        }


        @Test
        public void testPriorityHosts()
        {
            when(probe.filterHostsInLocalGroup(repairType, ImmutableSet.of(localEndpoint, otherEndpoint))).thenReturn(ImmutableSet.of(otherEndpoint));
            cmd.v2 = true;
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("priorityhost", String.join(",", localEndpoint.toString().substring(1), otherEndpoint.toString().substring(1)));

            cmd.execute(probe);

            verify(probe, times(1)).filterHostsInLocalGroup(repairType, ImmutableSet.of(localEndpoint, otherEndpoint));
            verify(probe, times(1)).setRepairPriorityForHosts(repairType, ImmutableSet.of(otherEndpoint));
        }

        @Test
        public void testForceRepairHosts()
        {
            when(probe.filterHostsInLocalGroup(repairType, ImmutableSet.of(localEndpoint, otherEndpoint))).thenReturn(ImmutableSet.of(otherEndpoint));
            cmd.v2 = true;
            cmd.repairType = repairType;
            cmd.args = ImmutableList.of("forcerepairhosts", String.join(",", localEndpoint.toString().substring(1), otherEndpoint.toString().substring(1)));

            cmd.execute(probe);

            verify(probe, times(1)).filterHostsInLocalGroup(repairType, ImmutableSet.of(localEndpoint, otherEndpoint));
            verify(probe, times(1)).setForceRepairForHosts(repairType, ImmutableSet.of(otherEndpoint));
        }
    }

    @RunWith(Parameterized.class)
    public static class RepairTypeAndArgsParamsTests
    {
        @Parameterized.Parameter
        public RepairType repairType;

        @Parameterized.Parameter(1)
        public String paramType;

        @Parameterized.Parameter(2)
        public String paramVal;

        @Parameterized.Parameter(3)
        public Consumer<RepairType> verifyFunc;

        @Parameterized.Parameters(name = "repairType={0},paramType={1}")
        public static Collection<Object[]> testCases()
        {
            return Stream.of(
            forEachRepairType("enabled", "true", (type) -> verify(probe, times(1)).setAutoRepairEnabled(type, true)),
            forEachRepairType("threads", "1", (type) -> verify(probe, times(1)).setRepairThreads(type, 1)),
            forEachRepairType("subranges", "2", (type) -> verify(probe, times(1)).setRepairSubRangeNum(type, 2)),
            forEachRepairType("minrepairintervalinhours", "3", (type) -> verify(probe, times(1)).setRepairMinIntervalInHours(type, 3)),
            forEachRepairType("sstablehigherthreshold", "4", (type) -> verify(probe, times(1)).setRepairSSTableCountHigherThreshold(type, 4)),
            forEachRepairType("ignorekeyspacesregex", "ignoreregex", (type) -> verify(probe, times(1)).setRepairIgnoreKeyspaces(type, "ignoreregex")),
            forEachRepairType("repaironlykeyspacesregex", "onlyregex", (type) -> verify(probe, times(1)).setRepairOnlyKeyspaces(type, "onlyregex")),
            forEachRepairType("tablemaxrepairtimeinsec", "5", (type) -> verify(probe, times(1)).setAutoRepairTableMaxRepairTimeInSec(type, 5)),
            forEachRepairType("primarytokenrangeonly", "true", (type) -> verify(probe, times(1)).setPrimaryTokenRangeOnly(type, true)),
            forEachRepairType("parallelrepaircount", "6", (type) -> verify(probe, times(1)).setParallelRepairCountInGroup(type, 6)),
            forEachRepairType("parallelrepairpercentage", "7", (type) -> verify(probe, times(1)).setParallelRepairPercentageInGroup(type, 7)),
            forEachRepairType("mvrepairenabled", "true", (type) -> verify(probe, times(1)).setMVRepairEnabled(type, true)),
            forEachRepairType("mvrepairenabled", "true", (type) -> verify(probe, times(1)).setMVRepairEnabled(type, true)),
            forEachRepairType("ignoredcs", "dc1,dc2", (type) -> verify(probe, times(1)).setAutoRepairIgnoreDCs(type, ImmutableSet.of("dc1", "dc2")))
            ).flatMap(Function.identity()).collect(Collectors.toList());
        }

        private static Stream<Object[]> forEachRepairType(String paramType, String paramVal, Consumer<RepairType> verifyFunc)
        {
            Object[][] testCases = new Object[RepairType.values().length][4];
            for (RepairType repairType : RepairType.values())
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
            cmd.v2 = true;
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
