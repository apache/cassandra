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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class AutoRepairStatusTest
{
    @Mock
    private static NodeProbe probe;

    private ByteArrayOutputStream cmdOutput;

    @Mock
    private static AutoRepairConfig config;

    private static AutoRepairStatus cmd;

    @Parameterized.Parameter()
    public RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<RepairType> repairTypes()
    {
        return Arrays.asList(RepairType.values());
    }

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        cmdOutput = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(cmdOutput);
        when(probe.output()).thenReturn(new Output(out, out));
        when(probe.getAutoRepairConfig()).thenReturn(config);
        cmd = new AutoRepairStatus();
        cmd.v2 = true;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExecuteWithoutRepairType()
    {
        cmd.repairType = null;
        cmd.execute(probe);
    }

    @Test
    public void testExecuteWithNoNodes()
    {
        cmd.repairType = repairType;

        cmd.execute(probe);

        assertEquals("Data center group Active repairs Acitve force repairs\n" +
                     "ALL NODES         EMPTY          EMPTY               \n" +
                     "Total             EMPTY          EMPTY               \n", cmdOutput.toString());
    }

    @Test
    public void testExecuteWithNoGroups()
    {
        when(config.getDCGroups(repairType)).thenReturn(ImmutableSet.of());
        when(probe.getOnGoingRepairHostIdsByGroupHash(eq(repairType), anyInt())).thenReturn(ImmutableSet.of("host1", "host2"));
        when(probe.getOnGoingForceRepairHostIdsByGroupHash(eq(repairType), anyInt())).thenReturn(ImmutableSet.of("host3", "host4"));
        cmd.repairType = repairType;

        cmd.execute(probe);

        assertEquals("Data center group Active repairs Acitve force repairs\n" +
                     "ALL NODES         host1,host2    host3,host4         \n" +
                     "Total             host1,host2    host4,host3         \n", cmdOutput.toString());
    }

    @Test
    public void testExecuteWithGroups()
    {
        when(config.getDCGroups(repairType)).thenReturn(ImmutableSet.of("group1", "group2"));
        when(probe.getOnGoingRepairHostIdsByGroupHash(repairType, "group1".hashCode())).thenReturn(ImmutableSet.of("host1", "host2"));
        when(probe.getOnGoingForceRepairHostIdsByGroupHash(repairType, "group1".hashCode())).thenReturn(ImmutableSet.of("host3", "host4"));
        when(probe.getOnGoingRepairHostIdsByGroupHash(repairType, "group2".hashCode())).thenReturn(ImmutableSet.of("host5", "host6"));
        when(probe.getOnGoingForceRepairHostIdsByGroupHash(repairType, "group2".hashCode())).thenReturn(ImmutableSet.of("host7", "host8"));
        cmd.repairType = repairType;

        cmd.execute(probe);

        assertEquals("Data center group Active repairs          Acitve force repairs   \n" +
                     "[group2]          host5,host6             host7,host8            \n" +
                     "[group1]          host1,host2             host3,host4            \n" +
                     "Total             host5,host6,host1,host2 host4,host7,host3,host8\n", cmdOutput.toString());
    }
}
