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


import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.tools.NodeProbe;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public class RunAutoRepairOnceTest
{
    @Mock
    private static NodeProbe probe;

    @Parameterized.Parameter()
    public AutoRepairConfig.RepairType repairType;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<AutoRepairConfig.RepairType> repairTypes()
    {
        return Arrays.asList(AutoRepairConfig.RepairType.values());
    }

    private static RunAutoRepairOnce cmd;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        cmd = new RunAutoRepairOnce();
    }

    @Test
    public void testExecuteWithRepairType()
    {
        cmd.repairType = repairType;
        cmd.millisToWait = (long) 1;

        cmd.execute(probe);

        verify(probe, times(1)).runAutoRepairOnce(repairType, 1);
    }
}
