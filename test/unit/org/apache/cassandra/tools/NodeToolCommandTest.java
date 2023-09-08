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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.repair.messages.RepairOption;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeToolCommandTest
{
    final NodeProbe nodeProbe = Mockito.mock(NodeProbe.class);
    final Output output = Output.CONSOLE;

    final NodeProbeFactory repairNodeFactory = new NodeProbeFactory()
    {
        @Override
        public NodeProbe create(String host, int port) throws IOException
        {
            return nodeProbe;
        }

        @Override
        public NodeProbe create(String host, int port, String username, String password) throws IOException
        {
            return nodeProbe;
        }
    };


    @Before
    public void beforeTest()
    {
        Mockito.reset(nodeProbe);
        when(nodeProbe.getKeyspaces()).thenReturn(Arrays.asList("ks"));
        when(nodeProbe.getNonSystemKeyspaces()).thenReturn(Arrays.asList("ks"));
        when(nodeProbe.output()).thenReturn(output);
    }

    private Map<String, String> testRepairCommand(int expectedExitCode, String ...args) throws IOException
    {
        int result = new NodeTool(repairNodeFactory, output).execute(ArrayUtils.addFirst(args, "repair"));
        Assert.assertEquals(result, expectedExitCode);
        ArgumentCaptor<Map<String, String>> optCaptor = ArgumentCaptor.forClass(Map.class);
        verify(nodeProbe).repairAsync(any(), any(), optCaptor.capture());
        return optCaptor.getValue();
    }

    @Test
    public void repairCommandTest() throws IOException
    {
        Map<String, String> options = testRepairCommand(0, "--paxos-only", "ks");
        Assert.assertEquals(options.get(RepairOption.PAXOS_ONLY_KEY), Boolean.toString(true));
        Assert.assertEquals(options.get(RepairOption.INCREMENTAL_KEY), Boolean.toString(false));
    }

}
