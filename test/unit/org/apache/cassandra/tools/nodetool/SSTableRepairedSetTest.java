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

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SSTableRepairedSetTest
{
    @Mock
    private NodeProbe probe;

    private SSTableRepairedSet cmd;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        PrintStream noopStream = new PrintStream(new OutputStream() {@Override public void write(int b) {}});
        when(probe.output()).thenReturn(new Output(noopStream, noopStream));
        cmd = new SSTableRepairedSet();
    }

    @Test
    public void testNoKeyspace() {
        when(probe.getNonLocalStrategyKeyspaces()).thenReturn(new ArrayList<>(Arrays.asList("ks1", "ks2")));
        when(probe.getKeyspaces()).thenReturn(new ArrayList<>(Arrays.asList("ks1", "ks2")));
        when(probe.getTablesForKeyspace("ks1")).thenReturn(new ArrayList<>(Arrays.asList("table1", "table2")));
        when(probe.getTablesForKeyspace("ks2")).thenReturn(new ArrayList<>(Arrays.asList("table3", "table4")));
        cmd.isRepaired = true;
        cmd.reallySet = true;

        cmd.execute(probe);

        verify(probe, times(1)).mutateSSTableRepairedState(true, false, "ks1", Arrays.asList("table1", "table2"));
        verify(probe, times(1)).mutateSSTableRepairedState(true, false, "ks2", Arrays.asList("table3", "table4"));
    }

    @Test
    public void testBothRepairedAndUnrepaired() {
        cmd.args = Arrays.asList("keyspace");
        cmd.isRepaired = true;
        cmd.isUnrepaired = true;
        cmd.execute(probe);
        verify(probe, never()).mutateSSTableRepairedState(anyBoolean(), anyBoolean(), anyString(), anyList());
    }

    @Test
    public void testNeitherRepairedNorUnrepaired() {
        cmd.args = Arrays.asList("keyspace");
        cmd.execute(probe);
        verify(probe, never()).mutateSSTableRepairedState(anyBoolean(), anyBoolean(), anyString(), anyList());
    }

    @Test
    public void testRepairedPreview() {
        cmd.args = Arrays.asList("keyspace");
        when(probe.getKeyspaces()).thenReturn(new ArrayList<>(Arrays.asList("keyspace")));
        cmd.isRepaired = true;
        cmd.execute(probe);
        verify(probe).mutateSSTableRepairedState(true, true, "keyspace", new ArrayList<>());
    }

    @Test
    public void testUnrepairedReallySet() {
        cmd.args = Arrays.asList("keyspace");
        when(probe.getKeyspaces()).thenReturn(new ArrayList<>(Arrays.asList("keyspace")));
        cmd.isUnrepaired = true;
        cmd.reallySet = true;
        cmd.execute(probe);
        verify(probe).mutateSSTableRepairedState(false, false, "keyspace", new ArrayList<>());
    }

    @Test
    public void testExecuteWithTableNames() {
        cmd.args = Arrays.asList("keyspace", "table1", "table2");
        when(probe.getKeyspaces()).thenReturn(new ArrayList<>(Arrays.asList("keyspace")));
        cmd.isRepaired = true;
        cmd.reallySet = true;
        cmd.execute(probe);
        verify(probe).mutateSSTableRepairedState(true, false, "keyspace", Arrays.asList("table1", "table2"));
    }
}
