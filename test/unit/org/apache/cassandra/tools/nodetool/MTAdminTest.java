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
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.cassandra.tools.NodeProbe;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MTAdminTest
{
    @Mock
    private NodeProbe probe;

    @Mock
    private PrintStream out;

    private MTAdmin.SetConfig cmd;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        when(probe.isMutationTrackingDisabled()).thenReturn(false);
        cmd = new MTAdmin.SetConfig();
        cmd.out = out;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoArgs()
    {
        cmd.execute(probe);
    }

    @Test
    public void testMutationTrackingDisabled()
    {
        when(probe.isMutationTrackingDisabled()).thenReturn(true);
        cmd.args = List.of("background_reconciliation_enabled", "true");

        cmd.execute(probe);

        verify(out, times(1)).println("Mutation tracking is not enabled");
        verify(probe, times(0)).setMutationTrackingBackgroundReconciliationEnabled(true);
    }

    @Test
    public void testSetBackgroundReconciliationEnabled()
    {
        cmd.args = List.of("background_reconciliation_enabled", "true");

        cmd.execute(probe);

        verify(probe, times(1)).setMutationTrackingBackgroundReconciliationEnabled(true);
    }

    @Test
    public void testSetBackgroundReconciliationDisabled()
    {
        cmd.args = List.of("background_reconciliation_enabled", "false");

        cmd.execute(probe);

        verify(probe, times(1)).setMutationTrackingBackgroundReconciliationEnabled(false);
    }

    @Test
    public void testSetBackgroundReconciliationIntervalMs()
    {
        cmd.args = List.of("background_reconciliation_interval_ms", "5000");

        cmd.execute(probe);

        verify(probe, times(1)).setMutationTrackingBackgroundReconciliationIntervalMilliseconds(5000L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParamType()
    {
        cmd.args = List.of("unknown_param", "value");

        cmd.execute(probe);
    }

    @Test(expected = NumberFormatException.class)
    public void testInvalidIntervalValue()
    {
        cmd.args = List.of("background_reconciliation_interval_ms", "not_a_number");

        cmd.execute(probe);
    }
}
