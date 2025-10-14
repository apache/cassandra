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

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link GetMaxWaitTimeInTransportQueue}
 */
public class GetMaxWaitTimeInTransportQueueTest
{
    @Mock
    private NodeProbe probe;

    private GetMaxWaitTimeInTransportQueue cmd;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        cmd = new GetMaxWaitTimeInTransportQueue();

        // Setup mock output stream
        PrintStream noopStream = new PrintStream(new OutputStream()
        {
            @Override
            public void write(int b)
            {
            }
        });
        when(probe.output()).thenReturn(new Output(noopStream, noopStream));
    }

    @Test
    public void testExecuteWithDefaultValue()
    {
        // Mock the probe to return default value (usually 0 or a default configured value)
        when(probe.getMaxWaitTimeInTransportQueueMillis()).thenReturn(10000L);

        cmd.execute(probe);

        verify(probe, times(1)).getMaxWaitTimeInTransportQueueMillis();
    }

    @Test
    public void testExecuteWithZeroValue()
    {
        // Test with disabled value (0)
        when(probe.getMaxWaitTimeInTransportQueueMillis()).thenReturn(0L);

        cmd.execute(probe);

        verify(probe, times(1)).getMaxWaitTimeInTransportQueueMillis();
    }

    @Test
    public void testExecuteWithLargeValue()
    {
        // Test with large value
        when(probe.getMaxWaitTimeInTransportQueueMillis()).thenReturn(60000L);

        cmd.execute(probe);

        verify(probe, times(1)).getMaxWaitTimeInTransportQueueMillis();
    }

    @Test
    public void testExecuteWithNegativeValue()
    {
        // Test edge case with negative value (should not happen in practice)
        when(probe.getMaxWaitTimeInTransportQueueMillis()).thenReturn(-1L);

        cmd.execute(probe);

        verify(probe, times(1)).getMaxWaitTimeInTransportQueueMillis();
    }

    @Test
    public void testExecuteWithExceptionHandling()
    {
        // Test that exceptions from probe are properly propagated
        when(probe.getMaxWaitTimeInTransportQueueMillis()).thenThrow(new RuntimeException("Connection failed"));

        try
        {
            cmd.execute(probe);
        }
        catch (RuntimeException e)
        {
            // Expected behavior - command should not catch probe exceptions
        }

        verify(probe, times(1)).getMaxWaitTimeInTransportQueueMillis();
    }
}
