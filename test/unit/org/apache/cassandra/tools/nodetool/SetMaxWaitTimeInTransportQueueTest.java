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
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.cassandra.tools.NodeProbe;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link SetMaxWaitTimeInTransportQueue}
 */
public class SetMaxWaitTimeInTransportQueueTest
{
    @Mock
    private NodeProbe probe;

    private SetMaxWaitTimeInTransportQueue cmd;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        cmd = new SetMaxWaitTimeInTransportQueue();
    }

    @Test
    public void testExecuteWithValidPositiveValue()
    {
        cmd.args = Arrays.asList("5000");

        cmd.execute(probe);

        verify(probe, times(1)).setMaxWaitTimeInTransportQueueMillis(5000L);
    }

    @Test
    public void testExecuteWithZeroValue()
    {
        cmd.args = Arrays.asList("0");

        cmd.execute(probe);

        verify(probe, times(1)).setMaxWaitTimeInTransportQueueMillis(0L);
    }

    @Test
    public void testExecuteWithLargeValue()
    {
        cmd.args = Arrays.asList("60000");

        cmd.execute(probe);

        verify(probe, times(1)).setMaxWaitTimeInTransportQueueMillis(60000L);
    }

    @Test
    public void testExecuteWithMaxLongValue()
    {
        String maxValue = String.valueOf(Long.MAX_VALUE);
        cmd.args = Arrays.asList(maxValue);

        cmd.execute(probe);

        verify(probe, times(1)).setMaxWaitTimeInTransportQueueMillis(Long.MAX_VALUE);
    }

    @Test
    public void testExecuteWithNoArguments()
    {
        cmd.args = Collections.emptyList();

        try
        {
            cmd.execute(probe);
            fail("Expected IllegalArgumentException for missing arguments");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Error message should mention required value",
                    e.getMessage().contains("setmaxwaittimeintransportqueue requires value"));
        }

        verifyNoInteractions(probe);
    }

    @Test
    public void testExecuteWithTooManyArguments()
    {
        cmd.args = Arrays.asList("5000", "10000");

        try
        {
            cmd.execute(probe);
            fail("Expected IllegalArgumentException for too many arguments");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Error message should mention required value",
                    e.getMessage().contains("setmaxwaittimeintransportqueue requires value"));
        }

        verifyNoInteractions(probe);
    }

    @Test
    public void testExecuteWithInvalidNumberFormat()
    {
        cmd.args = Arrays.asList("not_a_number");

        try
        {
            cmd.execute(probe);
            fail("Expected NumberFormatException for invalid number format");
        }
        catch (NumberFormatException e)
        {
            // Expected behavior
        }

        verifyNoInteractions(probe);
    }

    @Test
    public void testExecuteWithNegativeValue()
    {
        cmd.args = Arrays.asList("-1000");

        cmd.execute(probe);

        // Should accept negative values (implementation may handle validation)
        verify(probe, times(1)).setMaxWaitTimeInTransportQueueMillis(-1000L);
    }

    @Test
    public void testExecuteWithDecimalValue()
    {
        cmd.args = Arrays.asList("5000.5");

        try
        {
            cmd.execute(probe);
            fail("Expected NumberFormatException for decimal value");
        }
        catch (NumberFormatException e)
        {
            // Expected behavior - Long.parseLong doesn't accept decimals
        }

        verifyNoInteractions(probe);
    }

    @Test
    public void testExecuteWithEmptyStringValue()
    {
        cmd.args = Arrays.asList("");

        try
        {
            cmd.execute(probe);
            fail("Expected NumberFormatException for empty string");
        }
        catch (NumberFormatException e)
        {
            // Expected behavior
        }

        verifyNoInteractions(probe);
    }

    @Test
    public void testExecuteWithProbeException()
    {
        cmd.args = Arrays.asList("5000");
        doThrow(new RuntimeException("Connection failed")).when(probe).setMaxWaitTimeInTransportQueueMillis(anyLong());

        try
        {
            cmd.execute(probe);
            fail("Expected RuntimeException from probe");
        }
        catch (RuntimeException e)
        {
            assertEquals("Connection failed", e.getMessage());
        }

        verify(probe, times(1)).setMaxWaitTimeInTransportQueueMillis(5000L);
    }
}
