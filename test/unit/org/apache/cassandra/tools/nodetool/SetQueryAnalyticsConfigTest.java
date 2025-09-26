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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.cassandra.tools.NodeProbe;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link org.apache.cassandra.tools.nodetool.SetQueryAnalyticsConfig}
 */
public class SetQueryAnalyticsConfigTest
{
    @Mock
    private NodeProbe probe;
    
    @Mock 
    private PrintStream mockOut;
    
    private SetQueryAnalyticsConfig cmd;
    
    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        cmd = new SetQueryAnalyticsConfig();
        cmd.out = mockOut;
    }
    
    @Test
    public void testSetEnabledTrue()
    {
        // Mock the configuration response for warning check
        when(probe.queryAnalyticsConfiguration()).thenReturn(
            "Query Analytics Configuration:\n" +
            "  enabled: true\n" +
            "  producer:\n" +
            "    class_name: TestProducer\n" +
            "    parameters:\n" +
            "      kafka_topic: test-topic"
        );
        
        cmd.args = Arrays.asList("enabled", "true");
        cmd.execute(probe);
        
        verify(probe, times(1)).setQueryAnalyticsEnabled(true);
        verify(mockOut, times(1)).println("Query Analytics enabled: true");
    }
    
    @Test
    public void testSetEnabledFalse()
    {
        cmd.args = Arrays.asList("enabled", "false");
        cmd.execute(probe);
        
        verify(probe, times(1)).setQueryAnalyticsEnabled(false);
        verify(mockOut, times(1)).println("Query Analytics enabled: false");
    }
    
    @Test
    public void testInvalidParameter()
    {
        cmd.args = Arrays.asList("invalid_param", "value");
        
        try
        {
            cmd.execute(probe);
            fail("Expected IllegalArgumentException for invalid parameter");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Error message should mention unknown parameter", 
                      e.getMessage().contains("Unknown parameter: invalid_param"));
            assertTrue("Error message should list valid parameters", 
                      e.getMessage().contains("enabled"));
        }
    }
    
    @Test
    public void testInsufficientArguments()
    {
        cmd.args = Arrays.asList("enabled"); // Missing value
        
        try
        {
            cmd.execute(probe);
            fail("Expected IllegalArgumentException for insufficient arguments");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Error should mention required arguments", 
                      e.getMessage().contains("requires param and value args"));
        }
    }
    
    @Test
    public void testTooManyArguments()
    {
        cmd.args = Arrays.asList("enabled", "true", "extra");
        
        try
        {
            cmd.execute(probe);
            fail("Expected IllegalArgumentException for too many arguments");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Error should mention required arguments", 
                      e.getMessage().contains("requires param and value args"));
        }
    }
    
    @Test
    public void testCaseSensitiveParameters()
    {
        // Test that parameters are case sensitive
        cmd.args = Arrays.asList("ENABLED", "true");
        
        try
        {
            cmd.execute(probe);
            fail("Expected IllegalArgumentException for uppercase parameter");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue("Error should mention unknown parameter", 
                      e.getMessage().contains("Unknown parameter: ENABLED"));
        }
    }
    
    @Test
    public void testWarningWhenEnablingWithoutProducer()
    {
        // Mock configuration without producer
        when(probe.queryAnalyticsConfiguration()).thenReturn(
            "enabled: true\n" +
            "producer:\n" +
            "  class_name:"
        );
        
        cmd.args = Arrays.asList("enabled", "true");
        cmd.execute(probe);
        
        verify(probe, times(1)).setQueryAnalyticsEnabled(true);
        verify(mockOut, times(1)).println("Query Analytics enabled: true");
        verify(mockOut, times(1)).println("WARNING: QueryAnalytics is enabled but no producer is configured. Metrics will not be sent.");
    }
    
    @Test
    public void testNoWarningWhenEnablingWithProducer()
    {
        // Mock configuration with producer
        when(probe.queryAnalyticsConfiguration()).thenReturn(
            "enabled: true\n" +
            "producer:\n" +
            "  class_name: com.uber.cassandra.analytics.QueryAnalyticsProducerImp"
        );
        
        cmd.args = Arrays.asList("enabled", "true");
        cmd.execute(probe);
        
        verify(probe, times(1)).setQueryAnalyticsEnabled(true);
        verify(mockOut, times(1)).println("Query Analytics enabled: true");
        verify(mockOut, never()).println(contains("WARNING"));
    }
    
    @Test
    public void testNoWarningWhenDisabling()
    {
        // No configuration check needed when disabling
        cmd.args = Arrays.asList("enabled", "false");
        cmd.execute(probe);
        
        verify(probe, times(1)).setQueryAnalyticsEnabled(false);
        verify(mockOut, times(1)).println("Query Analytics enabled: false");
        verify(mockOut, never()).println(contains("WARNING"));
        verify(probe, never()).queryAnalyticsConfiguration(); // Should not check config when disabling
    }
}
