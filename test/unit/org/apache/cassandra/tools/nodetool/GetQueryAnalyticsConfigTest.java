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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.cassandra.tools.NodeProbe;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link org.apache.cassandra.tools.nodetool.GetQueryAnalyticsConfig}
 */
public class GetQueryAnalyticsConfigTest
{
    @Mock
    private NodeProbe probe;
    
    @Mock 
    private PrintStream mockOut;
    
    private GetQueryAnalyticsConfig cmd;
    
    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        cmd = new GetQueryAnalyticsConfig();
        cmd.out = mockOut;
    }
    
    @Test
    public void testGetConfigWhenEnabled()
    {
        String mockConfiguration = "Query Analytics Configuration:\n" +
                                 "  enabled: true\n" +
                                 "  producer:\n" +
                                 "    class_name: TestProducer\n" +
                                 "    parameters:\n" +
                                 "      kafka_topic: test-topic";
        
        when(probe.isQueryAnalyticsEnabled()).thenReturn(true);
        when(probe.queryAnalyticsConfiguration()).thenReturn(mockConfiguration);
        
        cmd.execute(probe);
        
        verify(probe, times(1)).isQueryAnalyticsEnabled();
        verify(probe, times(1)).queryAnalyticsConfiguration();
        verify(mockOut, times(1)).println(mockConfiguration);
    }
    
    @Test
    public void testGetConfigWhenDisabled()
    {
        when(probe.isQueryAnalyticsEnabled()).thenReturn(false);
        
        cmd.execute(probe);
        
        verify(probe, times(1)).isQueryAnalyticsEnabled();
        verify(probe, never()).queryAnalyticsConfiguration();
        verify(mockOut, times(1)).println("Query Analytics is not enabled");
    }
    
    @Test
    public void testGetConfigWithNullConfiguration()
    {
        when(probe.isQueryAnalyticsEnabled()).thenReturn(true);
        when(probe.queryAnalyticsConfiguration()).thenReturn(null);
        
        cmd.execute(probe);
        
        verify(probe, times(1)).isQueryAnalyticsEnabled();
        verify(probe, times(1)).queryAnalyticsConfiguration();
        verify(mockOut, times(1)).println((String) null);
    }
    
    @Test
    public void testGetConfigWithEmptyConfiguration()
    {
        when(probe.isQueryAnalyticsEnabled()).thenReturn(true);
        when(probe.queryAnalyticsConfiguration()).thenReturn("");
        
        cmd.execute(probe);
        
        verify(probe, times(1)).isQueryAnalyticsEnabled();
        verify(probe, times(1)).queryAnalyticsConfiguration();
        verify(mockOut, times(1)).println("");
    }
    
    @Test
    public void testGetConfigMultipleLines()
    {
        String multiLineConfig = "Query Analytics Configuration:\n" +
                               "  enabled: true\n" +
                               "  producer: none";
        
        when(probe.isQueryAnalyticsEnabled()).thenReturn(true);
        when(probe.queryAnalyticsConfiguration()).thenReturn(multiLineConfig);
        
        cmd.execute(probe);
        
        verify(probe, times(1)).isQueryAnalyticsEnabled();
        verify(probe, times(1)).queryAnalyticsConfiguration();
        verify(mockOut, times(1)).println(multiLineConfig);
    }
}
