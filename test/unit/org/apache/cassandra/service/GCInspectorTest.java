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
package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class GCInspectorTest
{
    
    GCInspector gcInspector;
    
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
    @Before
    public void before()
    {
        gcInspector = new GCInspector();
    }
    
    @Test
    public void ensureStaticFieldsHydrateFromConfig()
    {    
        assertEquals(DatabaseDescriptor.getGCLogThreshold(), gcInspector.getGcLogThresholdInMs());
        assertEquals(DatabaseDescriptor.getGCWarnThreshold(), gcInspector.getGcWarnThresholdInMs());
        assertEquals(DatabaseDescriptor.getGCConcurrentPhaseLogThreshold(), gcInspector.getGcConcurrentPhaseLogThresholdInMs());
        assertEquals(DatabaseDescriptor.getGCConcurrentPhaseWarnThreshold(), gcInspector.getGcConcurrentPhaseWarnThresholdInMs());
    }
    
    @Test
    public void ensureStatusIsCalculated()
    {
        assertTrue(gcInspector.getStatusThresholdInMs() > 0);
    }
    
    @Test
    public void ensureWarnGreaterThanLog()
    {
        try
        {
            gcInspector.setGcWarnThresholdInMs(gcInspector.getGcLogThresholdInMs());
            fail("Expect IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
        try
        {
            gcInspector.setGcConcurrentPhaseWarnThresholdInMs(gcInspector.getGcConcurrentPhaseLogThresholdInMs());
            fail("Expect IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }
    
    @Test
    public void ensureZeroIsOk()
    {
        gcInspector.setGcWarnThresholdInMs(0);
        assertEquals(gcInspector.getStatusThresholdInMs(), gcInspector.getGcLogThresholdInMs());
        assertEquals(0, DatabaseDescriptor.getGCWarnThreshold());
        assertEquals(200, DatabaseDescriptor.getGCLogThreshold());

        gcInspector.setGcConcurrentPhaseWarnThresholdInMs(0);
        assertEquals(0, DatabaseDescriptor.getGCConcurrentPhaseWarnThreshold());
        assertEquals(1000, DatabaseDescriptor.getGCConcurrentPhaseLogThreshold());
    }
    
    @Test
    public void ensureLogLessThanWarn()
    {
        assertEquals(200, gcInspector.getGcLogThresholdInMs());
        gcInspector.setGcWarnThresholdInMs(1000);
        assertEquals(1000, gcInspector.getGcWarnThresholdInMs());
        try
        {
            gcInspector.setGcLogThresholdInMs(gcInspector.getGcWarnThresholdInMs() + 1);
            fail("Expect IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
        assertEquals(1000, gcInspector.getGcConcurrentPhaseLogThresholdInMs());
        gcInspector.setGcConcurrentPhaseWarnThresholdInMs(2000);
        assertEquals(2000, gcInspector.getGcConcurrentPhaseWarnThresholdInMs());
        try
        {
            gcInspector.setGcConcurrentPhaseLogThresholdInMs(gcInspector.getGcConcurrentPhaseWarnThresholdInMs() + 1);
            fail("Expect IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }
    
    @Test
    public void testDefaults()
    {
        gcInspector.setGcLogThresholdInMs(200);
        gcInspector.setGcWarnThresholdInMs(1000);
        gcInspector.setGcConcurrentPhaseLogThresholdInMs(1000);
        gcInspector.setGcConcurrentPhaseWarnThresholdInMs(2000);
        assertEquals(200, DatabaseDescriptor.getGCLogThreshold());
        assertEquals(200, gcInspector.getGcLogThresholdInMs());
        assertEquals(1000, DatabaseDescriptor.getGCWarnThreshold());
        assertEquals(1000, gcInspector.getGcWarnThresholdInMs());
        assertEquals(1000, DatabaseDescriptor.getGCConcurrentPhaseLogThreshold());
        assertEquals(1000, gcInspector.getGcConcurrentPhaseLogThresholdInMs());
        assertEquals(2000, DatabaseDescriptor.getGCConcurrentPhaseWarnThreshold());
        assertEquals(2000, gcInspector.getGcConcurrentPhaseWarnThresholdInMs());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMaxValue()
    {
        gcInspector.setGcLogThresholdInMs(200);
        gcInspector.setGcWarnThresholdInMs(Integer.MAX_VALUE+1L);
    }

    @Test
    public void testIsConcurrentPhase()
    {
        assertTrue("No GC cause should be considered concurrent",
                         GCInspector.isConcurrentPhase("No GC", "SomeGCName"));
        assertTrue("Shenandoah Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "Shenandoah Cycles"));
        assertTrue("ZGC Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "ZGC Cycles"));

        assertFalse("ParallelGC should not be considered concurrent",
                          GCInspector.isConcurrentPhase("Allocation Failure", "PS Scavenge"));
        assertFalse("G1 Young Generation should not be considered concurrent",
                          GCInspector.isConcurrentPhase("G1 Evacuation Pause", "G1 Young Generation"));
    }
}
