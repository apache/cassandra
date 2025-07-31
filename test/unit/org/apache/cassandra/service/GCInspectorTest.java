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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
        Assert.assertEquals(DatabaseDescriptor.getGCLogThreshold(), gcInspector.getGcLogThresholdInMs());
        Assert.assertEquals(DatabaseDescriptor.getGCWarnThreshold(), gcInspector.getGcWarnThresholdInMs());
    }
    
    @Test
    public void ensureStatusIsCalculated()
    {
        Assert.assertTrue(gcInspector.getStatusThresholdInMs() > 0);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void ensureWarnGreaterThanLog()
    {
        gcInspector.setGcWarnThresholdInMs(gcInspector.getGcLogThresholdInMs());
    }
    
    @Test
    public void ensureZeroIsOk()
    {
        gcInspector.setGcWarnThresholdInMs(0);
        Assert.assertEquals(gcInspector.getStatusThresholdInMs(), gcInspector.getGcLogThresholdInMs());
        Assert.assertEquals(0, DatabaseDescriptor.getGCWarnThreshold());
        Assert.assertEquals(200, DatabaseDescriptor.getGCLogThreshold());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void ensureLogLessThanWarn()
    {
        Assert.assertEquals(200, gcInspector.getGcLogThresholdInMs());
        gcInspector.setGcWarnThresholdInMs(1000);
        Assert.assertEquals(1000, gcInspector.getGcWarnThresholdInMs());
        gcInspector.setGcLogThresholdInMs(gcInspector.getGcWarnThresholdInMs() + 1);
    }
    
    @Test
    public void testDefaults()
    {
        gcInspector.setGcLogThresholdInMs(200);
        gcInspector.setGcWarnThresholdInMs(1000);
        Assert.assertEquals(200, DatabaseDescriptor.getGCLogThreshold());
        Assert.assertEquals(200, gcInspector.getGcLogThresholdInMs());
        Assert.assertEquals(1000, DatabaseDescriptor.getGCWarnThreshold());
        Assert.assertEquals(1000, gcInspector.getGcWarnThresholdInMs());
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
        Assert.assertTrue("No GC cause should be considered concurrent",
                         GCInspector.isConcurrentPhase("No GC", "SomeGCName"));
        Assert.assertTrue("Shenandoah Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "Shenandoah Cycles"));
        Assert.assertTrue("ZGC Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "ZGC Cycles"));
        Assert.assertTrue("ZGC Generation Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "ZGC Generation Cycles"));
        Assert.assertTrue("ZGC Minor Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "ZGC Minor Cycles"));
        Assert.assertTrue("GPGC Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "GPGC Cycles"));
        Assert.assertTrue("GPGC Concurrent Cycles should be considered concurrent",
                         GCInspector.isConcurrentPhase("SomeCause", "GPGC Concurrent Cycles"));
        Assert.assertFalse("GPGC Pauses should not be considered concurrent",
                          GCInspector.isConcurrentPhase("SomeCause", "GPGC Pauses"));
        Assert.assertFalse("GPGC Minor Pauses should not be considered concurrent",
                          GCInspector.isConcurrentPhase("SomeCause", "GPGC Minor Pauses"));
        Assert.assertFalse("Regular GC should not be considered concurrent",
                          GCInspector.isConcurrentPhase("GCCause", "RegularGC"));
        Assert.assertFalse("ParallelGC should not be considered concurrent",
                          GCInspector.isConcurrentPhase("Allocation Failure", "PS Scavenge"));
        Assert.assertFalse("G1 Young Generation should not be considered concurrent",
                          GCInspector.isConcurrentPhase("G1 Evacuation Pause", "G1 Young Generation"));
    }
}
