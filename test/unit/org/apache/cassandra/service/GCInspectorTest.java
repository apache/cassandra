package org.apache.cassandra.service;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedJUnit4ClassRunner.class)
public class GCInspectorTest
{
    
    GCInspector gcInspector;
    
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    
    @Before
    public void before(){
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
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void ensureLogLessThanWarn()
    {
        gcInspector.setGcLogThresholdInMs(gcInspector.getGcWarnThresholdInMs() + 1);
    }
    
}
