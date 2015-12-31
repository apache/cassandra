package org.apache.cassandra.metrics;

import org.junit.Test;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;
import static org.junit.Assert.*;


public class CassandraMetricsRegistryTest
{
    // A class with a name ending in '$'
    private static class StrangeName$
    {
    }

    @Test
    public void testChooseType()
    {
        assertEquals("StrangeName", MetricName.chooseType(null, StrangeName$.class));
        assertEquals("StrangeName", MetricName.chooseType("", StrangeName$.class));
        assertEquals("String", MetricName.chooseType(null, String.class));
        assertEquals("String", MetricName.chooseType("", String.class));
        
        assertEquals("a", MetricName.chooseType("a", StrangeName$.class));
        assertEquals("b", MetricName.chooseType("b", String.class));
    }
    
    @Test
    public void testMetricName()
    {
         MetricName name = new MetricName(StrangeName$.class, "NaMe", "ScOpE");
         assertEquals("StrangeName", name.getType());
    }
    
}
