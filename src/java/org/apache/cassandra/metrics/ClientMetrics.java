package org.apache.cassandra.metrics;

import java.util.concurrent.Callable;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

public class ClientMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Client");
    
    public static final ClientMetrics instance = new ClientMetrics();
    
    private ClientMetrics()
    {
    }

    public void addCounter(String name, final Callable<Integer> provider)
    {
        Metrics.newGauge(factory.createMetricName(name), new Gauge<Integer>()
        {
            public Integer value()
            {
                try
                {
                    return provider.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
