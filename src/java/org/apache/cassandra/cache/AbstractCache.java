package org.apache.cassandra.cache;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class AbstractCache
{
    static void registerMBean(Object cache, String table, String name)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            String mbeanName = "org.apache.cassandra.db:type=Caches,keyspace=" + table + ",cache=" + name;
            mbs.registerMBean(cache, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
