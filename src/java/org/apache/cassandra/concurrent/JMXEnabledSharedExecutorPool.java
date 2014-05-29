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
package org.apache.cassandra.concurrent;

import java.lang.management.ManagementFactory;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.metrics.SEPMetrics;

public class JMXEnabledSharedExecutorPool extends SharedExecutorPool
{

    public static final JMXEnabledSharedExecutorPool SHARED = new JMXEnabledSharedExecutorPool("SharedPool");

    public JMXEnabledSharedExecutorPool(String poolName)
    {
        super(poolName);
    }

    public interface JMXEnabledSEPExecutorMBean extends JMXEnabledThreadPoolExecutorMBean
    {
    }

    public class JMXEnabledSEPExecutor extends SEPExecutor implements JMXEnabledSEPExecutorMBean
    {

        private final SEPMetrics metrics;
        private final String mbeanName;

        public JMXEnabledSEPExecutor(int poolSize, int maxQueuedLength, String name, String jmxPath)
        {
            super(JMXEnabledSharedExecutorPool.this, poolSize, maxQueuedLength);
            metrics = new SEPMetrics(this, jmxPath, name);

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbeanName = "org.apache.cassandra." + jmxPath + ":type=" + name;

            try
            {
                mbs.registerMBean(this, new ObjectName(mbeanName));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        private void unregisterMBean()
        {
            try
            {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(mbeanName));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            // release metrics
            metrics.release();
        }

        @Override
        public synchronized void shutdown()
        {
            // synchronized, because there is no way to access super.mainLock, which would be
            // the preferred way to make this threadsafe
            if (!isShutdown())
            {
                unregisterMBean();
            }
            super.shutdown();
        }

        public int getCoreThreads()
        {
            return 0;
        }

        public void setCoreThreads(int number)
        {
            throw new UnsupportedOperationException();
        }

        public void setMaximumThreads(int number)
        {
            throw new UnsupportedOperationException();
        }
    }

    public TracingAwareExecutorService newExecutor(int maxConcurrency, int maxQueuedTasks, String name, String jmxPath)
    {
        JMXEnabledSEPExecutor executor = new JMXEnabledSEPExecutor(maxConcurrency, maxQueuedTasks, name, jmxPath);
        executors.add(executor);
        return executor;
    }
}
