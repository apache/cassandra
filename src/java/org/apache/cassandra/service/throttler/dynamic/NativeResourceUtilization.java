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

package org.apache.cassandra.service.throttler.dynamic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.management.ManagementFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.sun.management.OperatingSystemMXBean;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;


public class NativeResourceUtilization implements IResourceUtilzation
{
    private static final Logger logger = LoggerFactory.getLogger(NativeResourceUtilization.class);

    private static final OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private static final String NR_THROTTLED_KEY = "nr_throttled";
    private static final MetricNameFactory factory = new DefaultNameFactory("NativeResourceUtilization");

    public String cpuStatFilePath = "/sys/fs/cgroup/cpu,cpuacct/cpu.stat";
    public final Counter readFailures = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ReadFailures"));

    @Override
    public void setup()
    {
    }

    @Override
    public Long getCurrentCpuUtil1()
    {
        // Cassandra container CPU utilization
        return Double.valueOf(osBean.getProcessCpuLoad() * 100d).longValue();
    }

    @Override
    public Long getCurrentCpuUtil2()
    {
        // Since we don't run anything else within the Cassandra container, so Cassandra JVM = Container, the reason for still keeping two metrics is that more signals are better
        return Double.valueOf(osBean.getSystemCpuLoad() * 100d).longValue();
    }

    @Override
    public Long getCpuNRThrottled1()
    {
        //BufferedReader reader;
        long nrThrottled = -1;
        try (BufferedReader reader = new BufferedReader(new FileReader(cpuStatFilePath)))
        {
            /** Usually, it is in the following format:
             * $> cat /sys/fs/cgroup/cpu,cpuacct/cpu.stat
             * nr_periods 53857
             * nr_throttled 1636
             * throttled_time 1182781210
             * nr_bursts 0
             * burst_time 0
             *
             */
            //reader = new BufferedReader(new FileReader(cpuStatFilePath));
            String line = reader.readLine();
            while (line != null)
            {
                if (line.startsWith(NR_THROTTLED_KEY))
                {
                    String[] split = line.split("\\s+");
                    nrThrottled = Long.parseLong(split[1]);
                    break;
                }
                line = reader.readLine();
            }
        }
        catch (Exception e)
        {
            readFailures.inc();
            logger.error("Exception while reading {}, error: {}", cpuStatFilePath, e);
        }
        return nrThrottled;
    }

    @Override
    public Long getCpuNRThrottled2()
    {
        return -1L;
    }
}
