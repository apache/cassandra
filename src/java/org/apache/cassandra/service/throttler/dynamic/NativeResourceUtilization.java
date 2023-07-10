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

import java.util.HashMap;
import java.util.Map;
import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;


public class NativeResourceUtilization implements IResourceUtilzation
{
    public static final String JVM_CPU_UTIL = "JVM";
    public static final String CONTAINER_CPU_UTIL = "Container";
    private static final OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    @Override
    public void setup()
    {
    }

    @Override
    public Map<String, Double> getCurrentCPUUtil()
    {
        return new HashMap<>()
        {
            {
                // Since we don't run anything else within the Cassandra container, so Cassandra JVM = Container, the reason for still keeping two metrics is that more signals are better.
                put(JVM_CPU_UTIL, osBean.getProcessCpuLoad() * 100d);
                put(CONTAINER_CPU_UTIL, osBean.getSystemCpuLoad() * 100d);
            }
        };
    }
}
