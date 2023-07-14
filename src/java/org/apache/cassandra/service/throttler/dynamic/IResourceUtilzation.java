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

/**
 * IResourceUtil defines an interface to get the current utilzation of the current JVM.
 */
public interface IResourceUtilzation
{
    /**
     * Called once in the lifetime of the daemon to set up the necessary one-time initialization parameters.
     * This is called after the node finishes joining the ring.
     */
    public void setup();

    /**
     * CPU utilization signal1. It provides the current CPU utilization signal.
     * There can be multiple ways we can capture CPU utilization. Some of them are highlighted below.
     * 1. OperatingSystemMXBean.getProcessCpuLoad()
     * 2. OperatingSystemMXBean.getSystemCpuLoad()
     * 3. vmstats
     *
     * @return Returns a CPU utilization signals1, and its value would be between [0-100].
     */
    public Long getCurrentCpuUtil1();

    /**
     * CPU utilization signal2. Same as "getCurrentCpuUtil1()" but a different source.
     *
     * @return Returns a CPU utilization signals1, and its value would be between [0-100]. If there is only one
     * source, then return -1.
     */
    public Long getCurrentCpuUtil2();

    /**
     * CPU throttling signal1. It represents the number of runnable periods in which the application used its entire quota and was throttled.
     * It is one of the CPU metrics (nr_throttled) for a cgroup located in /sys/fs/cgroup/cpu,cpuacct/<container>
     *
     * @return the value of nr_throttled
     **/
    public Long getCpuNRThrottled1();

    /**
     * CPU throttling signal2. Same as "getCpuNRThrottled1" but a different source.
     *
     * @return the value of nr_throttled. If there is only one source, then return -1
     **/
    public Long getCpuNRThrottled2();
}
