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

import java.util.Map;

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
     * Provides the current CPU utilization signal. There can be multiple ways we can capture CPU utilization.
     * Some of them are highlighted below.
     * 1. OperatingSystemMXBean.getProcessCpuLoad()
     * 2. OperatingSystemMXBean.getSystemCpuLoad()
     * 3. vmstats
     *
     * @return Returns a Map containing one or more CPU utilization signals and each entry with the following details:
     * key: Type of CPU signal, Value: actual CPU utilization between [0-100].
     * For example,
     *      "JVM"-75.24
     *      "Container"-74.12
     *      "Vmstats"-73.19
     */
    public Map<String, Double> getCurrentCPUUtil();
}
