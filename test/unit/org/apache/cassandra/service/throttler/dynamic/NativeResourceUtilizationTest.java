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

import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class NativeResourceUtilizationTest
{
    private static final String TEST_CPU_STAT_FILE_PATH = "throttling/test_cpu.stat";
    private static final String TEST_CPU_STAT_INVALID_FILE_PATH = "throttling/invalid_file.stat";

    @Test
    public void testGetCurCPUUtil()
    {
        Map<String, Double> cpuUtilization = new NativeResourceUtilization().getCurrentCpuUtil();
        Assert.assertEquals(2, cpuUtilization.size());
        Assert.assertTrue(NativeResourceUtilization.JVM_CPU_UTIL + ":" + cpuUtilization.get(NativeResourceUtilization.JVM_CPU_UTIL), cpuUtilization.get(NativeResourceUtilization.JVM_CPU_UTIL) >= 0.0 && cpuUtilization.get(NativeResourceUtilization.JVM_CPU_UTIL) <= 100.0);
        Assert.assertTrue(NativeResourceUtilization.CONTAINER_CPU_UTIL + ":" + cpuUtilization.get(NativeResourceUtilization.CONTAINER_CPU_UTIL), cpuUtilization.get(NativeResourceUtilization.CONTAINER_CPU_UTIL) >= 0.0 && cpuUtilization.get(NativeResourceUtilization.CONTAINER_CPU_UTIL) <= 100.0);
    }

    @Test
    public void testNRThrottledReadFailure()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        nativeResourceUtilization.cpuStatFilePath = TEST_CPU_STAT_INVALID_FILE_PATH;
        long nrThrottled = nativeResourceUtilization.getCpuNRThrottled();
        Assert.assertEquals(Long.MAX_VALUE, nrThrottled);
        Assert.assertEquals(1, nativeResourceUtilization.readFailures.getCount());
    }

    @Test
    public void testNRThrottledReadSuccess()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        nativeResourceUtilization.cpuStatFilePath = getClass().getClassLoader().getResource(TEST_CPU_STAT_FILE_PATH).getFile();
        long nrThrottled = nativeResourceUtilization.getCpuNRThrottled();
        Assert.assertEquals(5468, nrThrottled);
        Assert.assertEquals(0, nativeResourceUtilization.readFailures.getCount());
    }
}
