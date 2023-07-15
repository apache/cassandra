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

import org.junit.Assert;
import org.junit.Test;

public class NativeResourceUtilizationTest
{
    private static final String TEST_CPU_STAT_INVALID_FILE_PATH = "throttling/invalid_file.stat";
    public static final String TEST_CPU_STAT_FILE_PATH = "throttling/test_cpu.stat";

    @Test
    public void testCurrentCpuUtil1()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        Long currentCpuUtil1 = nativeResourceUtilization.getCurrentCpuUtil1();
        Assert.assertTrue( "GetCurrentCpuUtil1: " + currentCpuUtil1, currentCpuUtil1 >= 0.0 && currentCpuUtil1 <= 100.0);
    }

    public void testCurrentCpuUtil2()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        Long currentCpuUtil2 = nativeResourceUtilization.getCurrentCpuUtil2();
        Assert.assertTrue( "GetCurrentCpuUtil2: " + currentCpuUtil2, currentCpuUtil2 >= 0.0 && currentCpuUtil2 <= 100.0);
    }

    @Test
    public void testNRThrottled1ReadFailure()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        nativeResourceUtilization.cpuStatFilePath = TEST_CPU_STAT_INVALID_FILE_PATH;
        long nrThrottled1 = nativeResourceUtilization.getCpuNRThrottled1();
        Assert.assertEquals(-1, nrThrottled1);
        Assert.assertEquals(1, nativeResourceUtilization.readFailures.getCount());
    }

    @Test
    public void testNRThrottled1ReadSuccess()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        nativeResourceUtilization.cpuStatFilePath = getClass().getClassLoader().getResource(TEST_CPU_STAT_FILE_PATH).getFile();
        long nrThrottled1 = nativeResourceUtilization.getCpuNRThrottled1();
        Assert.assertEquals(5468, nrThrottled1);
        Assert.assertEquals(0, nativeResourceUtilization.readFailures.getCount());
    }

    @Test
    public void testNRThrottled2ReadSuccess()
    {
        NativeResourceUtilization nativeResourceUtilization = new NativeResourceUtilization();
        long nrThrottled2 = nativeResourceUtilization.getCpuNRThrottled2();
        Assert.assertEquals(-1, nrThrottled2);
        Assert.assertEquals(0, nativeResourceUtilization.readFailures.getCount());
    }
}
