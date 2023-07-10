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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

public class CassandraResourceUtilizationTest
{
    @Test
    public void testJVMCPU()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup();
        cassandraResourceUtilization.getCurrentUtilization();

        Assert.assertEquals(2, CassandraResourceUtilization.cpuMetrics.size());
        Gauge<Long> jvmCpuCur = CassandraResourceUtilization.cpuMetrics.get(NativeResourceUtilization.JVM_CPU_UTIL).cpuUtilCur;
        Assert.assertTrue("Current: " + jvmCpuCur.getValue(), jvmCpuCur.getValue() >= 0);

        Meter jvmCpuHistory = CassandraResourceUtilization.cpuMetrics.get(NativeResourceUtilization.JVM_CPU_UTIL).cpuUtilHistory;
        Assert.assertTrue("OneMinuteRate: " + jvmCpuHistory.getOneMinuteRate(), jvmCpuHistory.getOneMinuteRate() >= 0.0d && jvmCpuHistory.getOneMinuteRate() <= 100.0d);
        Assert.assertTrue("FiveMinuteRate: " + jvmCpuHistory.getFiveMinuteRate(), jvmCpuHistory.getFiveMinuteRate() >= 0.0d && jvmCpuHistory.getFiveMinuteRate() <= 100.0d);
        Assert.assertTrue("FifteenMinuteRate: " + jvmCpuHistory.getFifteenMinuteRate(), jvmCpuHistory.getFifteenMinuteRate() >= 0.0d && jvmCpuHistory.getFifteenMinuteRate() <= 100.0d);
    }

    @Test
    public void testContainerCPU()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup();
        cassandraResourceUtilization.getCurrentUtilization();

        Assert.assertEquals(2, CassandraResourceUtilization.cpuMetrics.size());
        Gauge<Long> containerCpuCur = CassandraResourceUtilization.cpuMetrics.get(NativeResourceUtilization.CONTAINER_CPU_UTIL).cpuUtilCur;
        Assert.assertTrue("Current: " + containerCpuCur.getValue(), containerCpuCur.getValue() >= 0);

        Meter containerCpuHistory = CassandraResourceUtilization.cpuMetrics.get(NativeResourceUtilization.CONTAINER_CPU_UTIL).cpuUtilHistory;
        Assert.assertTrue("OneMinuteRate: " + containerCpuHistory.getOneMinuteRate(), containerCpuHistory.getOneMinuteRate() >= 0.0d && containerCpuHistory.getOneMinuteRate() <= 100.0d);
        Assert.assertTrue("FiveMinuteRate: " + containerCpuHistory.getFiveMinuteRate(), containerCpuHistory.getFiveMinuteRate() >= 0.0d && containerCpuHistory.getFiveMinuteRate() <= 100.0d);
        Assert.assertTrue("FifteenMinuteRate: " + containerCpuHistory.getFifteenMinuteRate(), containerCpuHistory.getFifteenMinuteRate() >= 0.0d && containerCpuHistory.getFifteenMinuteRate() <= 100.0d);
    }

    @Test
    public void testPendingReads()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup();
        cassandraResourceUtilization.getCurrentUtilization();

        Gauge<Integer> pendingReadsCur = CassandraResourceUtilization.pendingReadsCur;
        Assert.assertTrue("Current: " + pendingReadsCur.getValue(), pendingReadsCur.getValue() >= 0);

        Meter pendingReadsHistory = CassandraResourceUtilization.pendingReadsHistory;
        Assert.assertTrue("OneMinuteRate: " + pendingReadsHistory.getOneMinuteRate(), pendingReadsHistory.getOneMinuteRate() >= 0.0d && pendingReadsHistory.getOneMinuteRate() <= 100.0d);
        Assert.assertTrue("FiveMinuteRate: " + pendingReadsHistory.getFiveMinuteRate(), pendingReadsHistory.getFiveMinuteRate() >= 0.0d && pendingReadsHistory.getFiveMinuteRate() <= 100.0d);
        Assert.assertTrue("FifteenMinuteRate: " + pendingReadsHistory.getFifteenMinuteRate(), pendingReadsHistory.getFifteenMinuteRate() >= 0.0d && pendingReadsHistory.getFifteenMinuteRate() <= 100.0d);
    }

    @Test
    public void testPendingMutations()
    {
        CassandraResourceUtilization cassandraResourceUtilization = new CassandraResourceUtilization();
        cassandraResourceUtilization.setup();
        cassandraResourceUtilization.getCurrentUtilization();

        Gauge<Integer> pendingMutationsCur = CassandraResourceUtilization.pendingMutationsCur;
        Assert.assertTrue("Current: " + pendingMutationsCur.getValue(), pendingMutationsCur.getValue() >= 0);

        Meter pendingMutationsHistory = CassandraResourceUtilization.pendingMutationsHistory;
        Assert.assertTrue("OneMinuteRate: " + pendingMutationsHistory.getOneMinuteRate(), pendingMutationsHistory.getOneMinuteRate() >= 0.0d && pendingMutationsHistory.getOneMinuteRate() <= 100.0d);
        Assert.assertTrue("FiveMinuteRate: " + pendingMutationsHistory.getFiveMinuteRate(), pendingMutationsHistory.getFiveMinuteRate() >= 0.0d && pendingMutationsHistory.getFiveMinuteRate() <= 100.0d);
        Assert.assertTrue("FifteenMinuteRate: " + pendingMutationsHistory.getFifteenMinuteRate(), pendingMutationsHistory.getFifteenMinuteRate() >= 0.0d && pendingMutationsHistory.getFifteenMinuteRate() <= 100.0d);
    }
}
