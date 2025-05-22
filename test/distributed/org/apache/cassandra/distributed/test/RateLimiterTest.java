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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.throttler.dynamic.CassandraResourceUtilization;
import org.apache.cassandra.service.throttler.dynamic.TrafficType;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class RateLimiterTest extends TestBaseImpl
{
    @Parameterized.Parameter
    public PaxosVariant paxosVariant;
    private static Cluster cluster1;
    private static Cluster cluster2;

    private static final int coordinatorNode = 1;
    private static final int totalNodes = 3;

    @Parameterized.Parameters(name = "paxosVariant={0}")
    public static List<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{ PaxosVariant.v1 });
        result.add(new Object[]{ PaxosVariant.v2 });
        return result;
    }

    @BeforeClass
    public static void init() throws IOException
    {
        cluster1 = Cluster.build(3).withInstanceInitializer(RateLimiterTest.BB::install1).start();
        cluster2 = Cluster.build(3).withInstanceInitializer(RateLimiterTest.BB::install2).start();

        cluster1.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        cluster2.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");

        cluster1.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
        cluster2.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
    }

    @Before
    public void set()
    {
        String variant = paxosVariant.toString();
        cluster1.forEach(i -> i.runOnInstance(() -> {
            StorageProxy.instance.setPaxosVariant(variant);
        }));
        cluster2.forEach(i -> i.runOnInstance(() -> {
            StorageProxy.instance.setPaxosVariant(variant);
        }));
    }

    @Test
    public void testCoordinatorThrottlingPointReadTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT", String.format("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'", KEYSPACE), true);
    }

    @Test
    public void testPeerThrottlingPointReadTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT", String.format("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'", KEYSPACE), true);
    }

    @Test
    public void testCoordinatorThrottlingScanTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT", String.format("SELECT * FROM %s.tbl", KEYSPACE), true);
    }

    @Test
    public void testPeerThrottlingScanTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT", String.format("SELECT * FROM %s.tbl", KEYSPACE), true);
    }

    @Test
    public void testCoordinatorThrottlingWriteTraffic()
    {
        helperThrottleCoordinatorTraffic("INSERT", String.format("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (0, 'abc', 10, 20)", KEYSPACE), true);
    }

    @Test
    public void testPeerThrottlingWriteTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("INSERT", String.format("INSERT INTO %s.tbl (pk, ck, v1, v2) VALUES (0, 'abc', 10, 20)", KEYSPACE), true);
    }

    @Test
    public void testCoordinatorThrottlingUpdateTraffic()
    {
        helperThrottleCoordinatorTraffic("UPDATE", String.format("UPDATE %s.tbl SET v2 = 11 WHERE pk = 0 AND ck = 'abc'", KEYSPACE), true);
    }

    @Test
    public void testPeerThrottlingUpdateTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("UPDATE", String.format("UPDATE %s.tbl SET v2 = 11 WHERE pk = 0 AND ck = 'abc'", KEYSPACE), true);
    }

    @Test
    public void testCoordinatorThrottlingDeleteTraffic()
    {
        helperThrottleCoordinatorTraffic("DELETE", String.format("DELETE FROM %s.tbl WHERE pk = 0 AND ck = 'abc'", KEYSPACE), true);
    }

    @Test
    public void testPeerThrottlingDeleteTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("DELETE", String.format("DELETE FROM %s.tbl WHERE pk = 0 AND ck = 'abc'", KEYSPACE), true);
    }

    @Test
    public void testCoordinatorThrottlingLWTTraffic()
    {
        long[] casWriteThrottleCount = new long[totalNodes];
        for (int i = 1; i <= totalNodes; i++)
        {
            casWriteThrottleCount[i - 1] = cluster1.get(i).callOnInstance(() -> {
                return ClientRequestsMetricsHolder.casWriteMetrics.rateLimiterThrottles.getCount();
            });
        }
        helperThrottleCoordinatorTraffic("LWT", String.format("UPDATE %s.tbl SET v2 = 11 WHERE pk = 0 AND ck = 'abc' IF v1 = 10", KEYSPACE), true);
        for (int i = 1; i <= totalNodes; i++)
        {
            long currentCount = cluster1.get(i).callOnInstance(() -> {
                return ClientRequestsMetricsHolder.casWriteMetrics.rateLimiterThrottles.getCount();
            });
            Assert.assertEquals(1, currentCount - casWriteThrottleCount[i - 1]);
        }
    }

    @Test
    public void testPeerThrottlingLWTTraffic() throws TimeoutException
    {
        long casWriteThrottleCount = cluster2.get(coordinatorNode).callOnInstance(() -> {
            return ClientRequestsMetricsHolder.casWriteMetrics.rateLimiterThrottles.getCount();
        });
        helperThrottlePeerTraffic("LWT", String.format("UPDATE %s.tbl SET v2 = 11 WHERE pk = 0 AND ck = 'abc' IF v1 = 10", KEYSPACE), true);
        long curCasWriteThrottleCount = cluster2.get(coordinatorNode).callOnInstance(() -> {
            return ClientRequestsMetricsHolder.casWriteMetrics.rateLimiterThrottles.getCount();
        });
        Assert.assertEquals(1, curCasWriteThrottleCount - casWriteThrottleCount);
    }

    @Test
    public void testCoordinatorThrottlingBatchTraffic()
    {
        helperThrottleCoordinatorTraffic("BATCH", String.format("BEGIN BATCH\n" +
                                                                "UPDATE %s.tbl SET v1 = 10 where pk = 0 and ck = 'abc';\n" +
                                                                "UPDATE %s.tbl SET v2 = 11 where pk = 0 and ck = 'abc';\n" +
                                                                "APPLY BATCH;", KEYSPACE, KEYSPACE), true);
    }

    @Test
    public void testPeerThrottlingBatchTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("BATCH", String.format("BEGIN BATCH\n" +
                                                         "UPDATE %s.tbl SET v1 = 10 where pk = 0 and ck = 'abc';\n" +
                                                         "UPDATE %s.tbl SET v2 = 11 where pk = 0 and ck = 'abc';\n" +
                                                         "APPLY BATCH;", KEYSPACE, KEYSPACE), true);
    }

    @Test
    public void testCoordinatorDoNotThrottleSystemReadTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT_SYSTEM", "SELECT * FROM system.local LIMIT 1", false);
    }

    @Test
    public void testPeerDoNotThrottleSystemReadTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT_SYSTEM", "SELECT * FROM system.local LIMIT 1", false);
    }

    @Test
    public void testCoordinatorDoNotThrottleSystemSchemaReadTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT_SYSTEM_SCHEMA", "SELECT * FROM system_schema.tables LIMIT 1", false);
    }

    @Test
    public void testPeerDoNotThrottleSystemSchemaReadTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT_SYSTEM_SCHEMA", "SELECT * FROM system_schema.tables LIMIT 1", false);
    }

    @Test
    public void testCoordinatorDoNotThrottleSystemDistributedReadTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT_SYSTEM_DISTRIBUTED", "SELECT * FROM system_distributed.repair_history LIMIT 1", false);
    }

    @Test
    public void testPeerDoNotThrottleSystemDistributedReadTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT_SYSTEM_DISTRIBUTED", "SELECT * FROM system_distributed.repair_history LIMIT 1", false);
    }

    @Test
    public void testCoordinatorDoNotThrottleSystemAuthReadTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT_SYSTEM_AUTH", "SELECT * FROM system_auth.roles LIMIT 1", false);
    }

    @Test
    public void testPeerDoNotThrottleSystemAuthReadTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT_SYSTEM_AUTH", "SELECT * FROM system_auth.roles LIMIT 1", false);
    }

    @Test
    public void testCoordinatorDoNotThrottleSystemTracesReadTraffic()
    {
        helperThrottleCoordinatorTraffic("SELECT_SYSTEM_TRACES", "SELECT * FROM system_traces.events LIMIT 1", false);
    }

    @Test
    public void testPeerDoNotThrottleSystemTracesReadTraffic() throws TimeoutException
    {
        helperThrottlePeerTraffic("SELECT_SYSTEM_TRACES", "SELECT * FROM system_traces.events LIMIT 1", false);
    }

    @Test
    public void testCoordinatorThrottlePaxosReadTraffic()
    {
        long[] casReadThrottleCount = new long[totalNodes];
        for (int i = 1; i <= totalNodes; i++)
        {
            casReadThrottleCount[i - 1] = cluster1.get(i).callOnInstance(() -> {
                return ClientRequestsMetricsHolder.casReadMetrics.rateLimiterThrottles.getCount();
            });
        }
        helperThrottleCoordinatorTraffic("PAXOS_READ", String.format("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'", KEYSPACE), true, ConsistencyLevel.LOCAL_SERIAL);
        for (int i = 1; i <= totalNodes; i++)
        {
            long currentCount = cluster1.get(i).callOnInstance(() -> {
                return ClientRequestsMetricsHolder.casReadMetrics.rateLimiterThrottles.getCount();
            });
            Assert.assertEquals(1, currentCount - casReadThrottleCount[i - 1]);
        }
    }

    @Test
    public void testPeerThrottlePaxosReadTraffic() throws TimeoutException
    {
        long casReadThrottleCount = cluster2.get(coordinatorNode).callOnInstance(() -> {
            return ClientRequestsMetricsHolder.casReadMetrics.rateLimiterThrottles.getCount();
        });
        helperThrottlePeerTraffic("PAXOS_READ", String.format("SELECT * FROM %s.tbl WHERE pk=1 AND ck='1'", KEYSPACE), true, ConsistencyLevel.LOCAL_SERIAL);
        long curCasReadThrottleCount = cluster2.get(coordinatorNode).callOnInstance(() -> {
            return ClientRequestsMetricsHolder.casReadMetrics.rateLimiterThrottles.getCount();
        });
        Assert.assertEquals(1, curCasReadThrottleCount - casReadThrottleCount);
    }

    private void helperThrottleCoordinatorTraffic(String operation, String query, boolean shouldFail)
    {
        helperThrottleCoordinatorTraffic(operation, query, shouldFail, ConsistencyLevel.ONE);
    }

    private void helperThrottleCoordinatorTraffic(String operation, String query, boolean shouldFail, ConsistencyLevel cl)
    {
        for (int i = 1; i <= totalNodes; i++)
        {
            try
            {
                cluster1.coordinator(i).execute(query, cl);
                if (shouldFail)
                {
                    fail(String.format("%s statement should fail for node: %d", operation, i));
                }
            }
            catch (Throwable t)
            {
                if (shouldFail)
                {
                    assertThat(t.getMessage(), containsString("from dynamic throttler: 127.0.0"));
                }
                else
                {
                    fail(String.format("%s statement should not have failed for node: %d", operation, i));
                }
            }
        }
    }
    private void helperThrottlePeerTraffic(String operation, String query, boolean shouldFail) throws TimeoutException
    {
        helperThrottlePeerTraffic(operation, query, shouldFail, ConsistencyLevel.LOCAL_QUORUM);
    }

    private void helperThrottlePeerTraffic(String operation, String query, boolean shouldFail, ConsistencyLevel cl) throws TimeoutException
    {
        long mark = cluster2.get(coordinatorNode).logs().mark();
        cluster2.get(coordinatorNode).nodetool("setlogginglevel", "org.apache.cassandra.service.StorageProxy", "ALL");
        try
        {
            cluster2.coordinator(coordinatorNode).execute(query, cl);
            if (shouldFail)
            {
                fail(String.format("%s statement should fail for node: %d", operation, coordinatorNode));
            }
        }
        catch (Throwable t)
        {
            if (shouldFail)
            {
                cluster2.get(coordinatorNode).logs().watchFor("from dynamic throttler: 127.0.0");
                Assert.assertTrue(cluster2.get(coordinatorNode).logs().grep(mark, "from dynamic throttler: 127.0.0.1").getResult().isEmpty());
            }
            else
            {
                fail(String.format("%s statement should not have failed for node: %d", operation, coordinatorNode));
            }
        }
    }

    public static class BB
    {
        public static void install1(ClassLoader classLoader, Integer num)
        {
            // always throttle traffic from all three nodes, i.e., node-1, node-2, and node-3
            new ByteBuddy().rebase(CassandraResourceUtilization.class)
                           .method(named("throttleUserTraffic"))
                           .intercept(MethodDelegation.to(RateLimiterTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void install2(ClassLoader classLoader, Integer num)
        {
            if (num == coordinatorNode)
            {
                return;
            }
            // only throttle the user traffic for node-2 and node-3
            new ByteBuddy().rebase(CassandraResourceUtilization.class)
                           .method(named("throttleUserTraffic"))
                           .intercept(MethodDelegation.to(RateLimiterTest.BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static boolean throttleUserTraffic(String keyspaceName, Collection<String> tables, TrafficType trafficType,
                                                  @SuperCall Callable<Boolean> zuper) throws Exception

        {
            if (!SchemaConstants.isSystemKeyspace(keyspaceName))
                return true;

            return zuper.call();
        }
    }
}
