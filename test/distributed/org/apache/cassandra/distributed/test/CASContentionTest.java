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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.service.paxos.ContentionStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_USE_SELF_EXECUTION;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REQ;

public class CASContentionTest extends CASTestBase
{
    private static Cluster THREE_NODES;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        PAXOS_USE_SELF_EXECUTION.setBoolean(false);
        TestBaseImpl.beforeClass();
        Consumer<IInstanceConfig> conf = config -> config
                .set("paxos_variant", "v2")
                .set("write_request_timeout_in_ms", 20000L)
                .set("cas_contention_timeout_in_ms", 20000L)
                .set("request_timeout_in_ms", 20000L);
        THREE_NODES = init(Cluster.create(3, conf));
    }

    @AfterClass
    public static void afterClass()
    {
        if (THREE_NODES != null)
            THREE_NODES.close();
    }

    @Test
    public void testDynamicContentionTracing() throws Throwable
    {
        try
        {

            String tableName = tableName("tbl");
            THREE_NODES.schemaChange("CREATE TABLE " + KEYSPACE + '.' + tableName + " (pk int, v int, PRIMARY KEY (pk))");

            CountDownLatch haveStarted = new CountDownLatch(1);
            CountDownLatch haveInvalidated = new CountDownLatch(1);
            THREE_NODES.verbs(PAXOS2_PREPARE_REQ).from(1).messagesMatching((from, to, verb) -> {
                haveStarted.countDown();
                Uninterruptibles.awaitUninterruptibly(haveInvalidated);
                return false;
            }).drop();
            THREE_NODES.get(1).runOnInstance(() -> ContentionStrategy.setStrategy("trace=1"));
            Future<?> insert = THREE_NODES.get(1).async(() -> {
                THREE_NODES.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, v) VALUES (1, 1) IF NOT EXISTS", QUORUM);
            }).call();
            haveStarted.await();
            THREE_NODES.coordinator(2).execute("INSERT INTO " + KEYSPACE + '.' + tableName + " (pk, v) VALUES (1, 1) IF NOT EXISTS", QUORUM);
            haveInvalidated.countDown();
            THREE_NODES.filters().reset();
            insert.get();
            Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
            THREE_NODES.forEach(i -> i.runOnInstance(() -> FBUtilities.waitOnFuture(Stage.TRACING.submit(() -> {}))));
            Object[][] result = THREE_NODES.coordinator(1).execute("SELECT parameters FROM system_traces.sessions", QUORUM);
            Assert.assertEquals(1, result.length);
            Assert.assertEquals(1, result[0].length);
            Assert.assertTrue(Map.class.isAssignableFrom(result[0][0].getClass()));
            Map<?, ?> params = (Map<?, ?>) result[0][0];
            Assert.assertEquals("SERIAL", params.get("consistency"));
            Assert.assertEquals(tableName, params.get("table"));
            Assert.assertEquals(KEYSPACE, params.get("keyspace"));
            Assert.assertEquals("1", params.get("partitionKey"));
            Assert.assertEquals("write", params.get("kind"));
        }
        finally
        {
            THREE_NODES.filters().reset();
        }
    }


}
