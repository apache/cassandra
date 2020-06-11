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

package org.apache.cassandra.distributed.upgrade;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.impl.DelegatingInvokableInstance;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.gms.Gossiper;

public class MixedModeReadTest extends UpgradeTestBase
{
    public static final String TABLE_NAME = "tbl";
    public static final String CREATE_TABLE = String.format(
      "CREATE TABLE %s.%s (key int, c1 text, c2 text, c3 text, PRIMARY KEY (key))",
      DistributedTestBase.KEYSPACE, TABLE_NAME);

    public static final String INSERT = String.format(
      "INSERT INTO %s.%s (key, c1, c2, c3) VALUES (?, ?, ?, ?)",
      DistributedTestBase.KEYSPACE, TABLE_NAME);

    public static final String SELECT_C1 = String.format("SELECT key, c1 FROM %s.%s WHERE key = ?",
                                                         DistributedTestBase.KEYSPACE, TABLE_NAME);
    public static final String SELECT_TRACE = "SELECT activity FROM system_traces.events where session_id = ? and source = ? ALLOW FILTERING;";

    @Test
    public void mixedModeReadColumnSubsetDigestCheck() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .upgrade(Versions.Major.v30, Versions.Major.v3X)
        .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK))
        .setup(cluster -> {
            cluster.schemaChange(CREATE_TABLE);
            cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, 1, "foo", "bar", "baz");
            cluster.coordinator(1).execute(INSERT, ConsistencyLevel.ALL, 2, "foo", "bar", "baz");

            // baseline to show no digest mismatches before upgrade
            checkTraceForDigestMismatch(cluster, 1);
            checkTraceForDigestMismatch(cluster, 2);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return; // shouldn't happen but guard for future test changes


            // we need to let gossip settle or the test will fail
            int attempts = 1;
            while (!((DelegatingInvokableInstance) (cluster.get(1))).delegate().callOnInstance(() -> Gossiper.instance.isAnyNodeOn30()))
            {
                if (attempts > 30)
                    throw new RuntimeException("Gossiper.instance.isAnyNodeOn30() continually returns false despite expecting to be true");
                Thread.sleep(1000);
            }

            // should not cause a disgest mismatch in mixed mode
            checkTraceForDigestMismatch(cluster, 1);
            checkTraceForDigestMismatch(cluster, 2);
        })
        .run();
    }

    private void checkTraceForDigestMismatch(UpgradeableCluster cluster, int coordinatorNode)
    {
        UUID sessionId = UUID.randomUUID();
        cluster.coordinator(coordinatorNode).executeWithTracing(sessionId, SELECT_C1, ConsistencyLevel.ALL, 1);
        Object[][] results = cluster.coordinator(coordinatorNode)
                                    .execute(SELECT_TRACE, ConsistencyLevel.ALL,
                                             sessionId, cluster.get(coordinatorNode).broadcastAddress().getAddress());
        for (Object[] result : results)
        {
            String activity = (String) result[0];
            Assert.assertFalse("Found Digest Mismatch", activity.toLowerCase().contains("mismatch for key"));
        }
    }


}
