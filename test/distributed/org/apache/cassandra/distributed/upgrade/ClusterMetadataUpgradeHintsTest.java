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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClusterMetadataUpgradeHintsTest extends UpgradeTestBase
{
    @Test
    public void upgradeWithHintsTest() throws Throwable
    {
        final int rowCount = 50;
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .upgradesToCurrentFrom(v41)
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int, v int, PRIMARY KEY (k))");
            cluster.get(2).nodetoolResult("pausehandoff").asserts().success();

            // insert some data while node1 is down so that hints are written
            cluster.get(1).shutdown().get();
            for (int i = 0; i < rowCount; i++)
                cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".tbl(k,v) VALUES (?, ?)", ConsistencyLevel.ANY, i, i);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(3).flush(KEYSPACE);
            cluster.get(1).startup();

            // Check that none of the writes got to node1
            SimpleQueryResult rows = cluster.get(1).executeInternalWithResult("SELECT * FROM " + KEYSPACE + ".tbl");
            assertFalse(rows.hasNext());
        })
        .runAfterClusterUpgrade((cluster) -> {
            Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> {
                SimpleQueryResult rows = cluster.get(1).executeInternalWithResult("SELECT * FROM " + KEYSPACE + ".tbl");
                return rows.toObjectArrays().length == rowCount;
            });

            IInvokableInstance inst = (IInvokableInstance)cluster.get(2);
            long hintsDelivered = inst.callOnInstance(() -> {
                return (long)HintsServiceMetrics.hintsSucceeded.getCount();
            });
            assertEquals(rowCount, hintsDelivered);
        }).run();
    }
}
