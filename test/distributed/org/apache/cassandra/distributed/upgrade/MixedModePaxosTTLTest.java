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
import org.apache.cassandra.distributed.upgrade.MixedModePaxosTestBase.FakePaxosHelper;

import static java.lang.String.format;

public class MixedModePaxosTTLTest extends UpgradeTestBase
{
    /**
     * Tests the mixed mode paxos loop bug in CASSANDRA-20514
     *
     * CEP-14 changed the ttl behavior of legacy paxos state to expire based off the ballot time of the operation being
     * persisted, not the time a commit is persisted. This eliminated the race addressed by CASSANDRA-12043, and so the
     * check it added to the most recent commit prepare logic was removed.
     *
     * When operating in mixed mode though, this can still be a problem. If a 4.1 or higher node is coordinating a paxos
     * operation with 2 or more replicas on 4.0 or lower, this race becomes a problem again. You need 3 things to make
     * this an infinite loop
     * 1. a 4.1 node coordinating a paxos operation with 2x 4.0 replicas
     * 2. replica A) a 4.0 node returns a most recent commit for a ballot that's could have been ttld
     * 3. replica B) a 4.0 node has ttl'd that mrc AND converted the ttld cells into tombstones
     *
     * The 4.1 coordinator receives the mrc from replica A, but since it no longer disregards missing most recent commits
     * past the ttl window, it sends the "missing" commit to replica B. Since replica B now has a tombstone for that mrc,
     * and tombstones win when reconciled with live cells, even ones with ttls, the commit is a noop and it continues
     * to report nothing for its mrc value when the coordinator restarts the prepare phase. This loops until the query
     * times out.
     */
    @Test
    public void legacyExpiredStateTest() throws Throwable
    {
        String keyspace = "ks";
        String table = "tbl";
        int gcGrace = 60*60*24; // 1 day
        int key = 100;  // hashes to nodes 2 & 3 w/ murmur @ RF=2
        new TestCase()
        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK).set("cas_contention_timeout", "500ms"))
        .nodes(3)
        .nodesToUpgrade(1)
        .upgradesToCurrentFrom(v40)
        .setup(cluster -> {
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': '2'}", keyspace));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) " +
                                        "WITH gc_grace_seconds=%s", keyspace, table, gcGrace));
        })
        .runAfterClusterUpgrade(cluster -> {
            // disable compaction to prevent paxos state from being purged
            cluster.forEach(instance -> instance.nodetool("disableautocompaction"));

            long ballotMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
            ballotMicros -= TimeUnit.SECONDS.toMicros(gcGrace + 10);
            FakePaxosHelper helper = FakePaxosHelper.create(cluster.coordinator(1), keyspace, table, key, gcGrace, ballotMicros);

            // confirm none of the nodes have paxos state
            for (int i = 1; i <= cluster.size(); i++)
                helper.assertNoPaxosData(cluster.coordinator(i));

            // save a tombstoned commit to one node to simulate expired cells being converted to tombstones
            helper.tombstoneCommit(cluster.coordinator(2));

            // insert paxos state and confirm it hasn't ttl'd yet
            helper.saveCommit(cluster.coordinator(3));
            helper.assertPaxosData(cluster.coordinator(3));

            // paxos operation should not timeout
            cluster.coordinator(1).execute(format("SELECT * FROM %s.%s WHERE k=%s", keyspace, table, key), ConsistencyLevel.SERIAL);
        })
        .run();
    }
}
