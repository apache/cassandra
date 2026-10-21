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
     * Regression test for mixed-mode paxos with ttl'd legacy paxos state. CEP-14 made legacy paxos
     * state expire off the ballot time rather than the commit-persist time, which eliminated the race
     * addressed by CASSANDRA-12043 and let that check be removed. Historically, a post-CEP-14
     * coordinator paired with pre-CEP-14 replicas could hit an infinite prepare loop when a tombstoned
     * most-recent-commit on one replica shadowed the coordinator's resend. This test keeps the
     * scenario covered for current upgrade paths.
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
        .upgradesToCurrentFrom(v41)
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
