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

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;

public abstract class MixedModePaxosTestBase extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(MixedModePaxosTestBase.class);

    abstract boolean upgradedCoordinator();

    /**
     * Tests the mixed mode loop bug in CASSANDRA-20493
     * <p>
     * Regression test for mixed-mode paxos with TTL'd paxos data on disk. CEP-14 changed the lsb of the
     * zero ballot uuid from TimeUUID.MIN_CLOCK_SEQ_AND_NODE (0x8080808080808080) to 0 and removed the
     * CASSANDRA-12043 check; the historical concern was that pre-CEP-14 replicas paired with newer
     * coordinators could get stuck in an infinite prepare loop when ttl'd system.paxos data shadowed
     * the coordinator's zero-ballot update. This test keeps the scenario covered for current upgrade
     * paths.
     */
    private void ttldPaxosStateTest(boolean legacyAware, boolean upgradeAware) throws Throwable
    {
        String keyspace = KEYSPACE;
        String table = "tbl";
        int gcGrace = 10;
        int key = 1;
        new TestCase()
        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
        .nodes(2)
        .nodesToUpgrade(1)
        .upgradesToCurrentFrom(v41)
        .setup(cluster -> {
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int primary key, v int) " +
                                        "WITH gc_grace_seconds=%s", keyspace, table, gcGrace));
        })
        .runAfterClusterUpgrade(cluster -> {
            // disable compaction to prevent paxos state from being purged
            cluster.forEach(instance -> instance.nodetool("disableautocompaction"));

            // insert a ttl'd committed paxos state
            long ballotMicros = TimeUnit.NANOSECONDS.toMicros(System.currentTimeMillis());
            FakePaxosHelper helper = FakePaxosHelper.create(cluster.coordinator(1), keyspace, table, key, gcGrace, ballotMicros);

            // confirm none of the nodes have paxos state
            for (int i = 1; i <= cluster.size(); i++)
                helper.assertNoPaxosData(cluster.coordinator(i));


            // save commit to both nodes
            if (upgradeAware)
                helper.saveCommit(cluster.coordinator(1));

            if (legacyAware)
                helper.saveCommit(cluster.coordinator(2));

            // wait for the paxos state to expire
            Thread.sleep(TimeUnit.SECONDS.toMillis(gcGrace * 2));

            // confirm paxos state has ttld
            helper.assertNoPaxosData(cluster.coordinator(1));
            helper.assertNoPaxosData(cluster.coordinator(2));

            // paxos operation should not timeout
            cluster.coordinator(upgradedCoordinator() ? 1 : 2).execute(format("SELECT * FROM %s.%s WHERE k=%s", keyspace, table, key), ConsistencyLevel.SERIAL);
        })
        .run();
    }

    @Test
    public void upgradeAwareTTldPaxosStateTest() throws Throwable
    {
        ttldPaxosStateTest(false, true);
    }

    @Test
    public void legacyAwareTTldPaxosStateTest() throws Throwable
    {
        ttldPaxosStateTest(true, false);
    }

    @Test
    public void bothAwareTTldPaxosStateTest() throws Throwable
    {
        ttldPaxosStateTest(true, true);
    }

    /**
     * This is an upgrade test, and paxos internally limits ttls to 3 hours, so we have to manually save commits in
     * the paxos table to get entries ttl'd in a reasonable amount of time
     */
    static class FakePaxosHelper
    {
        static final int current_version = MessagingService.current_version;
        static final int version_40a = MessagingService.VERSION_40;

        final UUID cfId;
        final ByteBuffer key;
        final long ballotMicros;
        final int ballotSeconds;
        final int ttl;
        final UUID ballot;
        final PartitionUpdate update;

        public FakePaxosHelper(String keyspace, String table, UUID cfId, int key, int ttl, long ballotMicros)
        {
            this.cfId = cfId;
            this.ttl = ttl;
            TableId tableId = TableId.fromUUID(cfId);
            TableMetadata metadata = TableMetadata.builder(keyspace, table)
                                                  .id(tableId)
                                                  .partitioner(Murmur3Partitioner.instance)
                                                  .addPartitionKeyColumn("k", Int32Type.instance)
                                                  .addRegularColumn("v", Int32Type.instance)
                                                  .build();

            this.key = ByteBufferUtil.bytes(key);
            Row row = new SimpleBuilders.RowBuilder(metadata).add("v", (int) key).build();
            this.update = PartitionUpdate.singleRowUpdate(metadata, this.key, row);



            this.ballotMicros = ballotMicros;
            this.ballotSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(ballotMicros);
            this.ballot = Ballot.atUnixMicrosWithLsb(ballotMicros, 0, Ballot.Flag.GLOBAL).asUUID();
        }

        ByteBuffer updateBytes(int version)
        {
            return PartitionUpdate.toBytes(update, version);
        }

        private Object[][] paxosData(ICoordinator coordinator)
        {
            return coordinator.execute("SELECT * FROM system.paxos WHERE row_key = ? AND cf_id = ?", ConsistencyLevel.ONE, key, cfId);
        }

        void assertNoPaxosData(ICoordinator coordinator)
        {
            Assert.assertEquals(0, paxosData(coordinator).length);
        }

        void assertPaxosData(ICoordinator coordinator)
        {
            Assert.assertEquals(1, paxosData(coordinator).length);
        }

        void saveCommit(ICoordinator coordinator)
        {
            String cql = "UPDATE system.paxos USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
            coordinator.execute(cql, ConsistencyLevel.ONE,
                                ballotMicros,
                                ttl,
                                ballot,
                                updateBytes(version_40a),
                                version_40a,
                                key,
                                cfId);
        }

        void tombstoneCommit(ICoordinator coordinator)
        {
            String cql = "DELETE proposal_ballot, proposal, most_recent_commit_at, most_recent_commit, most_recent_commit_version FROM system.paxos USING TIMESTAMP ? WHERE row_key = ? AND cf_id = ?";
            coordinator.execute(cql, ConsistencyLevel.ONE,
                                ballotMicros,
                                key,
                                cfId);
        }

        void saveCommitNoTTL(ICoordinator coordinator)
        {
            String cql = "UPDATE system.paxos USING TIMESTAMP ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
            coordinator.execute(cql, ConsistencyLevel.ONE,
                                ballotMicros,
                                ballot,
                                updateBytes(version_40a),
                                version_40a,
                                key,
                                cfId);
        }

        public static FakePaxosHelper create(ICoordinator coordinator, String keyspace, String table, int key, int ttl, long ballotMicros)
        {
            UUID cfId = (UUID) coordinator.execute("SELECT id FROM system_schema.tables WHERE keyspace_name=? AND table_name=?", ConsistencyLevel.ONE, keyspace, table)[0][0];
            return new FakePaxosHelper(keyspace, table, cfId, key, ttl, ballotMicros);
        }
    }
}
