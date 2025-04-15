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
     * Paxos uses a 'zero' ballot in place of null when it doesn't find a ballot in system.paxos. CEP-14 changed the lsb
     * of the zero ballot uuid from the TimeUUID.MIN_CLOCK_SEQ_AND_NODE value of 0x8080808080808080 (-9187201950435737472)
     * to 0. It also removed the check added in CASSANDRA-12043, since the way it read and filtered ttld paxos data had
     * been improved.
     * <p>
     * In mixed mode with a 4.0 or lower replica and a 4.1 and higher paxos coordinator, and in the absence of existing
     * paxos metadata for the key being queried, the prepare phase will interpret the mismatched ‘zero’ ballots as the
     * 4.0 and lower nodes having missed the most recent commit and will attempt to update them using the 4.1 zero ballot
     * and empty partition update.
     * <p>
     * In cases where this is the first paxos operation on a key, or the previously ttl'd paxos data on disk had been purged,
     * this would just add a retry step as it updated the 4.0 and lower hosts with its zero ballot.
     * <p>
     * On nodes where there was ttl'd paxos data though, the ttl'd data on disk would shadow this update. This would
     * happen because paxos commits are recorded to system.paxos using the ballot timestamp as the write timestamp, so
     * the more recent write updating the commit with timestamp 0 would be shadowed by the now ttl’d write with a ‘real’
     * timestamp. When the prepare phase restarted it would again get the old zero value and cause the prepare phase to
     * get into an infinite loop.
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
        .upgradesToCurrentFrom(v40)
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
