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
package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.replication.MutationTrackingService.KeyspaceShards;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.assertj.core.api.Assertions.assertThat;

/*
 * These tests are LLM-generated, and were only skimmed by me (AY).
 * Feel free to delete them if they ever get in the way.
 */
public class LegacyShardMigrationTest
{
    private static final String KEYSPACE = "legacy_shard_migration_ks";
    private static final String TABLE = "tbl";

    private static final String SELECT_SHARDS =
        format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS);
    private static final String SELECT_LOGS =
        format("SELECT * FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);
    private static final String INSERT_LEGACY_SHARD =
        format("INSERT INTO %s.%s (keyspace_name, since_epoch, range_start, range_end, participants) VALUES (?, ?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS);
    private static final String INSERT_SHARD =
        format("INSERT INTO %s.%s (keyspace_name, since_epoch, range_start, range_end, participants, state) VALUES (?, ?, ?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SHARDS);
    private static final String INSERT_LOG =
        format("INSERT INTO %s.%s (keyspace_name, since_epoch, range_start, range_end, host_id, host_log_id, participants, witnessed_offsets, persisted_offsets) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.COORDINATOR_LOGS);

    private static final int LEGACY_HOST_LOG_ID = 7;

    private static int localNodeId;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        localNodeId = ClusterMetadata.current().myNodeId().id();
    }

    @Before
    public void clearSystemTables()
    {
        truncate(SystemKeyspace.SHARDS);
        truncate(SystemKeyspace.COORDINATOR_LOGS);
    }

    @Test
    public void migratesLegacyShardsToPlacementEpoch()
    {
        List<ShardRow> expected = currentShards();
        rewriteAsLegacy(expected);

        List<KeyspaceShards> loaded = load();

        assertMigrated(expected, loaded);
    }

    @Test
    public void startupAfterMigrationKeepsOneShardPerRange()
    {
        List<ShardRow> expected = currentShards();
        rewriteAsLegacy(expected);

        MutationTrackingService service = MutationTrackingService.TestAccess.create();
        for (KeyspaceShards keyspaceShards : load())
            keyspaceShards.forEachShard(shard -> {
                if (shard.keyspace.equals(KEYSPACE))
                    MutationTrackingService.TestAccess.setKeyspaceShardsUnsafe(service, KEYSPACE, keyspaceShards);
            });
        MutationTrackingService.TestAccess.onNewClusterMetadata(service, null, ClusterMetadata.current());

        List<Shard> shards = new ArrayList<>();
        MutationTrackingService.TestAccess.getKeyspaceShards(service, KEYSPACE).forEachShard(shards::add);
        assertThat(shards).hasSameSizeAs(expected);
        assertThat(shardRows()).hasSameSizeAs(expected);
    }

    @Test
    public void completesMigrationInterruptedBeforeShardRowWasWritten()
    {
        List<ShardRow> expected = currentShards();
        rewriteAsLegacy(expected);
        for (ShardRow shard : expected)
            CoordinatorLog.migrateLegacyLogs(KEYSPACE, shard.rangeStart, shard.rangeEnd, shard.sinceEpoch);

        assertMigrated(expected, load());
    }

    @Test
    public void completesMigrationInterruptedBeforeLegacyRowsWereDeleted()
    {
        List<ShardRow> expected = currentShards();
        rewriteAsLegacy(expected);
        for (ShardRow shard : expected)
        {
            CoordinatorLog.migrateLegacyLogs(KEYSPACE, shard.rangeStart, shard.rangeEnd, shard.sinceEpoch);
            executeInternal(INSERT_SHARD, KEYSPACE, shard.sinceEpoch, shard.rangeStart, shard.rangeEnd, shard.participants, Shard.State.ACTIVE.name());
        }

        assertMigrated(expected, load());
    }

    @Test
    public void leavesLegacyShardsThatDoNotMatchCurrentPlacements()
    {
        List<ShardRow> expected = currentShards();
        rewriteAsLegacy(expected);
        ShardRow shard = expected.get(0);
        Set<Integer> otherParticipants = Set.of(localNodeId, localNodeId + 100);
        executeInternal(INSERT_LEGACY_SHARD, KEYSPACE, CoordinatorLog.LEGACY_SINCE_EPOCH, shard.rangeStart, shard.rangeEnd, otherParticipants);

        load();

        assertThat(shardRows()).anySatisfy(row -> {
            assertThat(row.sinceEpoch).isEqualTo(CoordinatorLog.LEGACY_SINCE_EPOCH);
            assertThat(row.rangeStart).isEqualTo(shard.rangeStart);
            assertThat(row.rangeEnd).isEqualTo(shard.rangeEnd);
        });
        assertThat(logRows(CoordinatorLog.LEGACY_SINCE_EPOCH)).isNotEmpty();
    }

    private static List<ShardRow> currentShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();
        MutationTrackingService.TestAccess.onNewClusterMetadata(service, null, ClusterMetadata.current());
        List<ShardRow> shards = shardRows();
        assertThat(shards).isNotEmpty();
        assertThat(shards).allSatisfy(shard -> assertThat(shard.sinceEpoch).isGreaterThan(CoordinatorLog.LEGACY_SINCE_EPOCH));
        return shards;
    }

    private static void rewriteAsLegacy(List<ShardRow> shards)
    {
        truncate(SystemKeyspace.SHARDS);
        truncate(SystemKeyspace.COORDINATOR_LOGS);
        for (ShardRow shard : shards)
        {
            executeInternal(INSERT_LEGACY_SHARD, KEYSPACE, CoordinatorLog.LEGACY_SINCE_EPOCH, shard.rangeStart, shard.rangeEnd, shard.participants);
            Map<Integer, List<Integer>> offsets = Map.of(localNodeId, List.of(0, 9));
            executeInternal(INSERT_LOG, KEYSPACE, CoordinatorLog.LEGACY_SINCE_EPOCH, shard.rangeStart, shard.rangeEnd,
                            localNodeId, LEGACY_HOST_LOG_ID, shard.participants, offsets, offsets);
        }
    }

    private static void assertMigrated(List<ShardRow> expected, List<KeyspaceShards> loaded)
    {
        Map<String, Long> expectedEpochs = new HashMap<>();
        for (ShardRow shard : expected)
            expectedEpochs.put(shard.rangeStart + ':' + shard.rangeEnd, shard.sinceEpoch);

        List<Shard> loadedShards = new ArrayList<>();
        for (KeyspaceShards keyspaceShards : loaded)
            keyspaceShards.forEachShard(shard -> {
                if (shard.keyspace.equals(KEYSPACE))
                    loadedShards.add(shard);
            });
        assertThat(loadedShards).hasSameSizeAs(expected);
        for (Shard shard : loadedShards)
            assertThat(shard.sinceEpoch).isEqualTo(expectedEpochs.get(shard.range.left + ":" + shard.range.right));

        assertThat(shardRows()).hasSameSizeAs(expected)
                               .allSatisfy(row -> assertThat(row.sinceEpoch).isEqualTo(expectedEpochs.get(row.rangeStart + ':' + row.rangeEnd)));
        assertThat(logRows(CoordinatorLog.LEGACY_SINCE_EPOCH)).isEmpty();

        for (ShardRow shard : expected)
        {
            List<UntypedResultSet.Row> logs = logRows(shard.sinceEpoch, shard);
            assertThat(logs).hasSize(1);
            UntypedResultSet.Row log = logs.get(0);
            assertThat(log.getInt("host_id")).isEqualTo(localNodeId);
            assertThat(log.getInt("host_log_id")).isEqualTo(LEGACY_HOST_LOG_ID);
            assertThat(log.getMap("witnessed_offsets", Int32Type.instance, ListType.getInstance(Int32Type.instance, false)))
                .isEqualTo(Map.of(localNodeId, List.of(0, 9)));
        }
    }

    private static List<KeyspaceShards> load()
    {
        AtomicInteger hostLogId = new AtomicInteger(1000);
        LongSupplier logIdProvider = () -> CoordinatorLogId.asLong(localNodeId, hostLogId.incrementAndGet());
        return KeyspaceShards.loadFromSystemTables(ClusterMetadata.current(), logIdProvider, (s, l) -> {});
    }

    private static List<ShardRow> shardRows()
    {
        List<ShardRow> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_SHARDS))
            if (row.getString("keyspace_name").equals(KEYSPACE))
                rows.add(new ShardRow(row));
        return rows;
    }

    private static List<UntypedResultSet.Row> logRows(long sinceEpoch)
    {
        return logRows(sinceEpoch, null);
    }

    private static List<UntypedResultSet.Row> logRows(long sinceEpoch, ShardRow shard)
    {
        List<UntypedResultSet.Row> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : executeInternal(SELECT_LOGS))
        {
            if (!row.getString("keyspace_name").equals(KEYSPACE) || row.getLong("since_epoch") != sinceEpoch)
                continue;
            if (shard != null && (!row.getString("range_start").equals(shard.rangeStart) || !row.getString("range_end").equals(shard.rangeEnd)))
                continue;
            rows.add(row);
        }
        return rows;
    }

    private static void truncate(String table)
    {
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(table).truncateBlocking();
    }

    private static final class ShardRow
    {
        final long sinceEpoch;
        final String rangeStart;
        final String rangeEnd;
        final Set<Integer> participants;

        ShardRow(UntypedResultSet.Row row)
        {
            sinceEpoch = row.getLong("since_epoch");
            rangeStart = row.getString("range_start");
            rangeEnd = row.getString("range_end");
            participants = row.getFrozenSet("participants", Int32Type.instance);
        }
    }
}
