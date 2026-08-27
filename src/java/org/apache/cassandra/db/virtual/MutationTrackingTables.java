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

package org.apache.cassandra.db.virtual;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.journal.ActiveSegment;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.replication.CoordinatorLog;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Shard;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.tcm.ClusterMetadata;

public class MutationTrackingTables
{
    public static final String MUTATION_JOURNAL = "mutation_journal";
    public static final String MUTATION_TRACKING_SHARDS = "mutation_tracking_shards";
    public static final String MUTATION_TRACKING_MIGRATION_STATE = "mutation_tracking_migration_state";

    private MutationTrackingTables() {}

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        if (!DatabaseDescriptor.getMutationTrackingEnabled())
            return Collections.emptyList();

        return List.of(new MutationJournalTable(keyspace),
                       new MutationTrackingShardsTable(keyspace),
                       new MutationTrackingMigrationStateTable(keyspace));
    }

    public static final class MutationJournalTable extends AbstractVirtualTable
    {
        private static final String SEGMENT_ID = "segment_id";
        private static final String IS_ACTIVE = "is_active";
        private static final String BYTES_ON_DISK = "bytes_on_disk";
        private static final String RECORDS_COUNT = "records_count";
        private static final String WRITTEN_TO = "written_to";
        private static final String FSYNCED_TO = "fsynced_to";
        private static final String NEEDS_REPLAY = "needs_replay";
        private static final String FILE_PATH = "file_path";
    
        MutationJournalTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, MUTATION_JOURNAL)
                               .comment("mutation journal segments and their contents")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(LongType.instance))
                               .addPartitionKeyColumn(SEGMENT_ID, LongType.instance)
                               .addRegularColumn(IS_ACTIVE, BooleanType.instance)
                               .addRegularColumn(BYTES_ON_DISK, LongType.instance)
                               .addRegularColumn(RECORDS_COUNT, Int32Type.instance)
                               .addRegularColumn(WRITTEN_TO, Int32Type.instance)
                               .addRegularColumn(FSYNCED_TO, Int32Type.instance)
                               .addRegularColumn(NEEDS_REPLAY, BooleanType.instance)
                               .addRegularColumn(FILE_PATH, UTF8Type.instance)
                               .build());
        }
    
        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
    
            for (Segment<ShortMutationId, Mutation> segment : MutationJournal.instance().getAllSegments())
            {
                result.row(segment.id())
                      .column(IS_ACTIVE, segment instanceof ActiveSegment)
                      .column(BYTES_ON_DISK, segment.segmentSizeOnDisk())
                      .column(RECORDS_COUNT, segment.metadata().totalCount())
                      .column(WRITTEN_TO, segment.writtenTo())
                      .column(FSYNCED_TO, segment.fsyncedTo())
                      .column(NEEDS_REPLAY, segment.metadata().needsReplay())
                      .column(FILE_PATH, segment.filePath());
            }
    
            return result;
        }
    }

    public static class MutationTrackingShardsTable extends AbstractVirtualTable
    {
        private static final String KEYSPACE = "keyspace";
        private static final String LOG_ID = "log_id";
        private static final String RANGE_START = "range_start";
        private static final String RANGE_END = "range_end";
        private static final String LOCAL_NODE_ID = "local_node_id";
        private static final String PARTICIPANTS = "participants";
        private static final String WITNESSED_OFFSETS = "witnessed_offsets";
        private static final String RECONCILED_OFFSETS = "reconciled_offsets";
        private static final String PERSISTED_OFFSETS = "persisted_offsets";
    
        MutationTrackingShardsTable(String keyspace) {
            super(TableMetadata.builder(keyspace, MUTATION_TRACKING_SHARDS)
                               .comment("mutation tracking shards and their offset information")
                               .kind(TableMetadata.Kind.VIRTUAL).partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                               .addClusteringColumn(LOG_ID, UTF8Type.instance)
                               .addClusteringColumn(RANGE_START, UTF8Type.instance)
                               .addClusteringColumn(RANGE_END, UTF8Type.instance)
                               .addRegularColumn(LOCAL_NODE_ID, Int32Type.instance)
                               .addRegularColumn(PARTICIPANTS, UTF8Type.instance)
                               .addRegularColumn(WITNESSED_OFFSETS, UTF8Type.instance)
                               .addRegularColumn(RECONCILED_OFFSETS, UTF8Type.instance)
                               .addRegularColumn(PERSISTED_OFFSETS, UTF8Type.instance)
                               .build());
        }
    
        private void addShardRows(Shard shard, SimpleDataSet result)
        {
            Shard.DebugInfo shardDebugInfo = shard.getDebugInfo();
            for (Map.Entry<CoordinatorLogId, CoordinatorLog.DebugInfo> entry : shardDebugInfo.logs.entrySet())
            {
                CoordinatorLogId logId = entry.getKey();
                CoordinatorLog.DebugInfo logDebugInfo = entry.getValue();
                result.row(shardDebugInfo.keyspace,
                           logId.toString(),
                           shardDebugInfo.range.left.toString(),
                           shardDebugInfo.range.right.toString())
                      .column(LOCAL_NODE_ID, shardDebugInfo.localNodeId)
                      .column(PARTICIPANTS, shardDebugInfo.participants.toString())
                      .column(WITNESSED_OFFSETS, logDebugInfo.witnessedOffsets)
                      .column(RECONCILED_OFFSETS, logDebugInfo.reconciledOffsets)
                      .column(PERSISTED_OFFSETS, logDebugInfo.persistedOffsets);
            }
        }
    
        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
    
            for (Shard shard : MutationTrackingService.instance().getShards())
            {
                addShardRows(shard, result);
            }
    
            return result;
        }
    
        @Override
        public DataSet data(DecoratedKey key)
        {
            String keyspaceName = UTF8Type.instance.compose(key.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
    
            for (Shard shard : MutationTrackingService.instance().getShards())
            {
                Shard.DebugInfo debugInfo = shard.getDebugInfo();
                if (!debugInfo.keyspace.equals(keyspaceName))
                    continue;
    
                addShardRows(shard, result);
            }
    
            return result;
        }
    }

    /**
     * Mutation tracking migration progress (held in {@link ClusterMetadata}).
     */
    public static class MutationTrackingMigrationStateTable extends AbstractVirtualTable
    {
        private static final String KEYSPACE_NAME = "keyspace_name";
        private static final String TABLE_NAME = "table_name";
        private static final String TABLE_ID = "table_id";
        private static final String STARTED_AT_EPOCH = "started_at_epoch";
        private static final String PENDING_RANGES = "pending_ranges";
        private static final String MIGRATED_RANGES = "migrated_ranges";

        private static final ListType<String> STRING_LIST_TYPE = ListType.getInstance(UTF8Type.instance, false);

        MutationTrackingMigrationStateTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, MUTATION_TRACKING_MIGRATION_STATE)
                               .comment("ranges still to be repaired for in-progress mutation tracking migrations")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .partitioner(new LocalPartitioner(UTF8Type.instance))
                               .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                               .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                               .addRegularColumn(TABLE_ID, UUIDType.instance)
                               .addRegularColumn(STARTED_AT_EPOCH, LongType.instance)
                               .addRegularColumn(PENDING_RANGES, STRING_LIST_TYPE)
                               .addRegularColumn(MIGRATED_RANGES, STRING_LIST_TYPE)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet result = new SimpleDataSet(metadata());
            ClusterMetadata metadata = ClusterMetadata.current();

            for (KeyspaceMigrationInfo info : metadata.mutationTrackingMigrationState.keyspaceInfo.values())
                addTableRows(metadata, info, result);

            return result;
        }

        @Override
        public DataSet data(DecoratedKey key)
        {
            String keyspaceName = UTF8Type.instance.compose(key.getKey());
            SimpleDataSet result = new SimpleDataSet(metadata());
            ClusterMetadata metadata = ClusterMetadata.current();

            KeyspaceMigrationInfo info = metadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspaceName);
            if (info != null)
                addTableRows(metadata, info, result);

            return result;
        }

        private static void addTableRows(ClusterMetadata metadata, KeyspaceMigrationInfo info, SimpleDataSet result)
        {
            NormalizedRanges<Token> fullRing = KeyspaceMigrationInfo.fullRing();
            for (Map.Entry<TableId, NormalizedRanges<Token>> entry : info.pendingRangesPerTable.entrySet())
            {
                TableId tid = entry.getKey();
                NormalizedRanges<Token> pendingRanges = entry.getValue();

                TableMetadata tm = metadata.schema.getTableMetadata(tid);
                if (tm == null)
                    continue;

                result.row(info.keyspace, tm.name)
                      .column(TABLE_ID, tid.asUUID())
                      .column(STARTED_AT_EPOCH, info.startedAtEpoch.getEpoch())
                      .column(PENDING_RANGES, rangesToStrings(pendingRanges))
                      .column(MIGRATED_RANGES, rangesToStrings(fullRing.subtract(pendingRanges)));
            }
        }

        private static List<String> rangesToStrings(NormalizedRanges<Token> ranges)
        {
            return ranges.stream().map(Range::toString).collect(Collectors.toList());
        }
    }
}
