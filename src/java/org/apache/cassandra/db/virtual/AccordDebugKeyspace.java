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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.impl.DurabilityScheduling;
import accord.impl.progresslog.DefaultProgressLog;
import accord.impl.progresslog.TxnStateKind;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.MaxConflicts;
import accord.local.RedundantBefore;
import accord.local.RejectBefore;
import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordCache;
import org.apache.cassandra.service.accord.AccordCommandStores;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.CommandStoreTxnBlockedGraph;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static accord.utils.async.AsyncChains.getBlockingAndRethrow;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_ACCORD_DEBUG;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class AccordDebugKeyspace extends VirtualKeyspace
{
    public static final String DURABILITY_SCHEDULING = "durability_scheduling";
    public static final String DURABLE_BEFORE = "durable_before";
    public static final String EXECUTOR_CACHE = "executor_cache";
    public static final String MAX_CONFLICTS = "max_conflicts";
    public static final String MIGRATION_STATE = "migration_state";
    public static final String PROGRESS_LOG = "progress_log";
    public static final String REDUNDANT_BEFORE = "redundant_before";
    public static final String REJECT_BEFORE = "reject_before";
    public static final String TXN_BLOCKED_BY = "txn_blocked_by";

    // {table_id, token} or {table_id, +Inf/-Inf}
    private static final TupleType ROUTING_KEY_TYPE = new TupleType(List.of(UUIDType.instance, UTF8Type.instance));
    private static final String ROUTING_KEY_TYPE_STRING = ROUTING_KEY_TYPE.asCQL3Type().toString();

    public static final AccordDebugKeyspace instance = new AccordDebugKeyspace();

    private AccordDebugKeyspace()
    {
        super(VIRTUAL_ACCORD_DEBUG, List.of(
            new DurabilitySchedulingTable(),
            new DurableBeforeTable(),
            new ExecutorCacheTable(),
            new MaxConflictsTable(),
            new MigrationStateTable(),
            new ProgressLogTable(),
            new RedundantBeforeTable(),
            new RejectBeforeTable(),
            new TxnBlockedByTable()
        ));
    }

    // TODO (consider): use a different type for the three timestamps in micros
    public static final class DurabilitySchedulingTable extends AbstractVirtualTable
    {
        private DurabilitySchedulingTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, DURABILITY_SCHEDULING,
                        "Accord per-Range Durability Scheduling State",
                        "CREATE TABLE %s (\n" +
                           format("range_start %s,\n", ROUTING_KEY_TYPE_STRING) +
                           format("range_end %s,\n", ROUTING_KEY_TYPE_STRING) +
                        "  node_offset int,\n" +
                        "  \"index\" int,\n" +
                        "  number_of_splits int,\n" +
                        "  range_started_at bigint,\n" +
                        "  cycle_started_at bigint,\n" +
                        "  retry_delay_micros bigint,\n" +
                        "  is_defunct boolean,\n" +
                        "  PRIMARY KEY ((range_start, range_end))" +
                        ')', CompositeType.getInstance(ROUTING_KEY_TYPE, ROUTING_KEY_TYPE)));
        }

        @Override
        public DataSet data()
        {
            DurabilityScheduling.ImmutableView view = ((AccordService) AccordService.instance()).durabilityScheduling();

            SimpleDataSet ds = new SimpleDataSet(metadata());
            while (view.advance())
            {
                ds.row(decompose(view.range().start()), decompose(view.range().end()))
                  .column("node_offset", view.nodeOffset())
                  .column("index", view.index())
                  .column("number_of_splits", view.numberOfSplits())
                  .column("range_started_at", view.rangeStartedAtMicros())
                  .column("cycle_started_at", view.cycleStartedAtMicros())
                  .column("retry_delay_micros", view.retryDelayMicros())
                  .column("is_defunct", view.isDefunct());
            }
            return ds;
        }
    }

    public static final class DurableBeforeTable extends AbstractVirtualTable
    {
        private DurableBeforeTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, DURABLE_BEFORE,
                        "Accord Node's DurableBefore State",
                        "CREATE TABLE %s (\n" +
                           format("range_start %s,\n", ROUTING_KEY_TYPE_STRING) +
                           format("range_end %s,\n", ROUTING_KEY_TYPE_STRING) +
                        "  majority_before text,\n" +
                        "  universal_before text,\n" +
                        "  PRIMARY KEY ((range_start, range_end))" +
                        ')', CompositeType.getInstance(ROUTING_KEY_TYPE, ROUTING_KEY_TYPE)));
        }

        @Override
        public DataSet data()
        {
            DurableBefore durableBefore = AccordService.instance().node().durableBefore();
            return durableBefore.foldlWithBounds(
                (entry, ds, start, end) -> {
                    ds.row(decompose(start), decompose(end))
                      .column("majority_before", entry.majorityBefore.toString())
                      .column("universal_before", entry.universalBefore.toString());
                    return ds;
                },
                new SimpleDataSet(metadata()),
                ignore -> false
            );
        }
    }

    public static final class ExecutorCacheTable extends AbstractVirtualTable
    {
        private ExecutorCacheTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, EXECUTOR_CACHE,
                        "Accord Executor Cache Metrics",
                        "CREATE TABLE %s (\n" +
                        "  executor_id int,\n" +
                        "  scope text,\n" +
                        "  queries bigint,\n" +
                        "  hits bigint,\n" +
                        "  misses bigint,\n" +
                        "  PRIMARY KEY (executor_id, scope)" +
                        ')', Int32Type.instance));
        }

        @Override
        public DataSet data()
        {
            AccordCommandStores stores = (AccordCommandStores) AccordService.instance().node().commandStores();
            SimpleDataSet ds = new SimpleDataSet(metadata());
            for (AccordExecutor executor : stores.executors())
            {
                try (AccordExecutor.ExclusiveGlobalCaches cache = executor.lockCaches())
                {
                    addRow(ds, executor.executorId(), AccordKeyspace.COMMANDS, cache.commands.statsSnapshot());
                    addRow(ds, executor.executorId(), AccordKeyspace.COMMANDS_FOR_KEY, cache.commandsForKey.statsSnapshot());
                    addRow(ds, executor.executorId(), AccordKeyspace.TIMESTAMPS_FOR_KEY, cache.timestampsForKey.statsSnapshot());
                }
            }
            return ds;
        }

        private static void addRow(SimpleDataSet ds, int executorId, String scope, AccordCache.ImmutableStats stats)
        {
            ds.row(executorId, scope)
              .column("queries", stats.queries)
              .column("hits", stats.hits)
              .column("misses", stats.misses);
        }
    }


    public static final class MaxConflictsTable extends AbstractVirtualTable
    {
        private MaxConflictsTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, MAX_CONFLICTS,
                        "Accord per-CommandStore MaxConflicts State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                           format("range_start %s,\n", ROUTING_KEY_TYPE_STRING) +
                           format("range_end %s,\n", ROUTING_KEY_TYPE_STRING) +
                        "  timestamp text,\n" +
                        "  PRIMARY KEY (command_store_id, range_start, range_end)" +
                        ')', Int32Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores stores = AccordService.instance().node().commandStores();
            List<Pair<Integer, MaxConflicts>> rangeMaps =
                getBlockingAndRethrow(stores.map(store -> Pair.create(store.commandStore().id(), store.commandStore().unsafeGetMaxConflicts())));
            rangeMaps.sort(comparing(p -> p.left));

            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            for (Pair<Integer, MaxConflicts> pair : rangeMaps)
            {
                int storeId = pair.left;
                MaxConflicts maxConflicts = pair.right;

                maxConflicts.foldlWithBounds(
                    (timestamp, ds, start, end) -> ds.row(storeId, decompose(start), decompose(end)).column("timestamp", timestamp.toString()),
                    dataSet,
                    ignore -> false
                );
            }
            return dataSet;
        }
    }

    public static final class MigrationStateTable extends AbstractVirtualTable
    {
        private static final Logger logger = LoggerFactory.getLogger(MigrationStateTable.class);
        
        private MigrationStateTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, MIGRATION_STATE,
                        "Accord Consensus Migration State",
                        "CREATE TABLE %s (\n" +
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  table_id uuid,\n" +
                        "  target_protocol text,\n" +
                        "  transactional_mode text,\n" +
                        "  transactional_migration_from text,\n" +
                        "  migrated_ranges frozen<list<text>>,\n" +
                        "  repair_pending_ranges frozen<list<text>>,\n" +
                        "  migrating_ranges_by_epoch frozen<map<bigint, list<text>>>,\n" +
                        "  PRIMARY KEY (keyspace_name, table_name)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            ConsensusMigrationState snapshot = ClusterMetadata.current().consensusMigrationState;
            Collection<TableMigrationState> tableStates = snapshot.tableStates();
            return data(tableStates);
        }

        @Override
        public DataSet data(DecoratedKey key)
        {
            String keyspaceName = UTF8Type.instance.compose(key.getKey());
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(keyspaceName);

            if (keyspace == null)
                throw new InvalidRequestException("Unknown keyspace: '" + keyspaceName + '\'');

            List<TableId> tableIDs = keyspace.getColumnFamilyStores()
                                             .stream()
                                             .map(ColumnFamilyStore::getTableId)
                                             .collect(Collectors.toList());

            ConsensusMigrationState snapshot = ClusterMetadata.current().consensusMigrationState;
            Collection<TableMigrationState> tableStates = snapshot.tableStatesFor(tableIDs);

            return data(tableStates);
        }

        private SimpleDataSet data(Collection<TableMigrationState> tableStates)
        {
            SimpleDataSet result = new SimpleDataSet(metadata());

            for (TableMigrationState state : tableStates)
            {
                TableMetadata table = Schema.instance.getTableMetadata(state.tableId);

                if (table == null)
                {
                    logger.warn("Table {}.{} (id: {}) no longer exists. It may have been dropped.",
                                state.keyspaceName, state.tableName, state.tableId);
                    continue;
                }

                result.row(state.keyspaceName, state.tableName);
                result.column("table_id", state.tableId.asUUID());
                result.column("target_protocol", state.targetProtocol.toString());
                result.column("transactional_mode", table.params.transactionalMode.toString());
                result.column("transactional_migration_from", table.params.transactionalMode.toString());

                List<String> primitiveMigratedRanges = state.migratedRanges.stream().map(Objects::toString).collect(toImmutableList());
                result.column("migrated_ranges", primitiveMigratedRanges);

                List<String> primitiveRepairPendingRanges = state.repairPendingRanges.stream().map(Objects::toString).collect(toImmutableList());
                result.column("repair_pending_ranges", primitiveRepairPendingRanges);
        
                Map<Long, List<String>> primitiveRangesByEpoch = new LinkedHashMap<>();
                for (Map.Entry<org.apache.cassandra.tcm.Epoch, NormalizedRanges<Token>> entry : state.migratingRangesByEpoch.entrySet())
                    primitiveRangesByEpoch.put(entry.getKey().getEpoch(), entry.getValue().stream().map(Objects::toString).collect(toImmutableList()));

                result.column("migrating_ranges_by_epoch", primitiveRangesByEpoch);
            }

            return result;
        }
    }

    // TODO (desired): human readable packed key tracker (but requires loading Txn, so might be preferable to only do conditionally)
    public static final class ProgressLogTable extends AbstractVirtualTable
    {
        private ProgressLogTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, PROGRESS_LOG,
                        "Accord per-CommandStore ProgressLog State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                        "  txn_id text,\n" +
                        // Timer + BaseTxnState
                        "  contact_everyone boolean,\n" +
                        // WaitingState
                        "  waiting_is_uninitialised boolean,\n" +
                        "  waiting_blocked_until text,\n" +
                        "  waiting_home_satisfies text,\n" +
                        "  waiting_progress text,\n" +
                        "  waiting_retry_counter int,\n" +
                        "  waiting_packed_key_tracker_bits text,\n" +
                        "  waiting_scheduled_at timestamp,\n" +
                        // HomeState/TxnState
                        "  home_phase text,\n" +
                        "  home_progress text,\n" +
                        "  home_retry_counter int,\n" +
                        "  home_scheduled_at timestamp,\n" +
                        "  PRIMARY KEY (command_store_id, txn_id)" +
                        ')', Int32Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores stores = AccordService.instance().node().commandStores();
            List<DefaultProgressLog.ImmutableView> views =
                getBlockingAndRethrow(stores.map(store -> ((DefaultProgressLog) store.progressLog()).immutableView()));
            views.sort(comparing(DefaultProgressLog.ImmutableView::storeId));

            SimpleDataSet ds = new SimpleDataSet(metadata());
            for (int i = 0, size = views.size(); i < size; ++i)
            {
                DefaultProgressLog.ImmutableView view = views.get(i);
                while (view.advance())
                {
                    ds.row(view.storeId(), view.txnId().toString())
                      .column("contact_everyone", view.contactEveryone())
                      .column("waiting_is_uninitialised", view.isWaitingUninitialised())
                      .column("waiting_blocked_until", view.waitingIsBlockedUntil().name())
                      .column("waiting_home_satisfies", view.waitingHomeSatisfies().name())
                      .column("waiting_progress", view.waitingProgress().name())
                      .column("waiting_retry_counter", view.waitingRetryCounter())
                      .column("waiting_packed_key_tracker_bits", Long.toBinaryString(view.waitingPackedKeyTrackerBits()))
                      .column("waiting_scheduled_at", toTimestamp(view.timerScheduledAt(TxnStateKind.Waiting)))
                      .column("home_phase", view.homePhase().name())
                      .column("home_progress", view.homeProgress().name())
                      .column("home_retry_counter", view.homeRetryCounter())
                      .column("home_scheduled_at", toTimestamp(view.timerScheduledAt(TxnStateKind.Home)))
                    ;
                }
            }
            return ds;
        }

        private Date toTimestamp(Long deadline)
        {
            if (deadline == null)
                return null;

            long millisSinceEpoch = approxTime.translate().toMillisSinceEpoch(deadline * 1000L);
            return new Date(millisSinceEpoch);
        }
    }

    public static final class RedundantBeforeTable extends AbstractVirtualTable
    {
        private RedundantBeforeTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, REDUNDANT_BEFORE,
                        "Accord per-CommandStore RedundantBefore State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                           format("range_start %s,\n", ROUTING_KEY_TYPE_STRING) +
                           format("range_end %s,\n", ROUTING_KEY_TYPE_STRING) +
                        "  start_ownership_epoch bigint,\n" +
                        "  end_ownership_epoch bigint,\n" +
                        "  locally_applied_or_invalidated_before text,\n" +
                        "  locally_decided_and_applied_or_invalidated_before text,\n" +
                        "  shard_applied_or_invalidated_before text,\n" +
                        "  gc_before text,\n" +
                        "  shard_only_applied_or_invalidated_before text,\n" +
                        "  bootstrapped_at text,\n" +
                        "  stale_until_at_least text,\n" +
                        "  PRIMARY KEY (command_store_id, range_start, range_end)" +
                        ')', Int32Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores stores = AccordService.instance().node().commandStores();
            List<Pair<Integer, RedundantBefore>> rangeMaps =
                getBlockingAndRethrow(stores.map(store -> Pair.create(store.commandStore().id(), store.commandStore().unsafeGetRedundantBefore())));
            rangeMaps.sort(comparing(p -> p.left));

            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            for (Pair<Integer, RedundantBefore> pair : rangeMaps)
            {
                int storeId = pair.left;
                RedundantBefore redundantBefore = pair.right;

                redundantBefore.foldlWithBounds(
                    (entry, ds, start, end) -> {
                        ds.row(storeId, decompose(start), decompose(end))
                          .column("start_ownership_epoch", entry.startOwnershipEpoch)
                          .column("end_ownership_epoch", entry.endOwnershipEpoch)
                          .column("locally_applied_or_invalidated_before", entry.locallyAppliedOrInvalidatedBefore.toString())
                          .column("locally_decided_and_applied_or_invalidated_before", entry.locallyDecidedAndAppliedOrInvalidatedBefore.toString())
                          .column("shard_applied_or_invalidated_before", entry.shardAppliedOrInvalidatedBefore.toString())
                          .column("gc_before", entry.gcBefore.toString())
                          .column("shard_only_applied_or_invalidated_before", entry.shardOnlyAppliedOrInvalidatedBefore.toString())
                          .column("bootstrapped_at", entry.bootstrappedAt.toString())
                          .column("stale_until_at_least", entry.staleUntilAtLeast != null ? entry.staleUntilAtLeast.toString() : null);
                        return ds;
                    },
                    dataSet,
                    ignore -> false
                );
            }
            return dataSet;
        }
    }

    public static final class RejectBeforeTable extends AbstractVirtualTable
    {
        private RejectBeforeTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, REJECT_BEFORE,
                        "Accord per-CommandStore RejectBefore State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                           format("range_start %s,\n", ROUTING_KEY_TYPE_STRING) +
                           format("range_end %s,\n", ROUTING_KEY_TYPE_STRING) +
                        "  txn_id text,\n" +
                        "  PRIMARY KEY (command_store_id, range_start, range_end)" +
                        ')', Int32Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores stores = AccordService.instance().node().commandStores();
            List<Pair<Integer, RejectBefore>> rangeMaps =
                getBlockingAndRethrow(stores.map(store -> Pair.create(store.commandStore().id(), store.commandStore().unsafeGetRejectBefore())));
            rangeMaps.sort(comparing(p -> p.left));

            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            for (Pair<Integer, RejectBefore> pair : rangeMaps)
            {
                int storeId = pair.left;
                RejectBefore rejectBefore = pair.right;

                if (rejectBefore == null)
                    continue;

                rejectBefore.foldlWithBounds(
                    (txnId, ds, start, end) -> ds.row(storeId, decompose(start), decompose(end)).column("txn_id", txnId.toString()),
                    dataSet,
                    ignore -> false
                );
            }
            return dataSet;
        }
    }

    public static class TxnBlockedByTable extends AbstractVirtualTable
    {
        enum Reason { Self, Txn, Key }

        protected TxnBlockedByTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN_BLOCKED_BY,
                        "Accord Transactions Blocked By Table" ,
                        "CREATE TABLE %s (\n" +
                        "  txn_id text,\n" +
                        "  command_store_id int,\n" +
                        "  depth int,\n" +
                        "  blocked_by text,\n" +
                        "  reason text,\n" +
                        "  save_status text,\n" +
                        "  execute_at text,\n" +
                           format("key %s,\n", ROUTING_KEY_TYPE_STRING) +
                        "  PRIMARY KEY (txn_id, command_store_id, depth, blocked_by, reason)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data(DecoratedKey partitionKey)
        {
            TxnId id = TxnId.parse(UTF8Type.instance.compose(partitionKey.getKey()));
            List<CommandStoreTxnBlockedGraph> shards = AccordService.instance().debugTxnBlockedGraph(id);

            SimpleDataSet ds = new SimpleDataSet(metadata());
            for (CommandStoreTxnBlockedGraph shard : shards)
            {
                Set<TxnId> processed = new HashSet<>();
                process(ds, shard, processed, id, 0, id, Reason.Self, null);
                // everything was processed right?
                if (!shard.txns.isEmpty() && !shard.txns.keySet().containsAll(processed))
                    throw new IllegalStateException("Skipped txns: " + Sets.difference(shard.txns.keySet(), processed));
            }

            return ds;
        }

        private void process(SimpleDataSet ds, CommandStoreTxnBlockedGraph shard, Set<TxnId> processed, TxnId userTxn, int depth, TxnId txnId, Reason reason, Runnable onDone)
        {
            if (!processed.add(txnId))
                throw new IllegalStateException("Double processed " + txnId);
            CommandStoreTxnBlockedGraph.TxnState txn = shard.txns.get(txnId);
            if (txn == null)
            {
                Invariants.checkState(reason == Reason.Self, "Txn %s unknown for reason %s", txnId, reason);
                return;
            }
            // was it applied?  If so ignore it
            if (reason != Reason.Self && txn.saveStatus.hasBeen(Status.Applied))
                return;
            ds.row(userTxn.toString(), shard.storeId, depth, reason == Reason.Self ? "" : txn.txnId.toString(), reason.name());
            ds.column("save_status", txn.saveStatus.name());
            if (txn.executeAt != null)
                ds.column("execute_at", txn.executeAt.toString());
            if (onDone != null)
                onDone.run();
            if (txn.isBlocked())
            {
                for (TxnId blockedBy : txn.blockedBy)
                {
                    if (!processed.contains(blockedBy))
                        process(ds, shard, processed, userTxn, depth + 1, blockedBy, Reason.Txn, null);
                }

                for (TokenKey blockedBy : txn.blockedByKey)
                {
                    TxnId blocking = shard.keys.get(blockedBy);
                    if (!processed.contains(blocking))
                        process(ds, shard, processed, userTxn, depth + 1, blocking, Reason.Key, () -> ds.column("key", decompose(blockedBy)));
                }
            }
        }

        @Override
        public DataSet data()
        {
            throw new InvalidRequestException("Must select a single txn_id");
        }
    }

    private static ByteBuffer decompose(RoutingKey routingKey)
    {
        AccordRoutingKey key = (AccordRoutingKey) routingKey;
        switch (key.kindOfRoutingKey())
        {
            case SENTINEL:
            case TOKEN:
                return ROUTING_KEY_TYPE.pack(UUIDType.instance.decompose(key.table().asUUID()), bytes(key.suffix()));
            default:
                throw new IllegalStateException("Unhandled key Kind " + key.kindOfRoutingKey());
        }
    }

    private static TableMetadata parse(String keyspace, String table, String comment, String schema, AbstractType<?> partitionKeyType)
    {
        return CreateTableStatement.parse(format(schema, table), keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(partitionKeyType))
                                   .build();
    }
}
