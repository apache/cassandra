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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.TraceEventType;
import accord.coordinate.FetchData;
import accord.coordinate.FetchRoute;
import accord.coordinate.MaybeRecover;
import accord.coordinate.RecoverWithRoute;
import accord.impl.CommandChange;
import accord.impl.progresslog.DefaultProgressLog;
import accord.impl.progresslog.TxnStateKind;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.Commands;
import accord.local.DurableBefore;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.MaxConflicts;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.RejectBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.durability.ShardDurability;
import accord.primitives.FullRoute;
import accord.primitives.Known;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordCache;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandStores;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.AccordJournal;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTracing;
import org.apache.cassandra.service.accord.CommandStoreTxnBlockedGraph;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.LocalizeString;

import static accord.api.TraceEventType.RECOVER;
import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.utils.async.AsyncChains.getBlockingAndRethrow;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_ACCORD_DEBUG;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class AccordDebugKeyspace extends VirtualKeyspace
{
    public static final String COMMANDS_FOR_KEY   = "commands_for_key";
    public static final String COMMANDS_FOR_KEY_UNMANAGED = "commands_for_key_unmanaged";
    public static final String DURABILITY_SERVICE = "durability_service";
    public static final String DURABLE_BEFORE     = "durable_before";
    public static final String EXECUTOR_CACHE     = "executor_cache";
    public static final String JOURNAL            = "journal";
    public static final String MAX_CONFLICTS      = "max_conflicts";
    public static final String MIGRATION_STATE    = "migration_state";
    public static final String PROGRESS_LOG       = "progress_log";
    public static final String REDUNDANT_BEFORE   = "redundant_before";
    public static final String REJECT_BEFORE      = "reject_before";
    public static final String TXN                = "txn";
    public static final String TXN_BLOCKED_BY     = "txn_blocked_by";
    public static final String TXN_TRACE          = "txn_trace";
    public static final String TXN_TRACES         = "txn_traces";
    public static final String TXN_OPS            = "txn_ops";

    public static final AccordDebugKeyspace instance = new AccordDebugKeyspace();

    private AccordDebugKeyspace()
    {
        super(VIRTUAL_ACCORD_DEBUG, List.of(
            new CommandsForKeyTable(),
            new CommandsForKeyUnmanagedTable(),
            new DurabilityServiceTable(),
            new DurableBeforeTable(),
            new ExecutorCacheTable(),
            new JournalTable(),
            new MaxConflictsTable(),
            new MigrationStateTable(),
            new ProgressLogTable(),
            new RedundantBeforeTable(),
            new RejectBeforeTable(),
            new TxnBlockedByTable(),
            new TxnTable(),
            new TxnTraceTable(),
            new TxnTracesTable(),
            new TxnOpsTable()
        ));
    }

    // TODO (desired): don't report null as "null"
    public static final class CommandsForKeyTable extends AbstractVirtualTable implements AbstractVirtualTable.DataSet
    {
        static class Entry
        {
            final int commandStoreId;
            final CommandsForKey cfk;

            Entry(int commandStoreId, CommandsForKey cfk)
            {
                this.commandStoreId = commandStoreId;
                this.cfk = cfk;
            }
        }
        private CommandsForKeyTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, COMMANDS_FOR_KEY,
                        "Accord per-CommandStore CommandsForKey Managed Transaction State",
                        "CREATE TABLE %s (\n" +
                        "  key text,\n" +
                        "  command_store_id int,\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  ballot text,\n" +
                        "  deps_known_before text,\n" +
                        "  execute_at text,\n" +
                        "  flags text,\n" +
                        "  missing text,\n" +
                        "  status text,\n" +
                        "  status_overrides text,\n" +
                        "  PRIMARY KEY (key, command_store_id, txn_id)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            return this;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public Partition getPartition(DecoratedKey partitionKey)
        {
            String keyStr = UTF8Type.instance.compose(partitionKey.getKey());
            TokenKey key = TokenKey.parse(keyStr, DatabaseDescriptor.getPartitioner());

            List<Entry> cfks = new CopyOnWriteArrayList<>();
            PreLoadContext context = PreLoadContext.contextFor(key, LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "commands_for_key table query");
            CommandStores commandStores = AccordService.instance().node().commandStores();
            getBlockingAndRethrow(commandStores.forEach(context, key, Long.MIN_VALUE, Long.MAX_VALUE, safeStore -> {
                SafeCommandsForKey safeCfk = safeStore.get(key);
                CommandsForKey cfk = safeCfk.current();
                if (cfk == null)
                    return;

                cfks.add(new Entry(safeStore.commandStore().id(), cfk));
            }).beginAsResult());

            if (cfks.isEmpty())
                return null;

            SimpleDataSet ds = new SimpleDataSet(metadata);
            for (Entry e : cfks)
            {
                CommandsForKey cfk = e.cfk;
                for (int i = 0 ; i < cfk.size() ; ++i)
                {
                    CommandsForKey.TxnInfo txn = cfk.get(i);
                    ds.row(keyStr, e.commandStoreId, toStringOrNull(txn.plainTxnId()))
                      .column("ballot", toStringOrNull(txn.ballot()))
                      .column("deps_known_before", toStringOrNull(txn.depsKnownUntilExecuteAt()))
                      .column("flags", flags(txn))
                      .column("execute_at", toStringOrNull(txn.plainExecuteAt()))
                      .column("missing", Arrays.toString(txn.missing()))
                      .column("status", toStringOrNull(txn.status()))
                      .column("status_overrides", txn.statusOverrides() == 0 ? null : ("0x" + Integer.toHexString(txn.statusOverrides())));
                }
            }

            return ds.getPartition(partitionKey);
        }

        @Override
        public Iterator<Partition> getPartitions(DataRange range)
        {
            throw new UnsupportedOperationException();
        }

        private static String flags(CommandsForKey.TxnInfo txn)
        {
            StringBuilder sb = new StringBuilder();
            if (!txn.mayExecute())
            {
                sb.append("NO EXECUTE");
            }
            if (txn.hasNotifiedReady())
            {
                if (sb.length() > 0) sb.append(", ");
                sb.append("NOTIFIED READY");
            }
            if (txn.hasNotifiedWaiting())
            {
                if (sb.length() > 0) sb.append(", ");
                sb.append("NOTIFIED WAITING");
            }
            return sb.toString();
        }
    }


    // TODO (expected): test this table
    public static final class CommandsForKeyUnmanagedTable extends AbstractVirtualTable implements AbstractVirtualTable.DataSet
    {
        static class Entry
        {
            final int commandStoreId;
            final CommandsForKey cfk;

            Entry(int commandStoreId, CommandsForKey cfk)
            {
                this.commandStoreId = commandStoreId;
                this.cfk = cfk;
            }
        }
        private CommandsForKeyUnmanagedTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, COMMANDS_FOR_KEY_UNMANAGED,
                        "Accord per-CommandStore CommandsForKey Unmanaged Transaction State",
                        "CREATE TABLE %s (\n" +
                        "  key text,\n" +
                        "  command_store_id int,\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  waiting_until text,\n" +
                        "  waiting_until_status text,\n" +
                        "  PRIMARY KEY (key, command_store_id, txn_id)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            return this;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public Partition getPartition(DecoratedKey partitionKey)
        {
            String keyStr = UTF8Type.instance.compose(partitionKey.getKey());
            TokenKey key = TokenKey.parse(keyStr, DatabaseDescriptor.getPartitioner());

            List<Entry> cfks = new CopyOnWriteArrayList<>();
            PreLoadContext context = PreLoadContext.contextFor(key, LoadKeys.SYNC, LoadKeysFor.READ_WRITE, "commands_for_key_unmanaged table query");
            CommandStores commandStores = AccordService.instance().node().commandStores();
            getBlockingAndRethrow(commandStores.forEach(context, key, Long.MIN_VALUE, Long.MAX_VALUE, safeStore -> {
                SafeCommandsForKey safeCfk = safeStore.get(key);
                CommandsForKey cfk = safeCfk.current();
                if (cfk == null)
                    return;

                cfks.add(new Entry(safeStore.commandStore().id(), cfk));
            }));

            if (cfks.isEmpty())
                return null;

            SimpleDataSet ds = new SimpleDataSet(metadata);
            for (Entry e : cfks)
            {
                CommandsForKey cfk = e.cfk;
                for (int i = 0 ; i < cfk.unmanagedCount() ; ++i)
                {
                    CommandsForKey.Unmanaged txn = cfk.getUnmanaged(i);
                    ds.row(keyStr, e.commandStoreId, toStringOrNull(txn.txnId))
                      .column("waiting_until", toStringOrNull(txn.waitingUntil))
                      .column("waiting_until_status", toStringOrNull(txn.pending));
                }
            }

            return ds.getPartition(partitionKey);
        }

        @Override
        public Iterator<Partition> getPartitions(DataRange range)
        {
            throw new UnsupportedOperationException();
        }
    }


    public static final class DurabilityServiceTable extends AbstractVirtualTable
    {
        private DurabilityServiceTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, DURABILITY_SERVICE,
                        "Accord per-Range Durability Service State",
                        "CREATE TABLE %s (\n" +
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  last_started_at bigint,\n" +
                        "  cycle_started_at bigint,\n" +
                        "  retries int,\n" +
                        "  min text,\n" +
                        "  requested_by text,\n" +
                        "  active text,\n" +
                        "  waiting text,\n" +
                        "  node_offset int,\n" +
                        "  cycle_offset int,\n" +
                        "  active_index int,\n" +
                        "  next_index int,\n" +
                        "  next_to_index int,\n" +
                        "  end_index int,\n" +
                        "  current_splits int,\n" +
                        "  stopping boolean,\n" +
                        "  stopped boolean,\n" +
                        "  PRIMARY KEY (keyspace_name, table_name, token_start)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            ShardDurability.ImmutableView view = ((AccordService) AccordService.instance()).shardDurability();

            SimpleDataSet ds = new SimpleDataSet(metadata());
            while (view.advance())
            {
                TableId tableId = (TableId) view.shard().range.start().prefix();
                TableMetadata tableMetadata = tableMetadata(tableId);
                ds.row(keyspace(tableMetadata), table(tableId, tableMetadata), printToken(view.shard().range.start()))
                  .column("token_end", printToken(view.shard().range.end()))
                  .column("last_started_at", approxTime.translate().toMillisSinceEpoch(view.lastStartedAtMicros() * 1000))
                  .column("cycle_started_at", approxTime.translate().toMillisSinceEpoch(view.cycleStartedAtMicros() * 1000))
                  .column("retries", view.retries())
                  .column("min", Objects.toString(view.min()))
                  .column("requested_by", Objects.toString(view.requestedBy()))
                  .column("active", Objects.toString(view.active()))
                  .column("waiting", Objects.toString(view.waiting()))
                  .column("node_offset", view.nodeOffset())
                  .column("cycle_offset", view.cycleOffset())
                  .column("active_index", view.activeIndex())
                  .column("next_index", view.nextIndex())
                  .column("next_to_index", view.toIndex())
                  .column("end_index", view.cycleLength())
                  .column("current_splits", view.currentSplits())
                  .column("stopping", view.stopping())
                  .column("stopped", view.stopped())
                ;
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
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  quorum 'TxnIdUtf8Type',\n" +
                        "  universal 'TxnIdUtf8Type',\n" +
                        "  PRIMARY KEY (keyspace_name, table_name, token_start)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            DurableBefore durableBefore = AccordService.instance().node().durableBefore();
            return durableBefore.foldlWithBounds(
                (entry, ds, start, end) -> {
                    TableId tableId = (TableId) start.prefix();
                    TableMetadata tableMetadata = tableMetadata(tableId);
                    ds.row(keyspace(tableMetadata), table(tableId, tableMetadata), printToken(start))
                      .column("token_end", printToken(end))
                      .column("quorum", entry.quorumBefore.toString())
                      .column("universal", entry.universalBefore.toString());
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
                    addRow(ds, executor.executorId(), "commands", cache.commands.statsSnapshot());
                    addRow(ds, executor.executorId(), AccordKeyspace.COMMANDS_FOR_KEY, cache.commandsForKey.statsSnapshot());
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
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  command_store_id bigint,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  timestamp text,\n" +
                        "  PRIMARY KEY (keyspace_name, table_name, command_store_id, token_start)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();

            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            for (CommandStore commandStore : commandStores.all())
            {
                int commandStoreId = commandStore.id();
                MaxConflicts maxConflicts = commandStore.unsafeGetMaxConflicts();
                TableId tableId = ((AccordCommandStore) commandStore).tableId();
                TableMetadata tableMetadata = tableMetadata(tableId);

                maxConflicts.foldlWithBounds(
                    (timestamp, ds, start, end) -> {
                        return ds.row(keyspace(tableMetadata), table(tableId, tableMetadata), commandStoreId, printToken(start))
                                 .column("token_end", printToken(end))
                                 .column("timestamp", timestamp.toString())
                        ;
                    },
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
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  table_id text,\n" +
                        "  command_store_id int,\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
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
                        "  PRIMARY KEY (keyspace_name, table_name, table_id, command_store_id, txn_id)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();
            SimpleDataSet ds = new SimpleDataSet(metadata());
            for (CommandStore commandStore : commandStores.all())
            {
                DefaultProgressLog.ImmutableView view = ((DefaultProgressLog) commandStore.unsafeProgressLog()).immutableView();
                TableId tableId = ((AccordCommandStore)commandStore).tableId();
                String tableIdStr = tableId.toString();
                TableMetadata tableMetadata = tableMetadata(tableId);
                while (view.advance())
                {
                    ds.row(keyspace(tableMetadata), table(tableId, tableMetadata), tableIdStr, view.commandStoreId(), view.txnId().toString())
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
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  table_id text,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  command_store_id int,\n" +
                        "  start_epoch bigint,\n" +
                        "  end_epoch bigint,\n" +
                        "  gc_before 'TxnIdUtf8Type',\n" +
                        "  shard_applied 'TxnIdUtf8Type',\n" +
                        "  quorum_applied 'TxnIdUtf8Type',\n" +
                        "  locally_applied 'TxnIdUtf8Type',\n" +
                        "  locally_durable_to_command_store 'TxnIdUtf8Type',\n" +
                        "  locally_durable_to_data_store 'TxnIdUtf8Type',\n" +
                        "  locally_redundant 'TxnIdUtf8Type',\n" +
                        "  locally_synced 'TxnIdUtf8Type',\n" +
                        "  locally_witnessed 'TxnIdUtf8Type',\n" +
                        "  pre_bootstrap 'TxnIdUtf8Type',\n" +
                        "  stale_until_at_least 'TxnIdUtf8Type',\n" +
                        "  PRIMARY KEY (keyspace_name, table_name, table_id, command_store_id, token_start)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();

            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            for (CommandStore commandStore : commandStores.all())
            {
                int commandStoreId = commandStore.id();
                TableId tableId = ((AccordCommandStore)commandStore).tableId();
                String tableIdStr = tableId.toString();
                TableMetadata tableMetadata = tableMetadata(tableId);
                String keyspace = keyspace(tableMetadata);
                String table = table(tableId, tableMetadata);
                commandStore.unsafeGetRedundantBefore().foldl(
                    (entry, ds) -> {
                        ds.row(keyspace, table, tableIdStr, commandStoreId, printToken(entry.range.start()))
                          .column("token_end", printToken(entry.range.end()))
                          .column("start_epoch", entry.startEpoch)
                          .column("end_epoch", entry.endEpoch)
                          .column("gc_before", entry.maxBound(GC_BEFORE).toString())
                          .column("shard_applied", entry.maxBound(SHARD_APPLIED).toString())
                          .column("quorum_applied", entry.maxBound(QUORUM_APPLIED).toString())
                          .column("locally_applied", entry.maxBound(LOCALLY_APPLIED).toString())
                          .column("locally_durable_to_command_store", entry.maxBound(LOCALLY_DURABLE_TO_COMMAND_STORE).toString())
                          .column("locally_durable_to_data_store", entry.maxBound(LOCALLY_DURABLE_TO_DATA_STORE).toString())
                          .column("locally_redundant", entry.maxBound(LOCALLY_REDUNDANT).toString())
                          .column("locally_synced", entry.maxBound(LOCALLY_SYNCED).toString())
                          .column("locally_witnessed", entry.maxBound(LOCALLY_WITNESSED).toString())
                          .column("pre_bootstrap", entry.maxBound(PRE_BOOTSTRAP).toString())
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
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  table_id text,\n" +
                        "  command_store_id int,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  timestamp text,\n" +
                        "  PRIMARY KEY (keyspace_name, table_name, table_id, command_store_id, token_start)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();
            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            for (CommandStore commandStore : commandStores.all())
            {
                RejectBefore rejectBefore = commandStore.unsafeGetRejectBefore();
                if (rejectBefore == null)
                    continue;

                TableId tableId = ((AccordCommandStore)commandStore).tableId();
                String tableIdStr = tableId.toString();
                TableMetadata tableMetadata = tableMetadata(tableId);
                String keyspace = keyspace(tableMetadata);
                String table = table(tableId, tableMetadata);
                rejectBefore.foldlWithBounds(
                    (timestamp, ds, start, end) -> ds.row(keyspace, table, tableIdStr, commandStore.id(), printToken(start))
                                                 .column("token_end", printToken(end))
                                                 .column("timestamp", timestamp.toString())
                ,
                    dataSet,
                    ignore -> false
                );
            }
            return dataSet;
        }
    }

    /**
     * Usage:
     * collect N events (may be more than N messages)
     * UPDATE system_accord_debug.txn_trace SET permits = N WHERE txn_id = ? AND event_type = ?
     * SELECT * FROM system_accord_debug.txn_traces WHERE txn_id = ? AND event_type = ?
     */
    public static final class TxnTraceTable extends AbstractMutableVirtualTable
    {
        private TxnTraceTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN_TRACE,
                        "Accord Transaction Trace Configuration",
                        "CREATE TABLE %s (\n" +
                        "  txn_id text,\n" +
                        "  event_type text,\n" +
                        "  permits int,\n" +
                        "  PRIMARY KEY (txn_id, event_type)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            AccordTracing tracing = tracing();
            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            tracing.forEach(id -> true, (txnId, eventType, permits, events) -> {
                dataSet.row(txnId.toString(), eventType.toString()).column("permits", permits);
            });
            return dataSet;
        }

        private AccordTracing tracing()
        {
            return ((AccordAgent)AccordService.instance().agent()).tracing();
        }

        @Override
        protected void applyPartitionDeletion(ColumnValues partitionKey)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            tracing().erasePermits(txnId);
        }

        @Override
        protected void applyRowDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            tracing().erasePermits(txnId, parseEventType(clusteringColumns.value(0)));
        }

        @Override
        protected void applyColumnDeletion(ColumnValues partitionKey, ColumnValues clusteringColumns, String columnName)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            TraceEventType eventType = parseEventType(clusteringColumns.value(0));
            tracing().erasePermits(txnId, eventType);
        }

        @Override
        protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            TraceEventType eventType = parseEventType(clusteringColumns.value(0));
            if (columnValue.isEmpty()) tracing().erasePermits(txnId, eventType);
            else tracing().setPermits(txnId, eventType, columnValue.get().value());
        }

        @Override
        public void truncate()
        {
            tracing().eraseAllEvents();
        }
    }

    public static final class TxnTracesTable extends AbstractMutableVirtualTable
    {
        private TxnTracesTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN_TRACES,
                        "Accord Transaction Traces",
                        "CREATE TABLE %s (\n" +
                        "  txn_id text,\n" +
                        "  event_type text,\n" +
                        "  id_micros bigint,\n" +
                        "  at_micros bigint,\n" +
                        "  command_store_id int,\n" +
                        "  message text,\n" +
                        "  PRIMARY KEY (txn_id, event_type, id_micros, at_micros)" +
                        ')', UTF8Type.instance));
        }

        private AccordTracing tracing()
        {
            return ((AccordAgent)AccordService.instance().agent()).tracing();
        }

        @Override
        protected void applyPartitionDeletion(ColumnValues partitionKey)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            tracing().eraseEvents(txnId);
        }

        @Override
        protected void applyRangeTombstone(ColumnValues partitionKey, Range<ColumnValues> range)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            if (!range.hasLowerBound() || range.lowerBoundType() != BoundType.CLOSED) throw invalidRequest("May restrict deletion by at most one event_type");
            if (range.lowerEndpoint().size() != 1) throw invalidRequest("Deletion restricted by lower bound on id_micros or at_micros is unsupported");
            if (!range.hasUpperBound() || (range.upperBoundType() != BoundType.CLOSED && range.upperEndpoint().size() == 1)) throw invalidRequest("Range deletion must specify one event_type");
            if (!range.upperEndpoint().value(0).equals(range.lowerEndpoint().value(0))) throw invalidRequest("May restrict deletion by at most one event_type");
            if (range.upperEndpoint().size() > 2) throw invalidRequest("Deletion restricted by upper bound on at_micros is unsupported");
            TraceEventType eventType = parseEventType(range.lowerEndpoint().value(0));
            if (range.upperEndpoint().size() == 1)
            {
                tracing().eraseEvents(txnId, eventType);
            }
            else
            {
                long before = range.upperEndpoint().value(1);
                tracing().eraseEventsBefore(txnId, eventType, before);
            }
        }

        @Override
        public void truncate()
        {
            tracing().eraseAllEvents();
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet dataSet = new SimpleDataSet(metadata());
            tracing().forEach(id -> true, (txnId, eventType, permits, events) -> {
                events.forEach(e -> {
                    e.messages().forEach(m -> {
                        dataSet.row(txnId.toString(), eventType.name(), e.idMicros, NANOSECONDS.toMicros(m.atNanos - e.atNanos))
                               .column("command_store_id", m.commandStoreId)
                               .column("message", m.message);
                    });
                });
            });
            return dataSet;
        }
    }

    // TODO (desired): don't report null as "null"
    public static final class TxnTable extends AbstractVirtualTable implements AbstractVirtualTable.DataSet
    {
        static class Entry
        {
            final int commandStoreId;
            final Command command;

            Entry(int commandStoreId, Command command)
            {
                this.commandStoreId = commandStoreId;
                this.command = command;
            }
        }
        private TxnTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN,
                        "Accord per-CommandStore Transaction State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                        "  txn_id text,\n" +
                        "  save_status text,\n" +
                        "  route text,\n" +
                        "  durability text,\n" +
                        "  execute_at text,\n" +
                        "  executes_at_least text,\n" +
                        "  txn text,\n" +
                        "  deps text,\n" +
                        "  waiting_on text,\n" +
                        "  writes text,\n" +
                        "  result text,\n" +
                        "  participants_owns text,\n" +
                        "  participants_touches text,\n" +
                        "  participants_has_touched text,\n" +
                        "  participants_executes text,\n" +
                        "  participants_waits_on text,\n" +
                        "  PRIMARY KEY (txn_id, command_store_id)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            return this;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public Partition getPartition(DecoratedKey partitionKey)
        {
            String txnIdStr = UTF8Type.instance.compose(partitionKey.getKey());
            TxnId txnId = TxnId.parse(txnIdStr);

            List<Entry> commands = new CopyOnWriteArrayList<>();
            AccordService.instance().node().commandStores().forEachCommandStore(store -> {
                Command command = ((AccordCommandStore)store).loadCommand(txnId);
                if (command != null)
                    commands.add(new Entry(store.id(), command));
            });

            if (commands.isEmpty())
                return null;

            SimpleDataSet ds = new SimpleDataSet(metadata);
            for (Entry e : commands)
            {
                Command command = e.command;
                ds.row(txnIdStr, e.commandStoreId)
                  .column("save_status", toStringOrNull(command.saveStatus()))
                  .column("route", toStringOrNull(command.route()))
                  .column("participants_owns", toStr(command, StoreParticipants::owns, StoreParticipants::stillOwns))
                  .column("participants_touches", toStr(command, StoreParticipants::touches, StoreParticipants::stillTouches))
                  .column("participants_has_touched", toStringOrNull(command.participants().hasTouched()))
                  .column("participants_executes", toStr(command, StoreParticipants::executes, StoreParticipants::stillExecutes))
                  .column("participants_waits_on", toStr(command, StoreParticipants::waitsOn, StoreParticipants::stillWaitsOn))
                  .column("durability", toStringOrNull(command.durability()))
                  .column("execute_at", toStringOrNull(command.executeAt()))
                  .column("executes_at_least", toStringOrNull(command.executesAtLeast()))
                  .column("txn", toStringOrNull(command.partialTxn()))
                  .column("deps", toStringOrNull(command.partialDeps()))
                  .column("waiting_on", toStringOrNull(command.waitingOn()))
                  .column("writes", toStringOrNull(command.writes()))
                  .column("result", toStringOrNull(command.result()));
            }

            return ds.getPartition(partitionKey);
        }

        @Override
        public Iterator<Partition> getPartitions(DataRange range)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static final class JournalTable extends AbstractVirtualTable implements AbstractVirtualTable.DataSet
    {
        static class Entry
        {
            final int commandStoreId;
            final long segment;
            final int position;
            final CommandChange.Builder builder;

            Entry(int commandStoreId, long segment, int position, CommandChange.Builder builder)
            {
                this.commandStoreId = commandStoreId;
                this.segment = segment;
                this.position = position;
                this.builder = builder;
            }
        }

        private JournalTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, JOURNAL,
                        "Accord per-CommandStore Journal State",
                        "CREATE TABLE %s (\n" +
                        "  txn_id text,\n" +
                        "  command_store_id int,\n" +
                        "  segment bigint,\n" +
                        "  segment_position int,\n" +
                        "  save_status text,\n" +
                        "  route text,\n" +
                        "  durability text,\n" +
                        "  execute_at text,\n" +
                        "  executes_at_least text,\n" +
                        "  txn text,\n" +
                        "  deps text,\n" +
                        "  writes text,\n" +
                        "  result text,\n" +
                        "  participants_owns text,\n" +
                        "  participants_touches text,\n" +
                        "  participants_has_touched text,\n" +
                        "  participants_executes text,\n" +
                        "  participants_waits_on text,\n" +
                        "  PRIMARY KEY (txn_id, command_store_id, segment, segment_position)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            return this;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public Partition getPartition(DecoratedKey partitionKey)
        {
            String txnIdStr = UTF8Type.instance.compose(partitionKey.getKey());
            TxnId txnId = TxnId.parse(txnIdStr);

            List<Entry> entries = new ArrayList<>();
            AccordService.instance().node().commandStores().forEachCommandStore(store -> {
                for (AccordJournal.DebugEntry e : ((AccordCommandStore)store).debugCommand(txnId))
                    entries.add(new Entry(store.id(), e.segment, e.position, e.builder));
            });

            if (entries.isEmpty())
                return null;

            SimpleDataSet ds = new SimpleDataSet(metadata);
            for (Entry e : entries)
            {
                CommandChange.Builder b = e.builder;
                StoreParticipants participants = b.participants();
                if (participants == null) participants = StoreParticipants.empty(txnId);
                ds.row(txnIdStr, e.commandStoreId, e.segment, e.position)
                  .column("save_status", toStringOrNull(b.saveStatus()))
                  .column("route", toStringOrNull(participants.route()))
                  .column("participants_owns", toStr(participants, StoreParticipants::owns, StoreParticipants::stillOwns))
                  .column("participants_touches", toStr(participants, StoreParticipants::touches, StoreParticipants::stillTouches))
                  .column("participants_has_touched", toStringOrNull(participants.hasTouched()))
                  .column("participants_executes", toStr(participants, StoreParticipants::executes, StoreParticipants::stillExecutes))
                  .column("participants_waits_on", toStr(participants, StoreParticipants::waitsOn, StoreParticipants::stillWaitsOn))
                  .column("durability", toStringOrNull(b.durability()))
                  .column("execute_at", toStringOrNull(b.executeAt()))
                  .column("executes_at_least", toStringOrNull(b.executesAtLeast()))
                  .column("txn", toStringOrNull(b.partialTxn()))
                  .column("deps", toStringOrNull(b.partialDeps()))
                  .column("writes", toStringOrNull(b.writes()))
                  .column("result", toStringOrNull(b.result()));
            }

            return ds.getPartition(partitionKey);
        }

        @Override
        public Iterator<Partition> getPartitions(DataRange range)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Write-only virtual table for updating Accord command cleanup states, both in CommandStore and persist the change in Journal.
     *
     * Use nonnegative command_store_id to update a command on a specific store.
     *
     * Example queries:
     *
     *    UPDATE system_virtual_schema.accord_debug_txn_update SET cleanup = 'TRUNCATE' WHERE txn_id = '[11,1751902116570000,146(KW),1]' AND command_store_id = 5;
     *
     */
    // Had to be separate from the "regular" journal table since it does not have segment and position, and command store id is inferred
    // TODO (required): add access control
    public static final class TxnOpsTable extends AbstractMutableVirtualTable implements AbstractVirtualTable.DataSet
    {
        // TODO (expected): test each of these operations
        enum Op { ERASE_VESTIGIAL, INVALIDATE, TRY_EXECUTE, FORCE_APPLY, FORCE_UPDATE, RECOVER, FETCH, RESET_PROGRESS_LOG }
        private TxnOpsTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN_OPS,
                        "Update Accord Command State",
                        "CREATE TABLE %s (\n" +
                        "  txn_id text,\n" +
                        "  command_store_id int,\n" +
                        "  op text," +
                        "  PRIMARY KEY (txn_id, command_store_id)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public DataSet data()
        {
            throw new UnsupportedOperationException(TXN_OPS + " is a write-only table");
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public Partition getPartition(DecoratedKey partitionKey)
        {
            throw new UnsupportedOperationException(TXN_OPS + " is a write-only table");
        }

        @Override
        public Iterator<Partition> getPartitions(DataRange range)
        {
            throw new UnsupportedOperationException(TXN_OPS + " is a write-only table");
        }


        @Override
        protected void applyColumnUpdate(ColumnValues partitionKey, ColumnValues clusteringColumns, Optional<ColumnValue> columnValue)
        {
            TxnId txnId = TxnId.parse(partitionKey.value(0));
            int commandStoreId = clusteringColumns.value(0);
            Invariants.require(columnValue.isPresent());
            Op op = Op.valueOf(columnValue.get().value());
            switch (op)
            {
                default: throw new UnhandledEnum(op);
                case ERASE_VESTIGIAL:
                    cleanup(txnId, commandStoreId, Cleanup.VESTIGIAL);
                    break;
                case INVALIDATE:
                    cleanup(txnId, commandStoreId, Cleanup.INVALIDATE);
                    break;
                case TRY_EXECUTE:
                    run(txnId, commandStoreId, safeStore -> {
                        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                        Commands.maybeExecute(safeStore, safeCommand, true, true);
                        return AsyncChains.success(null);
                    });
                    break;
                case FORCE_UPDATE:
                    run(txnId, commandStoreId, safeStore -> {
                        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                        safeCommand.update(safeStore, safeCommand.current(), true);
                        return AsyncChains.success(null);
                    });
                    break;
                case FORCE_APPLY:
                    run(txnId, commandStoreId, safeStore -> {
                        SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                        Command command = safeCommand.current();
                        // TODO (expected): we can call applyChain with TruncatedApplyWithOutcome in theory, but the type signature prevents it
                        if (command.saveStatus().compareTo(SaveStatus.PreApplied) < 0 || command.saveStatus().compareTo(SaveStatus.TruncatedApplyWithOutcome) >= 0)
                            throw new UnsupportedOperationException("Cannot apply a transaction with saveStatus " + command.saveStatus());
                        return Commands.applyChain(safeStore, (Command.Executed) command);
                    });
                    break;
                case FETCH:
                    runWithRoute(txnId, commandStoreId, command -> {
                        Timestamp executeAt = command.executeAtIfKnown();
                        return (route, result) -> fetch(txnId, executeAt, route, result);
                    });
                    break;
                case RECOVER:
                    runWithRoute(txnId, commandStoreId, command -> (route, result) -> {
                        recover(txnId, route, result);
                    });
                    break;
                case RESET_PROGRESS_LOG:
                    run(txnId, commandStoreId, safeStore -> {
                        ((DefaultProgressLog)safeStore.progressLog()).requeue(safeStore, TxnStateKind.Waiting, txnId);
                        ((DefaultProgressLog)safeStore.progressLog()).requeue(safeStore, TxnStateKind.Home, txnId);
                        return AsyncChains.success(null);
                    });
            }
        }

        private void runWithRoute(TxnId txnId, int commandStoreId, Function<Command, BiConsumer<Route<?>, AsyncResult.Settable<Void>>> apply)
        {
            run(txnId, commandStoreId, safeStore -> {
                SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                Command command = safeCommand.current();
                if (command == null)
                    throw new InvalidRequestException(txnId + " not known");
                Node node = AccordService.instance().node();
                AsyncResult.Settable<Void> result = new AsyncResults.SettableResult<>();
                BiConsumer<Route<?>, AsyncResult.Settable<Void>> consumer = apply.apply(command);
                if (command.route() == null)
                {
                    FetchRoute.fetchRoute(node, txnId, command.maxContactable(), LatentStoreSelector.standard(), (success, fail) -> {
                        if (fail != null) result.setFailure(fail);
                        else consumer.accept(success, result);
                    });
                }
                else
                {
                    consumer.accept(command.route(), result);
                }
                return result;
            });
        }

        private void fetch(TxnId txnId, Timestamp executeAtIfKnown, Route<?> route, AsyncResult.Settable<Void> result)
        {
            Node node = AccordService.instance().node();
            FetchData.fetchSpecific(Known.Apply, node, txnId, executeAtIfKnown, route, route.withHomeKey(), LatentStoreSelector.standard(), (success, fail) -> {
                if (fail != null) result.setFailure(fail);
                else result.setSuccess(null);
            });
        }

        private void recover(TxnId txnId, @Nullable Route<?> route, AsyncResult.Settable<Void> result)
        {
            Node node = AccordService.instance().node();
            if (Route.isFullRoute(route))
            {
                RecoverWithRoute.recover(node, node.someSequentialExecutor(), txnId, NotKnownToBeInvalid, (FullRoute<?>) route, null, LatentStoreSelector.standard(), (success, fail) -> {
                    if (fail != null) result.setFailure(fail);
                    else result.setSuccess(null);
                }, node.agent().trace(txnId, RECOVER));
            }
            else
            {
                MaybeRecover.maybeRecover(node, txnId, NotKnownToBeInvalid, route, ProgressToken.NONE, LatentStoreSelector.standard(), (success, fail) -> {
                    if (fail != null) result.setFailure(fail);
                    else result.setSuccess(null);
                });
            }
        }

        private void run(TxnId txnId, int commandStoreId, Function<SafeCommandStore, AsyncChain<Void>> apply)
        {
            AccordService accord = (AccordService) AccordService.instance();
            AsyncChains.awaitUninterruptibly(accord.node()
                                                   .commandStores()
                                                   .forId(commandStoreId)
                                                   .submit(PreLoadContext.contextFor(txnId, TXN_OPS), apply)
                                                   .flatMap(i -> i)
                                                   .beginAsResult());
        }

        private void cleanup(TxnId txnId, int commandStoreId, Cleanup cleanup)
        {
            run(txnId, commandStoreId, safeStore -> {
                SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                Command command = safeCommand.current();
                Command updated = Commands.purge(safeStore, command, command.participants(), cleanup, true);
                safeCommand.update(safeStore, updated);
                return AsyncChains.success(null);
            });
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
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  command_store_id int,\n" +
                        "  depth int,\n" +
                        "  blocked_by text,\n" +
                        "  reason text,\n" +
                        "  save_status text,\n" +
                        "  execute_at text,\n" +
                        "  key text,\n" +
                        "  PRIMARY KEY (txn_id, keyspace_name, table_name, command_store_id, depth, blocked_by, reason)" +
                        ')', UTF8Type.instance));
        }

        @Override
        public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter, RowFilter rowFilter)
        {
            Partition partition = data(partitionKey, rowFilter).getPartition(partitionKey);

            if (null == partition)
                return EmptyIterators.unfilteredPartition(metadata);

            long now = currentTimeMillis();
            UnfilteredRowIterator rowIterator = partition.toRowIterator(metadata(), clusteringIndexFilter, columnFilter, now);
            return new SingletonUnfilteredPartitionIterator(rowIterator);
        }

        public DataSet data(DecoratedKey partitionKey, RowFilter rowFilter)
        {
            int maxDepth = Integer.MAX_VALUE;
            if (rowFilter != null && rowFilter.getExpressions().size() > 0)
            {
                Invariants.require(rowFilter.getExpressions().size() == 1, "Only depth filter is supported");
                RowFilter.Expression expression = rowFilter.getExpressions().get(0);
                Invariants.require(expression.column().name.toString().equals("depth"), "Only depth filter is supported, but got: %s", expression.column().name);
                Invariants.require(expression.operator() == Operator.LT || expression.operator() == Operator.LTE, "Only < and <= queries are supported");
                if (expression.operator() == Operator.LT)
                    maxDepth = expression.getIndexValue().getInt(0);
                else
                    maxDepth = expression.getIndexValue().getInt(0) + 1;
            }

            TxnId id = TxnId.parse(UTF8Type.instance.compose(partitionKey.getKey()));
            List<CommandStoreTxnBlockedGraph> shards = AccordService.instance().debugTxnBlockedGraph(id);

            SimpleDataSet ds = new SimpleDataSet(metadata());
            CommandStores commandStores = AccordService.instance().node().commandStores();
            for (CommandStoreTxnBlockedGraph shard : shards)
            {
                Set<TxnId> processed = new HashSet<>();
                process(ds, commandStores, shard, processed, id, 0, maxDepth, id, Reason.Self, null);
                // everything was processed right?
                if (!shard.txns.isEmpty() && !shard.txns.keySet().containsAll(processed))
                    Invariants.expect(false, "Skipped txns: " + Sets.difference(shard.txns.keySet(), processed));
            }

            return ds;
        }

        private void process(SimpleDataSet ds, CommandStores commandStores, CommandStoreTxnBlockedGraph shard, Set<TxnId> processed, TxnId userTxn, int depth, int maxDepth, TxnId txnId, Reason reason, Runnable onDone)
        {
            if (!processed.add(txnId))
                throw new IllegalStateException("Double processed " + txnId);
            CommandStoreTxnBlockedGraph.TxnState txn = shard.txns.get(txnId);
            if (txn == null)
            {
                Invariants.require(reason == Reason.Self, "Txn %s unknown for reason %s", txnId, reason);
                return;
            }
            // was it applied?  If so ignore it
            if (reason != Reason.Self && txn.saveStatus.hasBeen(Status.Applied))
                return;
            TableId tableId = tableId(shard.commandStoreId, commandStores);
            TableMetadata tableMetadata = tableMetadata(tableId);
            ds.row(userTxn.toString(), keyspace(tableMetadata), table(tableId, tableMetadata),
                   shard.commandStoreId, depth, reason == Reason.Self ? "" : txn.txnId.toString(), reason.name());
            ds.column("save_status", txn.saveStatus.name());
            if (txn.executeAt != null)
                ds.column("execute_at", txn.executeAt.toString());
            if (onDone != null)
                onDone.run();
            if (txn.isBlocked())
            {
                for (TxnId blockedBy : txn.blockedBy)
                {
                    if (!processed.contains(blockedBy) && depth < maxDepth)
                        process(ds, commandStores, shard, processed, userTxn, depth + 1, maxDepth, blockedBy, Reason.Txn, null);
                }

                for (TokenKey blockedBy : txn.blockedByKey)
                {
                    TxnId blocking = shard.keys.get(blockedBy);
                    if (!processed.contains(blocking) && depth < maxDepth)
                        process(ds, commandStores, shard, processed, userTxn, depth + 1, maxDepth, blocking, Reason.Key, () -> ds.column("key", printToken(blockedBy)));
                }
            }
        }

        @Override
        public DataSet data()
        {
            throw new InvalidRequestException("Must select a single txn_id");
        }
    }

    private static TableId tableId(int commandStoreId, CommandStores commandStores)
    {
        AccordCommandStore commandStore = (AccordCommandStore) commandStores.forId(commandStoreId);
        if (commandStore == null)
            return null;
        return commandStore.tableId();
    }

    private static TableMetadata tableMetadata(TableId tableId)
    {
        if (tableId == null)
            return null;
        return Schema.instance.getTableMetadata(tableId);
    }

    private static String keyspace(TableMetadata metadata)
    {
        return metadata == null ? "Unknown" : metadata.keyspace;
    }

    private static String table(TableId tableId, TableMetadata metadata)
    {
        return metadata == null ? tableId.toString() : metadata.name;
    }

    private static String printToken(RoutingKey routingKey)
    {
        TokenKey key = (TokenKey) routingKey;
        return key.token().getPartitioner().getTokenFactory().toString(key.token());
    }

    private static ByteBuffer sortToken(RoutingKey routingKey)
    {
        TokenKey key = (TokenKey) routingKey;
        Token token = key.token();
        IPartitioner partitioner = token.getPartitioner();
        ByteBuffer out = ByteBuffer.allocate(partitioner.accordSerializedSize(token));
        partitioner.accordSerialize(token, out);
        out.flip();
        return out;
    }

    private static TableMetadata parse(String keyspace, String table, String comment, String schema, AbstractType<?> partitionKeyType)
    {
        return CreateTableStatement.parse(format(schema, table), keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(partitionKeyType))
                                   .build();
    }

    private static String toStr(Command command, Function<StoreParticipants, Participants<?>> a, Function<StoreParticipants, Participants<?>> b)
    {
        return toStr(command.participants(), a, b);
    }

    private static String toStr(StoreParticipants participants, Function<StoreParticipants, Participants<?>> a, Function<StoreParticipants, Participants<?>> b)
    {
        Participants<?> av = a.apply(participants);
        Participants<?> bv = b.apply(participants);
        if (av == bv || av.equals(bv))
            return Objects.toString(av);
        return av + " (" + bv + ')';
    }

    private static TraceEventType parseEventType(String input)
    {
        try { return TraceEventType.valueOf(LocalizeString.toUpperCaseLocalized(input, Locale.ENGLISH)); }
        catch (Throwable t) { throw invalidRequest("event_type must be one of %s; received %s", TraceEventType.values(), input); }
    }

    private static String toStringOrNull(Object o)
    {
        if (o == null)
            return null;
        return Objects.toString(o);
    }
}
