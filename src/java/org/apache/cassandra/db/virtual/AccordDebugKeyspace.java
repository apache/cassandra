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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import accord.coordinate.AbstractCoordination;
import accord.coordinate.Coordination;
import accord.coordinate.Coordinations;
import accord.coordinate.PrepareRecovery;
import accord.coordinate.tracking.AbstractTracker;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.RoutingKeys;
import accord.utils.SortedListMap;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.TxnIdUtf8Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.TraceEventType;
import accord.coordinate.FetchData;
import accord.coordinate.FetchRoute;
import accord.coordinate.MaybeRecover;
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
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
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
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
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
import org.apache.cassandra.service.accord.DebugBlockedTxns;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.JournalKey;
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
import static accord.local.RedundantStatus.Property.LOG_UNAVAILABLE;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.local.RedundantStatus.Property.UNREADY;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.db.virtual.AbstractLazyVirtualTable.OnTimeout.BEST_EFFORT;
import static org.apache.cassandra.db.virtual.AbstractLazyVirtualTable.OnTimeout.FAIL;
import static org.apache.cassandra.db.virtual.VirtualTable.Sorted.ASC;
import static org.apache.cassandra.db.virtual.VirtualTable.Sorted.SORTED;
import static org.apache.cassandra.db.virtual.VirtualTable.Sorted.UNSORTED;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_ACCORD_DEBUG;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class AccordDebugKeyspace extends VirtualKeyspace
{
    public static final String COORDINATIONS      = "coordinations";
    public static final String EXECUTORS          = "executors";
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

    private static final Function<Object, String> TO_STRING = AccordDebugKeyspace::toStringOrNull;

    public static final AccordDebugKeyspace instance = new AccordDebugKeyspace();

    private AccordDebugKeyspace()
    {
        super(VIRTUAL_ACCORD_DEBUG, List.of(
            new ExecutorsTable(),
            new CoordinationsTable(),
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

    // TODO (desired): human readable packed key tracker (but requires loading Txn, so might be preferable to only do conditionally)
    public static final class ExecutorsTable extends AbstractLazyVirtualTable
    {
        private ExecutorsTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, EXECUTORS,
                        "Accord Executor State",
                        "CREATE TABLE %s (\n" +
                        "  executor_id int,\n" +
                        "  status text,\n" +
                        "  position int,\n" +
                        "  unique_position int,\n" +
                        "  description text,\n" +
                        "  command_store_id int,\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  txn_id_additional 'TxnIdUtf8Type',\n" +
                        "  keys text,\n" +
                        "  keys_loading text,\n" +
                        "  keys_loading_for text,\n" +
                        "  PRIMARY KEY (executor_id, status, position, unique_position)" +
                        ')', Int32Type.instance), FAIL, ASC);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            AccordCommandStores commandStores = (AccordCommandStores) AccordService.instance().node().commandStores();
            // TODO (desired): we can easily also support sorted collection for DESC queries
            for (AccordExecutor executor : commandStores.executors())
            {
                int executorId = executor.executorId();
                collector.partition(executorId).collect(rows -> {
                    int uniquePos = 0;
                    AccordExecutor.TaskInfo prev = null;
                    for (AccordExecutor.TaskInfo info : executor.taskSnapshot())
                    {
                        if (prev != null && info.status() == prev.status() && info.position() == prev.position()) ++uniquePos;
                        else uniquePos = 0;
                        prev = info;
                        PreLoadContext preLoadContext = info.preLoadContext();
                        rows.add(info.status().name(), info.position(), uniquePos)
                                 .lazyCollect(columns -> {
                                     columns.add("description", info.describe())
                                            .add("command_store_id", info.commandStoreId())
                                            .add("txn_id", preLoadContext, PreLoadContext::primaryTxnId, TO_STRING)
                                            .add("txn_id_additional", preLoadContext, PreLoadContext::additionalTxnId, TO_STRING)
                                            .add("keys", preLoadContext, PreLoadContext::keys, TO_STRING)
                                            .add("keys_loading", preLoadContext, PreLoadContext::loadKeys, TO_STRING)
                                            .add("keys_loading_for", preLoadContext, PreLoadContext::loadKeysFor, TO_STRING);
                        });
                    }
                });
            }
        }
    }

    // TODO (desired): human readable packed key tracker (but requires loading Txn, so might be preferable to only do conditionally)
    public static final class CoordinationsTable extends AbstractLazyVirtualTable
    {
        private CoordinationsTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, COORDINATIONS,
                        "Accord Coordination State",
                        "CREATE TABLE %s (\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  kind text,\n" +
                        "  coordination_id bigint,\n" +
                        "  description text,\n" +
                        "  nodes text,\n" +
                        "  nodes_inflight text,\n" +
                        "  nodes_contacted text,\n" +
                        "  participants text,\n" +
                        "  replies text,\n" +
                        "  tracker text,\n" +
                        "  PRIMARY KEY (txn_id, kind, coordination_id)" +
                        ')', TxnIdUtf8Type.instance), FAIL, UNSORTED);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            Coordinations coordinations = AccordService.instance().node().coordinations();
            for (Coordination c : coordinations)
            {
                collector.row(toStringOrNull(c.txnId()), c.kind().toString(), c.coordinationId())
                .lazyCollect(columns -> {
                    columns.add("nodes", c, Coordination::nodes, TO_STRING)
                           .add("nodes_inflight", c, Coordination::inflight, TO_STRING)
                           .add("nodes_contacted", c, Coordination::contacted, TO_STRING)
                           .add("description", c, Coordination::describe, TO_STRING)
                           .add("participants", c, Coordination::scope, TO_STRING)
                           .add("replies", c, Coordination::replies, CoordinationsTable::summarise)
                           .add("tracker", c, Coordination::tracker, AbstractTracker::summariseTracker);
                });
            }
        }

        private static String summarise(SortedListMap<Node.Id, ?> replies)
        {
            return AbstractCoordination.summariseReplies(replies, 60);
        }
    }

    private static abstract class AbstractCommandsForKeyTable extends AbstractLazyVirtualTable
    {
        static class Entry implements Comparable<Entry>
        {
            final int commandStoreId;
            final CommandsForKey cfk;

            Entry(int commandStoreId, CommandsForKey cfk)
            {
                this.commandStoreId = commandStoreId;
                this.cfk = cfk;
            }

            @Override
            public int compareTo(Entry that)
            {
                return Integer.compare(this.commandStoreId, that.commandStoreId);
            }
        }

        AbstractCommandsForKeyTable(TableMetadata metadata)
        {
            super(metadata, BEST_EFFORT, SORTED);
        }

        abstract void collect(PartitionCollector partition, int commandStoreId, CommandsForKey cfk);

        @Override
        public void collect(PartitionsCollector collector)
        {
            Object[] partitionKey = collector.singlePartitionKey();
            if (partitionKey == null)
                throw new InvalidRequestException(metadata + " currently only supports querying single partitions");

            TokenKey key = TokenKey.parse((String) partitionKey[0], DatabaseDescriptor.getPartitioner());

            List<Entry> cfks = new CopyOnWriteArrayList<>();
            CommandStores commandStores = AccordService.instance().node().commandStores();
            AccordService.getBlocking(commandStores.forEach("commands_for_key table query", RoutingKeys.of(key), Long.MIN_VALUE, Long.MAX_VALUE, safeStore -> {
                SafeCommandsForKey safeCfk = safeStore.get(key);
                CommandsForKey cfk = safeCfk.current();
                if (cfk == null)
                    return;

                cfks.add(new Entry(safeStore.commandStore().id(), cfk));
            }));

            if (cfks.isEmpty())
                return;

            cfks.sort(collector.dataRange().isReversed() ? Comparator.reverseOrder() : Comparator.naturalOrder());
            for (Entry entry : cfks)
                collect(collector.partition(partitionKey[0]), entry.commandStoreId, entry.cfk);
        }
    }

    public static final class CommandsForKeyTable extends AbstractCommandsForKeyTable
    {
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
        void collect(PartitionCollector partition, int commandStoreId, CommandsForKey cfk)
        {
            partition.collect(rows -> {
                for (int i = 0 ; i < cfk.size() ; ++i)
                {
                    TxnInfo txn = cfk.get(i);
                    rows.add(commandStoreId, txn.plainTxnId().toString())
                        .lazyCollect(columns -> {
                            columns.add("ballot", txn.ballot(), AccordDebugKeyspace::toStringOrNull)
                                   .add("deps_known_before", txn, TxnInfo::depsKnownUntilExecuteAt, TO_STRING)
                                   .add("flags", txn, CommandsForKeyTable::flags)
                                   .add("execute_at", txn, TxnInfo::plainExecuteAt, TO_STRING)
                                   .add("missing", txn, TxnInfo::missing, Arrays::toString)
                                   .add("status", txn, TxnInfo::status, TO_STRING)
                                   .add("status_overrides", txn.statusOverrides() == 0 ? null : ("0x" + Integer.toHexString(txn.statusOverrides())));
                        });
                }
            });
        }

        private static String flags(TxnInfo txn)
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

    public static final class CommandsForKeyUnmanagedTable extends AbstractCommandsForKeyTable
    {
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
        void collect(PartitionCollector partition, int commandStoreId, CommandsForKey cfk)
        {
            partition.collect(rows -> {
                for (int i = 0 ; i < cfk.unmanagedCount() ; ++i)
                {
                    CommandsForKey.Unmanaged txn = cfk.getUnmanaged(i);
                    rows.add(commandStoreId, toStringOrNull(txn.txnId))
                        .lazyCollect(columns -> {
                            columns.add("waiting_until", txn.waitingUntil, TO_STRING)
                                   .add("waiting_until_status", txn.pending, TO_STRING);
                        });
                }
            });
        }
    }

    public static final class DurabilityServiceTable extends AbstractLazyVirtualTable
    {
        private DurabilityServiceTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, DURABILITY_SERVICE,
                        "Accord per-Range Durability Service State",
                        "CREATE TABLE %s (\n" +
                        "  table_id text,\n" +
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
                        "  PRIMARY KEY (table_id, token_start)" +
                        ')', UTF8Type.instance), FAIL, UNSORTED);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            ShardDurability.ImmutableView view = ((AccordService) AccordService.instance()).shardDurability();

            while (view.advance())
            {
                TableId tableId = (TableId) view.shard().range.start().prefix();
                collector.row(tableId.toString(), printToken(view.shard().range.start()))
                         .eagerCollect(columns -> {
                              columns.add("token_end", printToken(view.shard().range.end()))
                                     .add("last_started_at", approxTime.translate().toMillisSinceEpoch(view.lastStartedAtMicros() * 1000))
                                     .add("cycle_started_at", approxTime.translate().toMillisSinceEpoch(view.cycleStartedAtMicros() * 1000))
                                     .add("retries", view.retries())
                                     .add("min", Objects.toString(view.min()))
                                     .add("requested_by", Objects.toString(view.requestedBy()))
                                     .add("active", Objects.toString(view.active()))
                                     .add("waiting", Objects.toString(view.waiting()))
                                     .add("node_offset", view.nodeOffset())
                                     .add("cycle_offset", view.cycleOffset())
                                     .add("active_index", view.activeIndex())
                                     .add("next_index", view.nextIndex())
                                     .add("next_to_index", view.toIndex())
                                     .add("end_index", view.cycleLength())
                                     .add("current_splits", view.currentSplits())
                                     .add("stopping", view.stopping())
                                     .add("stopped", view.stopped());
                         });
            }
        }
    }

    public static final class DurableBeforeTable extends AbstractLazyVirtualTable
    {
        private DurableBeforeTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, DURABLE_BEFORE,
                        "Accord Node's DurableBefore State",
                        "CREATE TABLE %s (\n" +
                        "  table_id text,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  quorum 'TxnIdUtf8Type',\n" +
                        "  universal 'TxnIdUtf8Type',\n" +
                        "  PRIMARY KEY (table_id, token_start)" +
                        ')', UTF8Type.instance), FAIL, UNSORTED);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            DurableBefore durableBefore = AccordService.instance().node().durableBefore();
            durableBefore.foldlWithBounds(
                (entry, ignore, start, end) -> {
                    TableId tableId = (TableId) start.prefix();
                    collector.row(tableId.toString(), printToken(start))
                             .lazyCollect(columns -> {
                                  columns.add("token_end", end, AccordDebugKeyspace::printToken)
                                         .add("quorum", entry.quorumBefore, TO_STRING)
                                         .add("universal", entry.universalBefore, TO_STRING);
                             });
                    return null;
                }, null, ignore -> false);
        }
    }

    public static final class ExecutorCacheTable extends AbstractLazyVirtualTable
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
                        ')', Int32Type.instance), FAIL, UNSORTED);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            AccordCommandStores stores = (AccordCommandStores) AccordService.instance().node().commandStores();
            for (AccordExecutor executor : stores.executors())
            {
                try (AccordExecutor.ExclusiveGlobalCaches cache = executor.lockCaches())
                {
                    addRow(collector, executor.executorId(), "commands", cache.commands.statsSnapshot());
                    addRow(collector, executor.executorId(), AccordKeyspace.COMMANDS_FOR_KEY, cache.commandsForKey.statsSnapshot());
                }
            }
        }

        private static void addRow(PartitionsCollector collector, int executorId, String scope, AccordCache.ImmutableStats stats)
        {
            collector.row(executorId, scope)
                     .eagerCollect(columns -> {
                         columns.add("queries", stats.hits + stats.misses)
                                .add("hits", stats.hits)
                                .add("misses", stats.misses);
                     });
        }
    }

    public static final class MaxConflictsTable extends AbstractLazyVirtualTable
    {
        private MaxConflictsTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, MAX_CONFLICTS,
                        "Accord per-CommandStore MaxConflicts State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id bigint,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  table_id text,\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  timestamp text,\n" +
                        "  PRIMARY KEY (command_store_id, token_start)" +
                        ')', Int32Type.instance), FAIL, ASC);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();

            for (CommandStore commandStore : commandStores.all())
            {
                int commandStoreId = commandStore.id();
                MaxConflicts maxConflicts = commandStore.unsafeGetMaxConflicts();
                TableId tableId = ((AccordCommandStore) commandStore).tableId();
                String tableIdStr = tableId.toString();

                collector.partition(commandStoreId).collect(rows -> {
                    maxConflicts.foldlWithBounds(
                        (timestamp, rs, start, end) -> {
                            rows.add(printToken(start))
                                .lazyCollect(columns -> {
                                    columns.add("token_end", end, AccordDebugKeyspace::printToken)
                                           .add("table_id", tableIdStr)
                                           .add("timestamp", timestamp, TO_STRING);
                                });
                             return rows;
                        }, rows, ignore -> false
                    );
                });
            }
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
    public static final class ProgressLogTable extends AbstractLazyVirtualTable
    {
        private ProgressLogTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, PROGRESS_LOG,
                        "Accord per-CommandStore ProgressLog State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  table_id text,\n" +
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
                        ')', Int32Type.instance), FAIL, ASC);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();
            for (CommandStore commandStore : commandStores.all())
            {
                DefaultProgressLog.ImmutableView view = ((DefaultProgressLog) commandStore.unsafeProgressLog()).immutableView();
                TableId tableId = ((AccordCommandStore)commandStore).tableId();
                String tableIdStr = tableId.toString();
                collector.partition(commandStore.id()).collect(collect -> {
                    while (view.advance())
                    {
                        // TODO (required): view should return an immutable per-row view so that we can call lazyAdd
                        collect.add(view.txnId().toString())
                               .eagerCollect(columns -> {
                                   columns.add("table_id", tableIdStr)
                                          .add("contact_everyone", view.contactEveryone())
                                          .add("waiting_is_uninitialised", view.isWaitingUninitialised())
                                          .add("waiting_blocked_until", view.waitingIsBlockedUntil().name())
                                          .add("waiting_home_satisfies", view.waitingHomeSatisfies().name())
                                          .add("waiting_progress", view.waitingProgress().name())
                                          .add("waiting_retry_counter", view.waitingRetryCounter())
                                          .add("waiting_packed_key_tracker_bits", Long.toBinaryString(view.waitingPackedKeyTrackerBits()))
                                          .add("waiting_scheduled_at", view.timerScheduledAt(TxnStateKind.Waiting), ProgressLogTable::toTimestamp)
                                          .add("home_phase", view.homePhase().name())
                                          .add("home_progress", view.homeProgress().name())
                                          .add("home_retry_counter", view.homeRetryCounter())
                                          .add("home_scheduled_at", view.timerScheduledAt(TxnStateKind.Home), ProgressLogTable::toTimestamp);
                               });
                    }

                });
            }
        }

        private static Date toTimestamp(Long deadline)
        {
            if (deadline == null)
                return null;

            long millisSinceEpoch = approxTime.translate().toMillisSinceEpoch(deadline * 1000L);
            return new Date(millisSinceEpoch);
        }
    }

    public static final class RedundantBeforeTable extends AbstractLazyVirtualTable
    {
        private RedundantBeforeTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, REDUNDANT_BEFORE,
                        "Accord per-CommandStore RedundantBefore State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  table_id text,\n" +
                        "  token_end 'TokenUtf8Type',\n" +
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
                        "  log_unavailable 'TxnIdUtf8Type',\n" +
                        "  unready 'TxnIdUtf8Type',\n" +
                        "  stale_until_at_least 'TxnIdUtf8Type',\n" +
                        "  PRIMARY KEY (command_store_id, token_start)" +
                        ')', Int32Type.instance), FAIL, ASC);
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();

            for (CommandStore commandStore : commandStores.all())
            {
                int commandStoreId = commandStore.id();
                collector.partition(commandStoreId).collect(rows -> {
                    TableId tableId = ((AccordCommandStore)commandStore).tableId();
                    String tableIdStr = tableId.toString();
                    commandStore.unsafeGetRedundantBefore().foldl(
                        (entry, rs) -> {
                            rs.add(printToken(entry.range.start())).lazyCollect(columns -> {
                                columns.add("table_id", tableIdStr)
                                       .add("token_end", entry.range.end(), AccordDebugKeyspace::printToken)
                                       .add("start_epoch", entry.startEpoch)
                                       .add("end_epoch", entry.endEpoch)
                                       .add("gc_before", entry, e -> e.maxBound(GC_BEFORE), TO_STRING)
                                       .add("shard_applied", entry, e -> e.maxBound(SHARD_APPLIED), TO_STRING)
                                       .add("quorum_applied", entry, e -> e.maxBound(QUORUM_APPLIED), TO_STRING)
                                       .add("locally_applied", entry, e -> e.maxBound(LOCALLY_APPLIED), TO_STRING)
                                       .add("locally_durable_to_command_store", entry, e -> e.maxBound(LOCALLY_DURABLE_TO_COMMAND_STORE), TO_STRING)
                                       .add("locally_durable_to_data_store", entry, e -> e.maxBound(LOCALLY_DURABLE_TO_DATA_STORE), TO_STRING)
                                       .add("locally_redundant", entry, e -> e.maxBound(LOCALLY_REDUNDANT), TO_STRING)
                                       .add("locally_synced", entry, e -> e.maxBound(LOCALLY_SYNCED), TO_STRING)
                                       .add("locally_witnessed", entry, e -> e.maxBound(LOCALLY_WITNESSED), TO_STRING)
                                       .add("log_unavailable", entry, e -> e.maxBound(LOG_UNAVAILABLE), TO_STRING)
                                       .add("unready", entry, e -> e.maxBound(UNREADY), TO_STRING)
                                       .add("stale_until_at_least", entry.staleUntilAtLeast, TO_STRING);
                            });
                            return rs;
                        }, rows, ignore -> false
                    );
                });
            }
        }
    }

    public static final class RejectBeforeTable extends AbstractLazyVirtualTable
    {
        private RejectBeforeTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, REJECT_BEFORE,
                        "Accord per-CommandStore RejectBefore State",
                        "CREATE TABLE %s (\n" +
                        "  command_store_id int,\n" +
                        "  token_start 'TokenUtf8Type',\n" +
                        "  table_id text,\n" +
                        "  token_end 'TokenUtf8Type',\n" +
                        "  timestamp text,\n" +
                        "  PRIMARY KEY (command_store_id, token_start)" +
                        ')', UTF8Type.instance), FAIL, ASC);
        }

        @Override
        protected void collect(PartitionsCollector collector)
        {
            CommandStores commandStores = AccordService.instance().node().commandStores();
            for (CommandStore commandStore : commandStores.all())
            {
                RejectBefore rejectBefore = commandStore.unsafeGetRejectBefore();
                if (rejectBefore == null)
                    continue;

                collector.partition(commandStore.id()).collect(rows -> {
                    TableId tableId = ((AccordCommandStore)commandStore).tableId();
                    String tableIdStr = tableId.toString();
                    rejectBefore.foldlWithBounds((timestamp, rs, start, end) -> {
                        rs.add(printToken(start))
                          .lazyCollect(columns -> columns.add("table_id", tableIdStr)
                                                         .add("token_end", end, AccordDebugKeyspace::printToken)
                                                         .add("timestamp", timestamp, AccordDebugKeyspace::toStringOrNull));
                        return rs;
                    }, rows, ignore -> false);
                });
            }
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
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  event_type text,\n" +
                        "  permits int,\n" +
                        "  PRIMARY KEY (txn_id, event_type)" +
                        ')', TxnIdUtf8Type.instance));
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

    public static final class TxnTracesTable extends AbstractMutableLazyVirtualTable
    {
        private TxnTracesTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN_TRACES,
                        "Accord Transaction Traces",
                        "CREATE TABLE %s (\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  event_type text,\n" +
                        "  id_micros bigint,\n" +
                        "  at_micros bigint,\n" +
                        "  command_store_id int,\n" +
                        "  message text,\n" +
                        "  PRIMARY KEY (txn_id, event_type, id_micros, at_micros)" +
                        ')', TxnIdUtf8Type.instance), FAIL, UNSORTED, UNSORTED);
        }

        private AccordTracing tracing()
        {
            return ((AccordAgent)AccordService.instance().agent()).tracing();
        }

        @Override
        protected void applyPartitionDeletion(Object[] partitionKeys)
        {
            TxnId txnId = TxnId.parse((String)partitionKeys[0]);
            tracing().eraseEvents(txnId);
        }

        @Override
        protected void applyRangeTombstone(Object[] partitionKeys, Object[] starts, boolean startInclusive, Object[] ends, boolean endInclusive)
        {
            TxnId txnId = TxnId.parse((String) partitionKeys[0]);
            if (!startInclusive) throw invalidRequest("May restrict deletion by at most one event_type");
            if (starts.length != 1) throw invalidRequest("Deletion restricted by lower bound on id_micros or at_micros is unsupported");
            if (ends.length == 0 || (ends.length == 1 && !endInclusive)) throw invalidRequest("Range deletion must specify one event_type");
            if (!ends[0].equals(starts[0])) throw invalidRequest("May restrict deletion by at most one event_type");
            if (ends.length > 2) throw invalidRequest("Deletion restricted by upper bound on at_micros is unsupported");
            TraceEventType eventType = parseEventType((String) starts[0]);
            if (ends.length == 1)
            {
                tracing().eraseEvents(txnId, eventType);
            }
            else
            {
                long before = (Long)ends[1];
                tracing().eraseEventsBefore(txnId, eventType, before);
            }
        }

        @Override
        public void truncate()
        {
            tracing().eraseAllEvents();
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            tracing().forEach(id -> true, (txnId, eventType, permits, events) -> {
                events.forEach(e -> {
                    if (e.messages().isEmpty())
                    {
                        collector.row(txnId.toString(), eventType.name(), e.idMicros, 0L)
                                 .eagerCollect(columns -> {
                                     columns.add("message", "<Initialised but no events (yet) recorded>");
                                 });
                    }
                    else
                    {
                        e.messages().forEach(m -> {
                            collector.row(txnId.toString(), eventType.name(), e.idMicros, NANOSECONDS.toMicros(m.atNanos - e.atNanos))
                            .eagerCollect(columns -> {
                                columns.add("command_store_id", m.commandStoreId)
                                       .add("message", m.message);
                            });
                        });
                    }
                });
            });
        }
    }

    // TODO (desired): don't report null as "null"
    abstract static class AbstractJournalTable extends AbstractLazyVirtualTable
    {
        static final CompositeType PK = CompositeType.getInstance(Int32Type.instance, UTF8Type.instance);

        AbstractJournalTable(TableMetadata metadata)
        {
            super(metadata, FAIL, ASC);
        }

        @Override
        public boolean allowFilteringImplicitly()
        {
            return false;
        }

        @Override
        public boolean allowFilteringPrimaryKeysImplicitly()
        {
            return true;
        }

        @Override
        public void collect(PartitionsCollector collector)
        {
            AccordService accord;
            {
                IAccordService iaccord = AccordService.instance();
                if (!iaccord.isEnabled())
                    return;

                accord = (AccordService) iaccord;
            }

            DataRange dataRange = collector.dataRange();
            JournalKey min = toJournalKey(dataRange.startKey()),
            max = toJournalKey(dataRange.stopKey());

            if (min == null && max == null)
            {
                FilterRange<TxnId> filterTxnId = collector.filters("txn_id", TxnId::parse, UnaryOperator.identity(), UnaryOperator.identity());
                FilterRange<Integer> filterCommandStoreId = collector.filters("command_store_id", UnaryOperator.identity(), i -> i + 1, i -> i - 1);

                int minCommandStoreId = filterCommandStoreId.min == null ? -1 : filterCommandStoreId.min;
                int maxCommandStoreId = filterCommandStoreId.max == null ? Integer.MAX_VALUE : filterCommandStoreId.max;

                if (filterTxnId.min != null && filterTxnId.max != null && filterTxnId.min.equals(filterTxnId.max))
                {
                    TxnId txnId = filterTxnId.min;
                    accord.node().commandStores().forAllUnsafe(commandStore -> {
                        if (commandStore.id() < minCommandStoreId || commandStore.id() > maxCommandStoreId)
                            return;

                        collect(collector, accord, new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, commandStore.id()));
                    });
                    return;
                }

                if (filterTxnId.min != null || filterTxnId.max != null || minCommandStoreId >= 0 || maxCommandStoreId < Integer.MAX_VALUE)
                {
                    min = new JournalKey(filterTxnId.min == null ? TxnId.NONE : filterTxnId.min, JournalKey.Type.COMMAND_DIFF, Math.max(0, minCommandStoreId));
                    max = new JournalKey(filterTxnId.max == null ? TxnId.MAX.withoutNonIdentityFlags() : filterTxnId.max, JournalKey.Type.COMMAND_DIFF, maxCommandStoreId);
                }
            }

            accord.journal().forEach(key -> collect(collector, accord, key), min, max, true);
        }

        abstract void collect(PartitionsCollector collector, AccordService accord, JournalKey key);

        private static JournalKey toJournalKey(PartitionPosition position)
        {
            if (position.isMinimum())
                return null;

            if (!(position instanceof DecoratedKey))
                throw new InvalidRequestException("Cannot filter this table by partial partition key");

            ByteBuffer[] keys = PK.split(((DecoratedKey) position).getKey());
            return new JournalKey(TxnId.parse(UTF8Type.instance.compose(keys[1])), JournalKey.Type.COMMAND_DIFF, Int32Type.instance.compose(keys[0]));
        }
    }

    // TODO (desired): don't report null as "null"
    public static final class TxnTable extends AbstractJournalTable
    {
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
                        "  PRIMARY KEY ((command_store_id, txn_id))" +
                        ')', PK));
        }

        @Override
        void collect(PartitionsCollector collector, AccordService accord, JournalKey key)
        {
            if (key.type != JournalKey.Type.COMMAND_DIFF)
                return;

            AccordCommandStore commandStore = (AccordCommandStore) accord.node().commandStores().forId(key.commandStoreId);
            if (commandStore == null)
                return;

            Command command = commandStore.loadCommand(key.id);
            if (command == null)
                return;

            collector.row(key.commandStoreId, key.id.toString())
                     .lazyCollect(columns -> addColumns(command, columns));
        }

        private static void addColumns(Command command, ColumnsCollector columns)
        {
            StoreParticipants participants = command.participants();
            columns.add("save_status", command.saveStatus(), TO_STRING)
                   .add("route", participants, StoreParticipants::route, TO_STRING)
                   .add("participants_owns", participants, p -> toStr(p, StoreParticipants::owns, StoreParticipants::stillOwns))
                   .add("participants_touches", participants, p -> toStr(p, StoreParticipants::touches, StoreParticipants::stillTouches))
                   .add("participants_has_touched", participants, StoreParticipants::hasTouched, TO_STRING)
                   .add("participants_executes", participants, p -> toStr(p, StoreParticipants::executes, StoreParticipants::stillExecutes))
                   .add("participants_waits_on", participants, p -> toStr(p, StoreParticipants::waitsOn, StoreParticipants::stillWaitsOn))
                   .add("durability", command, Command::durability, TO_STRING)
                   .add("execute_at", command, Command::executeAt, TO_STRING)
                   .add("executes_at_least", command, Command::executesAtLeast, TO_STRING)
                   .add("txn", command, Command::partialTxn, TO_STRING)
                   .add("deps", command, Command::partialDeps, TO_STRING)
                   .add("waiting_on", command, Command::waitingOn, TO_STRING)
                   .add("writes", command, Command::writes, TO_STRING)
                   .add("result", command, Command::result, TO_STRING);
        }
    }

    public static final class JournalTable extends AbstractJournalTable
    {
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
                        "  PRIMARY KEY ((command_store_id, txn_id), segment, segment_position)" +
                        ')', PK));
        }

        @Override
        void collect(PartitionsCollector collector, AccordService accord, JournalKey key)
        {
            AccordCommandStore commandStore = (AccordCommandStore) accord.node().commandStores().forId(key.commandStoreId);
            collector.partition(key.commandStoreId, key.id.toString()).collect(rows -> {
                for (AccordJournal.DebugEntry e : commandStore.debugCommand(key.id))
                {
                    CommandChange.Builder b = e.builder;
                    StoreParticipants participants = b.participants() != null ? b.participants() : StoreParticipants.empty(key.id);
                    rows.add(e.segment, e.position)
                        .lazyCollect(columns -> {
                            columns.add("save_status", b.saveStatus(), TO_STRING)
                                   .add("route", participants, StoreParticipants::route, TO_STRING)
                                   .add("participants_owns", participants, p -> toStr(p, StoreParticipants::owns, StoreParticipants::stillOwns))
                                   .add("participants_touches", participants, p -> toStr(p, StoreParticipants::touches, StoreParticipants::stillTouches))
                                   .add("participants_has_touched", participants, StoreParticipants::hasTouched, TO_STRING)
                                   .add("participants_executes", participants, p -> toStr(p, StoreParticipants::executes, StoreParticipants::stillExecutes))
                                   .add("participants_waits_on", participants, p -> toStr(p, StoreParticipants::waitsOn, StoreParticipants::stillWaitsOn))
                                   .add("durability", b.durability(), TO_STRING)
                                   .add("execute_at", b.executeAt(), TO_STRING)
                                   .add("executes_at_least", b.executesAtLeast(), TO_STRING)
                                   .add("txn", b.partialTxn(), TO_STRING)
                                   .add("deps", b.partialDeps(), TO_STRING)
                                   .add("writes", b.writes(), TO_STRING)
                                   .add("result", b.result(), TO_STRING);
                        });
                }
            });
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
    public static final class TxnOpsTable extends AbstractMutableLazyVirtualTable
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
                        ')', UTF8Type.instance), FAIL, UNSORTED);
        }

        @Override
        protected void collect(PartitionsCollector collector)
        {
            throw new UnsupportedOperationException(TXN_OPS + " is a write-only table");
        }

        @Override
        protected void applyRowUpdate(Object[] partitionKeys, Object[] clusteringKeys, ColumnMetadata[] columns, Object[] values)
        {
            TxnId txnId = TxnId.parse((String) partitionKeys[0]);
            int commandStoreId = (Integer) clusteringKeys[0];
            Op op = Op.valueOf((String)values[0]);
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
                return result.chain();
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
                PrepareRecovery.recover(node, node.someSequentialExecutor(), txnId, NotKnownToBeInvalid, (FullRoute<?>) route, null, LatentStoreSelector.standard(), (success, fail) -> {
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
            AccordService.getBlocking(accord.node()
                                            .commandStores()
                                            .forId(commandStoreId)
                                            .chain(PreLoadContext.contextFor(txnId, TXN_OPS), apply));
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

    public static class TxnBlockedByTable extends AbstractLazyVirtualTable
    {
        enum Reason
        {Self, Txn, Key}

        protected TxnBlockedByTable()
        {
            super(parse(VIRTUAL_ACCORD_DEBUG, TXN_BLOCKED_BY,
                        "Accord Transactions Blocked By Table",
                        "CREATE TABLE %s (\n" +
                        "  txn_id 'TxnIdUtf8Type',\n" +
                        "  command_store_id int,\n" +
                        "  depth int,\n" +
                        "  blocked_by_key text,\n" +
                        "  blocked_by_txn_id 'TxnIdUtf8Type',\n" +
                        "  save_status text,\n" +
                        "  execute_at text,\n" +
                        "  PRIMARY KEY (txn_id, command_store_id, depth, blocked_by_key, blocked_by_txn_id)" +
                        ')', TxnIdUtf8Type.instance), BEST_EFFORT, ASC);
        }

        @Override
        protected void collect(PartitionsCollector collector)
        {
            Object[] pks = collector.singlePartitionKey();
            if (pks == null)
                throw new InvalidRequestException(metadata + " only supports single partition key queries");

            FilterRange<Integer> depthRange = collector.filters("depth", Function.identity(), i -> i + 1, i -> i - 1);
            int maxDepth = depthRange.max == null ? Integer.MAX_VALUE : depthRange.max;

            TxnId txnId = TxnId.parse((String) pks[0]);
            PartitionCollector partition = collector.partition(pks[0]);
            partition.collect(rows -> {
                try
                {
                    DebugBlockedTxns.visit(AccordService.instance(), txnId, maxDepth, collector.deadlineNanos(), txn -> {
                        String keyStr = txn.blockedViaKey == null ? "" : txn.blockedViaKey.toString();
                        String txnIdStr = txn.txnId == null || txn.txnId.equals(txnId) ? "" : txn.txnId.toString();
                        rows.add(txn.commandStoreId, txn.depth, keyStr, txnIdStr)
                            .eagerCollect(columns -> {
                                columns.add("save_status", txn.saveStatus, TO_STRING)
                                       .add("execute_at", txn.executeAt, TO_STRING);
                            });
                    });
                }
                catch (TimeoutException e)
                {
                    throw new InternalTimeoutException();
                }
            });
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

    private static String printToken(RoutingKey routingKey)
    {
        TokenKey key = (TokenKey) routingKey;
        return key.token().getPartitioner().getTokenFactory().toString(key.token());
    }

    private static TableMetadata parse(String keyspace, String table, String comment, String schema, AbstractType<?> partitionKeyType)
    {
        return CreateTableStatement.parse(format(schema, table), keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(partitionKeyType))
                                   .build();
    }

    private static String toStr(StoreParticipants participants, Function<StoreParticipants, Participants<?>> a, Function<StoreParticipants, Participants<?>> b)
    {
        if (participants == null)
            return null;

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

        try
        {
            return Objects.toString(o);
        }
        catch (Throwable t)
        {
            return "<error: " + t.getLocalizedMessage() + '>';
        }
    }
}
