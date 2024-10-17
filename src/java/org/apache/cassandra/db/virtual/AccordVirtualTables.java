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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.Range;
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
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class AccordVirtualTables
{
    private AccordVirtualTables() {}

    public static Collection<VirtualTable> getAll(String keyspace)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return Collections.emptyList();

        return List.of(
            new ExecutorCache(keyspace),
            new MigrationState(keyspace),
            new CoordinationStatus(keyspace),
            new TxnBlockedByTable(keyspace)
        );
    }

    public static final class ExecutorCache extends AbstractVirtualTable
    {
        private ExecutorCache(String keyspace)
        {
            super(parse(keyspace,
                        "Accord Executor Cache Metrics",
                        "CREATE TABLE accord_executor_cache(\n" +
                        "  id int,\n" +
                        "  scope text,\n" +
                        "  queries bigint,\n" +
                        "  hits bigint,\n" +
                        "  misses bigint,\n" +
                        "  PRIMARY KEY (id, scope)" +
                        ')'));
        }

        @Override
        public DataSet data()
        {
            AccordCommandStores stores = (AccordCommandStores) ((AccordService) AccordService.instance()).node().commandStores();
            SimpleDataSet result = new SimpleDataSet(metadata());
            for (AccordExecutor executor : stores.executors())
            {
                Map<String, AccordCache.ImmutableStats> snapshots = new HashMap<>(3);
                try (AccordExecutor.ExclusiveGlobalCaches cache = executor.lockCaches())
                {
                    addRow(cache.commands.statsSnapshot(), result, executor.executorId(), AccordKeyspace.COMMANDS);
                    addRow(cache.commandsForKey.statsSnapshot(), result, executor.executorId(), AccordKeyspace.COMMANDS_FOR_KEY);
                    addRow(cache.timestampsForKey.statsSnapshot(), result, executor.executorId(), AccordKeyspace.TIMESTAMPS_FOR_KEY);
                }
            }
            return result;
        }

        private static void addRow(AccordCache.ImmutableStats stats, SimpleDataSet result, int storeID, String scope)
        {
            result.row(storeID, scope);
            result.column("queries", stats.queries);
            result.column("hits", stats.hits);
            result.column("misses", stats.misses);
        }
    }

    public static final class MigrationState extends AbstractVirtualTable
    {
        private static final Logger logger = LoggerFactory.getLogger(MigrationState.class);
        
        private MigrationState(String keyspace)
        {
            super(parse(keyspace,
                        "Consensus Migration State",
                        "CREATE TABLE consensus_migration_state(\n" +
                        "  keyspace_name text,\n" +
                        "  table_name text,\n" +
                        "  table_id uuid,\n" +
                        "  target_protocol text,\n" +
                        "  transactional_mode text,\n" +
                        "  transactional_migration_from text,\n" +
                        "  migrated_ranges frozen<list<text>>,\n" +
                        "  migrating_ranges_by_epoch frozen<map<bigint, list<text>>>,\n" +
                        "  PRIMARY KEY (keyspace_name, table_name)" +
                        ')'));
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
        
                Map<Long, List<String>> primitiveRangesByEpoch = new LinkedHashMap<>();
                for (Map.Entry<org.apache.cassandra.tcm.Epoch, List<Range<Token>>> entry : state.migratingRangesByEpoch.entrySet())
                    primitiveRangesByEpoch.put(entry.getKey().getEpoch(), entry.getValue().stream().map(Objects::toString).collect(toImmutableList()));

                result.column("migrating_ranges_by_epoch", primitiveRangesByEpoch);
            }

            return result;
        }
    }

    public static final class CoordinationStatus extends AbstractVirtualTable
    {
        private CoordinationStatus(String keyspace)
        {
            super(parse(keyspace,
                        "Accord Coordination Status",
                        "CREATE TABLE accord_coordination_status(\n" +
                        "  node_id int,\n" +
                        "  epoch bigint,\n" +
                        "  start_time_micros bigint,\n" +
                        "  duration_millis bigint,\n" +
                        "  kind text,\n" +
                        "  domain text,\n" +
                        "  PRIMARY KEY (node_id, epoch, start_time_micros)" +
                        ')'));
        }

        @Override
        public DataSet data()
        {
            AccordService accord = (AccordService) AccordService.instance();
            SimpleDataSet result = new SimpleDataSet(metadata());

            for (TxnId txn : accord.node().coordinating().keySet())
            {
                result.row(txn.node.id, txn.epoch(), txn.hlc());
                result.column("duration_millis", Clock.Global.currentTimeMillis() - TimeUnit.MICROSECONDS.toMillis(txn.hlc()));
                result.column("kind", txn.kind().toString());
                result.column("domain", txn.domain().toString());
            }

            return result;
        }
    }

    public static class TxnBlockedByTable extends AbstractVirtualTable
    {
        enum Reason { Self, Txn, Key }
        private final UserType partitionKeyType;

        protected TxnBlockedByTable(String keyspace)
        {
            super(TableMetadata.builder(keyspace, "txn_blocked_by")
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .addPartitionKeyColumn("txn_id", UTF8Type.instance)
                               .addClusteringColumn("store_id", Int32Type.instance)
                               .addClusteringColumn("depth", Int32Type.instance)
                               .addClusteringColumn("blocked_by", UTF8Type.instance)
                               .addClusteringColumn("reason", UTF8Type.instance)
                               .addRegularColumn("save_status", UTF8Type.instance)
                               .addRegularColumn("execute_at", UTF8Type.instance)
                               .addRegularColumn("key", pkType(keyspace))
                               .build());
            partitionKeyType = pkType(keyspace);
        }

        private static UserType pkType(String keyspace)
        {
            return new UserType(keyspace, bytes("partition_key"),
                                Arrays.asList(FieldIdentifier.forQuoted("table"), FieldIdentifier.forQuoted("token")),
                                Arrays.asList(UTF8Type.instance, UTF8Type.instance), false);
        }

        private ByteBuffer pk(TokenKey pk)
        {
            var tm = Schema.instance.getTableMetadata(pk.table());
            return partitionKeyType.pack(UTF8Type.instance.decompose(tm.toString()),
                                         UTF8Type.instance.decompose(pk.token().toString()));
        }

        @Override
        public Iterable<UserType> userTypes()
        {
            return Arrays.asList(partitionKeyType);
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
                    if (processed.contains(blockedBy)) continue; // already listed
                    process(ds, shard, processed, userTxn, depth + 1, blockedBy, Reason.Txn, null);
                }
                for (TokenKey blockedBy : txn.blockedByKey)
                {
                    TxnId blocking = shard.keys.get(blockedBy);
                    if (processed.contains(blocking)) continue; // already listed
                    process(ds, shard, processed, userTxn, depth + 1, blocking, Reason.Key, () -> ds.column("key", pk(blockedBy)));
                }
            }
        }

        @Override
        public DataSet data()
        {
            throw new InvalidRequestException("Must select a single txn_id");
        }
    }

    private static TableMetadata parse(String keyspace, String comment, String query)
    {
        return CreateTableStatement.parse(query, keyspace)
                                   .comment(comment)
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .build();
    }
}
