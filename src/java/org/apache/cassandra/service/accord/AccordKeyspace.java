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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.Serialize;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.LocalPartitioner.LocalToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.accord.RouteJournalIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.accord.AccordConfigurationService.SyncStatus;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Clock.Global;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static accord.utils.Invariants.require;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.partitions.PartitionUpdate.singleRowUpdate;
import static org.apache.cassandra.db.rows.BTreeRow.singleCellRow;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.serializers.KeySerializers.blobMapToRanges;

public class AccordKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(AccordKeyspace.class);

    public static final String JOURNAL = "journal";
    public static final String COMMANDS_FOR_KEY = "commands_for_key";
    public static final String TOPOLOGIES = "topologies";
    public static final String EPOCH_METADATA = "epoch_metadata";
    public static final String JOURNAL_INDEX_NAME = "record";

    public static final Set<String> TABLE_NAMES = ImmutableSet.of(COMMANDS_FOR_KEY, TOPOLOGIES, EPOCH_METADATA, JOURNAL);

    // TODO (desired): implement a custom type so we can get correct sort order
    public static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance));

    private static final ClusteringIndexFilter FULL_PARTITION = new ClusteringIndexNamesFilter(BTreeSet.of(new ClusteringComparator(), Clustering.EMPTY), false);

    public static TableMetadata journalMetadata(String tableName, boolean index)
    {
        TableMetadata.Builder builder = parse(tableName,
                                                     "accord journal",
                                                     "CREATE TABLE %s ("
                                                     + "store_id int,"
                                                     + "type tinyint,"
                                                     + "id blob,"
                                                     + "descriptor bigint,"
                                                     + "offset int,"
                                                     + "user_version int,"
                                                     + "record blob,"
                                                     + "PRIMARY KEY((store_id, type, id), descriptor, offset)"
                                                     + ") WITH CLUSTERING ORDER BY (descriptor DESC, offset DESC)" +
                                                     " WITH compression = {'class':'NoopCompressor'};")
                                               .compaction(CompactionParams.lcs(emptyMap()))
                                               .bloomFilterFpChance(0.01)
                                               .partitioner(new LocalPartitioner(BytesType.instance));
        if (index)
            builder.indexes(Indexes.builder()
                                   .add(IndexMetadata.fromSchemaMetadata(JOURNAL_INDEX_NAME, IndexMetadata.Kind.CUSTOM, ImmutableMap.of("class_name", RouteJournalIndex.class.getCanonicalName(), "target", "record,user_version")))
                                   .build());
        return builder.build();
    }

    public static final TableMetadata Journal = journalMetadata(JOURNAL, true);

    private static ColumnMetadata getColumn(TableMetadata metadata, String name)
    {
        ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(name, true));
        if (column == null)
            throw new IllegalArgumentException(format("Unknown column %s for %s.%s", name, metadata.keyspace, metadata.name));
        return column;
    }

    private static final LocalPartitioner CFKPartitioner = new LocalPartitioner(BytesType.instance);
    public static final TableMetadata CommandsForKeys = commandsForKeysTable(COMMANDS_FOR_KEY);
    public static final CommandsForKeyAccessor CFKAccessor = new CommandsForKeyAccessor(CommandsForKeys);
    private static TableMetadata commandsForKeysTable(String tableName)
    {
        return parse(tableName,
              "accord commands per key",
              "CREATE TABLE %s ("
              + "key blob, "
              + "data blob, "
              + "PRIMARY KEY(key)"
              + ')'
               + " WITH compression = {'class':'NoopCompressor'};")
        .partitioner(CFKPartitioner)
        .compaction(CompactionParams.lcs(emptyMap()))
        .bloomFilterFpChance(0.01)
        .build();
    }

    public static class CommandsForKeyAccessor
    {
        final TableMetadata table;
        final ClusteringComparator keyComparator;
        final ColumnFilter allColumns;
        final ColumnMetadata data;

        final RegularAndStaticColumns columns;

        public CommandsForKeyAccessor(TableMetadata table)
        {
            this.table = table;
            this.keyComparator = table.partitionKeyAsClusteringComparator();
            this.allColumns = ColumnFilter.all(table);
            this.data = getColumn(table, "data");
            this.columns = new RegularAndStaticColumns(Columns.NONE, Columns.from(Lists.newArrayList(data)));
        }

        public static int getCommandStoreId(ByteBuffer partitionKey)
        {
            return partitionKey.getInt(partitionKey.position());
        }

        public static TokenKey getUserTableKey(TableId tableId, DecoratedKey key)
        {
            return getUserTableKey(tableId, key.getKey());
        }

        public static TokenKey getUserTableKey(TableId tableId, ByteBuffer partitionKey)
        {
            return TokenKey.serializer.deserializeWithPrefixAndImpliedLength(tableId, partitionKey, ByteBufferAccessor.instance, 4);
        }

        public static TokenKey getUserTableKey(TableId tableId, DecoratedKey key, IPartitioner partitioner)
        {
            return getUserTableKey(tableId, key.getKey(), partitioner);
        }

        public static TokenKey getUserTableKey(TableId tableId, ByteBuffer partitionKey, IPartitioner partitioner)
        {
            return TokenKey.serializer.deserializeWithPrefixAndImpliedLength(tableId, partitionKey, ByteBufferAccessor.instance, 4, partitioner);
        }

        public static DecoratedKey makeSystemTableKey(int commandStoreId, TokenKey key)
        {
            return CFKPartitioner.decorateKey(makeSystemTableKeyBytes(commandStoreId, key));
        }

        public static LocalToken makeSystemTableToken(int commandStore, TokenKey key)
        {
            return CFKPartitioner.getToken(makeSystemTableKeyBytes(commandStore, key));
        }

        public static ByteBuffer makeSystemTableKeyBytes(int commandStore, TokenKey key)
        {
            ByteBuffer result = ByteBuffer.allocate(4 + TokenKey.serializer.serializedSizeWithoutPrefix(key));
            result.putInt(commandStore);
            TokenKey.serializer.serializeWithoutPrefixOrLength(key, result);
            result.flip();
            return result;
        }

        public static ByteBuffer serializeUserTableKey(TokenKey key)
        {
            return TokenKey.serializer.serializeWithoutPrefixOrLength(key);
        }

        public CommandsForKey fromRow(TokenKey key, Row row)
        {
            Cell<?> cell = row.getCell(data);
            if (cell == null)
                return null;

            return Serialize.fromBytes(key, cell.buffer());
        }

        public static CommandsForKey load(int commandStoreId, TokenKey key)
        {
            return unsafeLoad(CFKAccessor, commandStoreId, key);
        }

        static CommandsForKey unsafeLoad(CommandsForKeyAccessor accessor, int commandStoreId, TokenKey key)
        {
            long timestampMicros = TimeUnit.MILLISECONDS.toMicros(Global.currentTimeMillis());
            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

            SinglePartitionReadCommand command = makeRead(accessor, commandStoreId, key, nowInSeconds);

            try (ReadExecutionController controller = command.executionController();
                 FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
            {
                if (!partitions.hasNext())
                    return null;

                try (RowIterator partition = partitions.next())
                {
                    Invariants.require(partition.hasNext());
                    Row row = partition.next();
                    ByteBuffer data = cellValue(row, accessor.data);
                    return Serialize.fromBytes(key, data);
                }
            }
            catch (Throwable t)
            {
                logger.error("Exception loading AccordCommandsForKey " + key, t);
                throw t;
            }
        }

        // TODO (expected): garbage-free filtering, reusing encoding
        public Row withoutRedundantCommands(TokenKey key, Row row, RedundantBefore.Entry redundantBefore)
        {
            Invariants.require(row.columnCount() == 1);
            Cell<?> cell = row.getCell(data);
            if (cell == null)
                return row;

            CommandsForKey current = Serialize.fromBytes(key, cell.buffer());
            if (current == null)
                return null;

            CommandsForKey updated = current.withRedundantBeforeAtLeast(redundantBefore.gcBefore());
            if (current == updated)
                return row;

            if (updated.isEmpty())
                return null;

            ByteBuffer buffer = Serialize.toBytesWithoutKey(updated);
            return BTreeRow.singleCellRow(Clustering.EMPTY, BufferCell.live(data, cell.timestamp(), buffer));
        }

        public static SinglePartitionReadCommand makeRead(int storeId, TokenKey key, int nowInSeconds)
        {
            return makeRead(CFKAccessor, storeId, key, nowInSeconds);
        }

        private static SinglePartitionReadCommand makeRead(CommandsForKeyAccessor accessor, int storeId, TokenKey key, long nowInSeconds)
        {
            return SinglePartitionReadCommand.create(accessor.table, nowInSeconds,
                                                     accessor.allColumns,
                                                     RowFilter.none(),
                                                     DataLimits.NONE,
                                                     makeSystemTableKey(storeId, key),
                                                     FULL_PARTITION);
        }

        private static PartitionUpdate makeUpdate(int storeId, TokenKey key, CommandsForKey commandsForKey, Object serialized, long timestampMicros)
        {
            ByteBuffer bytes;
            if (serialized instanceof ByteBuffer) bytes = (ByteBuffer) serialized;
            else bytes = Serialize.toBytesWithoutKey(commandsForKey);
            return makeUpdate(storeId, key, timestampMicros, bytes);
        }

        @VisibleForTesting
        public static PartitionUpdate makeUpdate(int storeId, TokenKey key, long timestampMicros, ByteBuffer bytes)
        {
            return singleRowUpdate(CFKAccessor.table,
                                   CommandsForKeyAccessor.makeSystemTableKey(storeId, key),
                                   singleCellRow(Clustering.EMPTY, BufferCell.live(CFKAccessor.data, timestampMicros, bytes)));
        }

        public static Runnable systemTableUpdater(int storeId, TokenKey key, CommandsForKey update, Object serialized, long timestampMicros)
        {
            PartitionUpdate upd = makeUpdate(storeId, key, update, serialized, timestampMicros);
            return () -> {
                ColumnFamilyStore cfs = AccordColumnFamilyStores.commandsForKey;
                try (OpOrder.Group group = Keyspace.writeOrder.start())
                {
                    cfs.getCurrentMemtable().put(upd, UpdateTransaction.NO_OP, group, true);
                }
            };
        }

        /**
         * Calculates token bounds based on key prefixes.
         */
        public static void findAllKeysBetween(int commandStore, TableId tableId, IPartitioner partitioner,
                                              TokenKey start, boolean startInclusive,
                                              TokenKey end, boolean endInclusive,
                                              Consumer<TokenKey> consumer)
        {

            Token startToken = CommandsForKeyAccessor.makeSystemTableToken(commandStore, start);
            Token endToken = CommandsForKeyAccessor.makeSystemTableToken(commandStore, end);

            if (start.isTableSentinel())
                startInclusive = true;
            if (end.isTableSentinel())
                endInclusive = true;

            PartitionPosition startPosition = startInclusive ? startToken.minKeyBound() : startToken.maxKeyBound();
            PartitionPosition endPosition = endInclusive ? endToken.maxKeyBound() : endToken.minKeyBound();
            AbstractBounds<PartitionPosition> bounds;
            if (startInclusive && endInclusive)
                bounds = new Bounds<>(startPosition, endPosition);
            else if (endInclusive)
                bounds = new Range<>(startPosition, endPosition);
            else if (startInclusive)
                bounds = new IncludingExcludingBounds<>(startPosition, endPosition);
            else
                bounds = new ExcludingBounds<>(startPosition, endPosition);

            ColumnFamilyStore baseCfs = AccordColumnFamilyStores.commandsForKey;
            try (OpOrder.Group baseOp = baseCfs.readOrdering.start();
                 WriteContext writeContext = baseCfs.keyspace.getWriteHandler().createContextForRead();
                 CloseableIterator<DecoratedKey> iter = keyIterator(CommandsForKeys, bounds))
            {
                // Need the second try to handle callback errors vs read errors.
                // Callback will see the read errors, but if the callback fails the outer try will see those errors
                while (iter.hasNext())
                {
                    TokenKey pk = CommandsForKeyAccessor.getUserTableKey(tableId, iter.next(), partitioner);
                    consumer.accept(pk);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        /**
         * Returns a DecoratedKey iterator for the given range. Skips reading data files for sstable formats with a partition index file
         */
        private static CloseableIterator<DecoratedKey> keyIterator(Memtable memtable, AbstractBounds<PartitionPosition> range)
        {
            // TODO (required): why are we replacing the right bound with max bound?
            AbstractBounds<PartitionPosition> memtableRange = range.withNewRight(memtable.metadata().partitioner.getMinimumToken().maxKeyBound());
            DataRange dataRange = new DataRange(memtableRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
            UnfilteredPartitionIterator iter = memtable.partitionIterator(ColumnFilter.NONE, dataRange, SSTableReadsListener.NOOP_LISTENER);

            int rangeStartCmpMin = range.isStartInclusive() ? 0 : 1;
            int rangeEndCmpMax = range.isEndInclusive() ? 0 : -1;

            return new AbstractIterator<>()
            {
                @Override
                protected DecoratedKey computeNext()
                {
                    while (iter.hasNext())
                    {
                        DecoratedKey key = iter.next().partitionKey();
                        if (key.compareTo(range.left) < rangeStartCmpMin)
                            continue;

                        if (key.compareTo(range.right) > rangeEndCmpMax)
                            break;

                        return key;
                    }
                    return endOfData();
                }

                @Override
                public void close()
                {
                    iter.close();
                }
            };
        }

        private static CloseableIterator<DecoratedKey> keyIterator(TableMetadata metadata, AbstractBounds<PartitionPosition> range) throws IOException
        {
            ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
            ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(range));

            List<CloseableIterator<?>> closeableIterators = new ArrayList<>();
            List<Iterator<DecoratedKey>> iterators = new ArrayList<>();

            try
            {
                for (Memtable memtable : view.memtables)
                {
                    CloseableIterator<DecoratedKey> iter = keyIterator(memtable, range);
                    iterators.add(iter);
                    closeableIterators.add(iter);
                }

                for (SSTableReader sstable : view.sstables)
                {
                    CloseableIterator<DecoratedKey> iter = sstable.keyIterator(range);
                    iterators.add(iter);
                    closeableIterators.add(iter);
                }
            }
            catch (Throwable e)
            {
                for (CloseableIterator<?> iter: closeableIterators)
                {
                    try
                    {
                        iter.close();
                    }
                    catch (Throwable e2)
                    {
                        e.addSuppressed(e2);
                    }
                }
                throw e;
            }

            return MergeIterator.get(iterators, DecoratedKey::compareTo, new MergeIterator.Reducer.Trivial<>());
        }
    }

    public static final TableMetadata Topologies =
        parse(TOPOLOGIES,
              "accord topologies",
              "CREATE TABLE %s (" +
              "epoch bigint primary key, " +
              "sync_state int, " +
              "pending_sync_notify set<int>, " + // nodes that need to be told we're synced
              "remote_sync_complete set<int>, " +  // nodes that have told us they're synced
              "closed map<blob, blob>, " +
              "retired map<blob, blob>" +
              ')').build();

    public static final TableMetadata EpochMetadata =
        parse(EPOCH_METADATA,
              "global epoch info",
              "CREATE TABLE %s (" +
              "key int primary key, " +
              "min_epoch bigint, " +
              "max_epoch bigint " +
              ')').build();

    private static TableMetadata.Builder parse(String name, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, name), ACCORD_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(ACCORD_KEYSPACE_NAME, name))
                                   .comment(description)
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90));
    }

    private static void flush(TableMetadata table)
    {
        Keyspace.open(table.keyspace).getColumnFamilyStore(table.id).forceBlockingFlush(ColumnFamilyStore.FlushReason.ACCORD);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(ACCORD_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), UserFunctions.none());
    }

    public static Tables TABLES = Tables.of(CommandsForKeys, Topologies, EpochMetadata, Journal);
    public static Tables tables()
    {
        return TABLES;
    }

    public static void truncateAllCaches()
    {
        Keyspace ks = Keyspace.open(ACCORD_KEYSPACE_NAME);
        for (String table : new String[]{ CommandsForKeys.name })
        {
            if (!ks.getColumnFamilyStore(table).isEmpty())
                ks.getColumnFamilyStore(table).truncateBlocking();
        }
    }

    private static <T> ByteBuffer cellValue(Cell<T> cell)
    {
        return cell.accessor().toBuffer(cell.value());
    }

    // TODO: convert to byte array
    private static ByteBuffer cellValue(Row row, ColumnMetadata column)
    {
        Cell<?> cell = row.getCell(column);
        return (cell != null && !cell.isTombstone()) ? cellValue(cell) : null;
    }

    public static class EpochDiskState
    {
        public static final EpochDiskState EMPTY = new EpochDiskState(0, 0);
        public final long minEpoch;
        public final long maxEpoch;

        private EpochDiskState(long minEpoch, long maxEpoch)
        {
            Invariants.requireArgument(minEpoch >= 0, "Min Epoch %d < 0", minEpoch);
            Invariants.requireArgument(maxEpoch >= minEpoch, "Max epoch %d < min %d", maxEpoch, minEpoch);
            this.minEpoch = minEpoch;
            this.maxEpoch = maxEpoch;
        }

        public static EpochDiskState create(long minEpoch, long maxEpoch)
        {
            if (minEpoch == maxEpoch && minEpoch == 0)
                return EMPTY;
            return new EpochDiskState(minEpoch, maxEpoch);
        }

        public static EpochDiskState create(long epoch)
        {
            return create(epoch, epoch);
        }

        public boolean isEmpty()
        {
            return minEpoch == maxEpoch && maxEpoch == 0;
        }

        @VisibleForTesting
        EpochDiskState withNewMaxEpoch(long epoch)
        {
            Invariants.requireArgument(epoch > maxEpoch, "Epoch %d <= %d (max)", epoch, maxEpoch);
            return EpochDiskState.create(Math.max(1, minEpoch), epoch);
        }

        private EpochDiskState withNewMinEpoch(long epoch)
        {
            Invariants.requireArgument(epoch > minEpoch, "epoch %d <= %d (min)", epoch, minEpoch);
            Invariants.requireArgument(epoch <= maxEpoch, "epoch %d > %d (max)", epoch, maxEpoch);
            return EpochDiskState.create(epoch, maxEpoch);
        }

        @Override
        public String toString()
        {
            return "EpochDiskState{" +
                   "minEpoch=" + minEpoch +
                   ", maxEpoch=" + maxEpoch +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EpochDiskState diskState = (EpochDiskState) o;
            return minEpoch == diskState.minEpoch && maxEpoch == diskState.maxEpoch;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class JournalColumns
    {
        static final ClusteringComparator keyComparator = Journal.partitionKeyAsClusteringComparator();
        static final CompositeType partitionKeyType = (CompositeType) Journal.partitionKeyType;
        public static final ColumnMetadata store_id = getColumn(Journal, "store_id");
        public static final ColumnMetadata type = getColumn(Journal, "type");
        public static final ColumnMetadata id = getColumn(Journal, "id");
        public static final ColumnMetadata record = getColumn(Journal, "record");
        public static final ColumnMetadata user_version = getColumn(Journal, "user_version");

        public static DecoratedKey decorate(JournalKey key)
        {
            ByteBuffer id = ByteBuffer.allocate(CommandSerializers.txnId.serializedSize());
            CommandSerializers.txnId.serialize(key.id, id);
            id.flip();
            ByteBuffer pk = keyComparator.make(key.commandStoreId, (byte)key.type.id, id).serializeAsPartitionKey();
            Invariants.require(getTxnId(splitPartitionKey(pk)).equals(key.id));
            return Journal.partitioner.decorateKey(pk);
        }

        public static ByteBuffer[] splitPartitionKey(DecoratedKey key)
        {
            return JournalColumns.partitionKeyType.split(key.getKey());
        }

        public static ByteBuffer[] splitPartitionKey(ByteBuffer key)
        {
            return JournalColumns.partitionKeyType.split(key);
        }

        public static int getStoreId(DecoratedKey pk)
        {
            return getStoreId(splitPartitionKey(pk));
        }

        public static int getStoreId(ByteBuffer[] partitionKeyComponents)
        {
            return Int32Type.instance.compose(partitionKeyComponents[store_id.position()]);
        }

        public static JournalKey.Type getType(ByteBuffer[] partitionKeyComponents)
        {
            return JournalKey.Type.fromId(ByteType.instance.compose(partitionKeyComponents[type.position()]));
        }

        public static TxnId getTxnId(DecoratedKey key)
        {
            return getTxnId(splitPartitionKey(key));
        }

        public static TxnId getTxnId(ByteBuffer[] partitionKeyComponents)
        {
            ByteBuffer buffer = partitionKeyComponents[id.position()];
            return CommandSerializers.txnId.deserialize(buffer, buffer.position());
        }

        public static JournalKey getJournalKey(DecoratedKey key)
        {
            ByteBuffer[] parts = splitPartitionKey(key);
            return new JournalKey(getTxnId(parts), getType(parts), getStoreId(parts));
        }
    }

    private static EpochDiskState saveEpochDiskState(EpochDiskState diskState)
    {
        String cql = "INSERT INTO " + ACCORD_KEYSPACE_NAME + '.' + EPOCH_METADATA + ' ' +
                     "(key, min_epoch, max_epoch) VALUES (0, ?, ?);";
        executeInternal(cql, diskState.minEpoch, diskState.maxEpoch);
        return diskState;
    }

    @Nullable
    @VisibleForTesting
    public static EpochDiskState loadEpochDiskState()
    {
        String cql = "SELECT * FROM " + ACCORD_KEYSPACE_NAME + '.' + EPOCH_METADATA + ' ' +
                     "WHERE key=0";
        UntypedResultSet result = executeInternal(format(cql, ACCORD_KEYSPACE_NAME, EPOCH_METADATA));
        if (result.isEmpty())
            return null;
        UntypedResultSet.Row row = result.one();
        return EpochDiskState.create(row.getLong("min_epoch"), row.getLong("max_epoch"));
    }

    /**
     * Update the disk state for this epoch, if it's higher than the one we have one disk.
     * <p>
     * This is meant to be called before any update involving the new epoch, not after. This way if the update
     * fails, we can detect and cleanup. If we updated disk state after an update and it failed, we could "forget"
     * about (now acked) topology updates after a restart.
     */
    private static EpochDiskState maybeUpdateMaxEpoch(EpochDiskState diskState, long epoch)
    {
        if (diskState.isEmpty())
            return saveEpochDiskState(EpochDiskState.create(epoch));
        Invariants.requireArgument(epoch >= diskState.minEpoch, "Epoch %d < %d (min)", epoch, diskState.minEpoch);
        if (epoch > diskState.maxEpoch)
        {
            diskState = diskState.withNewMaxEpoch(epoch);
            saveEpochDiskState(diskState);
        }
        return diskState;
    }

    public static EpochDiskState saveTopology(Topology topology, EpochDiskState diskState)
    {
        return maybeUpdateMaxEpoch(diskState, topology.epoch());
    }

    public static EpochDiskState markRemoteTopologySync(Node.Id node, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET remote_sync_complete = remote_sync_complete + ? WHERE epoch = ?";
        executeInternal(cql,
                        Collections.singleton(node.id), epoch);
        return diskState;
    }

    public static EpochDiskState markClosed(Ranges ranges, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET closed = closed + ? WHERE epoch = ?";
        executeInternal(cql, KeySerializers.rangesToBlobMap(ranges), epoch);
        return diskState;
    }

    public static EpochDiskState markRetired(Ranges ranges, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET retired = retired + ? WHERE epoch = ?";
        executeInternal(cql, KeySerializers.rangesToBlobMap(ranges), epoch);
        return diskState;
    }

    public static EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> pending, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET sync_state = ?, pending_sync_notify = ? WHERE epoch = ?";
        executeInternal(cql,
                        SyncStatus.NOTIFYING.ordinal(),
                        pending.stream().map(i -> i.id).collect(Collectors.toSet()),
                        epoch);
        return diskState;
    }

    public static EpochDiskState markLocalSyncAck(Node.Id node, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET pending_sync_notify = pending_sync_notify - ? WHERE epoch = ?";
        executeInternal(cql,
                        Collections.singleton(node.id), epoch);
        return diskState;
    }

    public static EpochDiskState setCompletedLocalSync(long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET sync_state = ?, pending_sync_notify = {} WHERE epoch = ?";
        executeInternal(cql,
                        SyncStatus.COMPLETED.ordinal(),
                        epoch);
        return diskState;
    }

    public static EpochDiskState truncateTopologyUntil(final long epoch, EpochDiskState diskState)
    {
        while (diskState.minEpoch < epoch)
        {
            long delete = diskState.minEpoch;
            diskState = diskState.withNewMinEpoch(delete + 1);
            saveEpochDiskState(diskState);
            String cql = "DELETE FROM " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                         "WHERE epoch = ?";
            executeInternal(cql, delete);
        }
        return diskState;
    }

    public interface TopologyLoadConsumer
    {
        void load(long epoch, SyncStatus syncStatus, Set<Node.Id> pendingSyncNotify, Set<Node.Id> remoteSyncComplete, Ranges closed, Ranges redundant);
    }

    @VisibleForTesting
    public static void loadEpoch(long epoch, TopologyLoadConsumer consumer) throws IOException
    {
        UntypedResultSet result = executeInternal(String.format("SELECT * FROM %s.%s WHERE epoch=?", ACCORD_KEYSPACE_NAME, TOPOLOGIES), epoch);
        if (result.isEmpty())
        {
            // topology updates disk state for epoch but doesn't save the topology to the table, so there maybe an epoch we know about, but no fields are present
            consumer.load(epoch, SyncStatus.NOT_STARTED, Collections.emptySet(), Collections.emptySet(), Ranges.EMPTY, Ranges.EMPTY);
            return;
        }
        require(!result.isEmpty(), "Nothing found for epoch %d", epoch);
        UntypedResultSet.Row row = result.one();

        SyncStatus syncStatus = row.has("sync_state")
                                ? SyncStatus.values()[row.getInt("sync_state")]
                                : SyncStatus.NOT_STARTED;
        Set<Node.Id> pendingSyncNotify = row.has("pending_sync_notify")
                                         ? row.getSet("pending_sync_notify", Int32Type.instance).stream().map(Node.Id::new).collect(Collectors.toSet())
                                         : Collections.emptySet();
        Set<Node.Id> remoteSyncComplete = row.has("remote_sync_complete")
                                          ? row.getSet("remote_sync_complete", Int32Type.instance).stream().map(Node.Id::new).collect(Collectors.toSet())
                                          : Collections.emptySet();
        Ranges closed = row.has("closed") ? blobMapToRanges(row.getMap("closed", BytesType.instance, BytesType.instance)) : Ranges.EMPTY;
        Ranges redundant = row.has("redundant") ? blobMapToRanges(row.getMap("redundant", BytesType.instance, BytesType.instance)) : Ranges.EMPTY;

        consumer.load(epoch, syncStatus, pendingSyncNotify, remoteSyncComplete, closed, redundant);
    }

    public static EpochDiskState loadTopologies(TopologyLoadConsumer consumer)
    {
        try
        {
            EpochDiskState diskState = loadEpochDiskState();
            if (diskState == null)
                return EpochDiskState.EMPTY;

            for (long epoch = diskState.minEpoch; epoch < diskState.maxEpoch; epoch++)
                loadEpoch(epoch, consumer);

            return diskState;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public static void unsafeClear()
    {
        for (ColumnFamilyStore store : Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.truncateBlockingWithoutSnapshot();
    }

    public static class AccordColumnFamilyStores
    {
        public static final ColumnFamilyStore journal = Schema.instance.getColumnFamilyStoreInstance(Journal.id);
        public static final ColumnFamilyStore commandsForKey = Schema.instance.getColumnFamilyStoreInstance(CommandsForKeys.id);
    }
}
