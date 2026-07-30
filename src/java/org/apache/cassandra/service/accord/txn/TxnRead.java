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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.api.Data;
import accord.api.Key;
import accord.api.Read;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDSerializer;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.require;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.service.accord.IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS;
import static org.apache.cassandra.service.accord.serializers.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.CAS_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.USER;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.ArraySerializers.skipArray;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class TxnRead extends AbstractKeySorted<TxnNamedRead> implements Read
{
    private static final TxnRead EMPTY_KEY = new TxnRead(TableMetadatas.none(), Domain.Key, null);
    private static final TxnRead EMPTY_RANGE = new TxnRead(TableMetadatas.none(), Domain.Range, null);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY_KEY);
    private static final Comparator<TxnNamedRead> TXN_NAMED_READ_KEY_COMPARATOR = Comparator.comparing(a -> ((PartitionKey) a.key()));
    private static final byte TYPE_EMPTY_KEY = 0;
    private static final byte TYPE_EMPTY_RANGE = 1;
    private static final byte TYPE_NOT_EMPTY = 2;
    private static final byte TYPE_IMPORT = 3;

    public static TxnRead empty(Domain domain)
    {
        switch (domain)
        {
            default:
                throw new IllegalStateException("Unhandled domain " + domain);
            case Key:
                return EMPTY_KEY;
            case Range:
                return EMPTY_RANGE;
        }
    }

    final TableMetadatas tables;
    // Cassandra's consistency level used by Accord to safely read data written outside of Accord
    @Nullable
    private final ConsistencyLevel cassandraConsistencyLevel;

    // Specifies the domain in case the TxnRead is empty and it can't be inferred
    private final Domain domain;

    @Nullable
    private final ImportMetadata importMetadata;

    private TxnRead(TableMetadatas tables, Domain domain, @Nullable ImportMetadata importMetadata)
    {
        super(new TxnNamedRead[0], domain);
        this.tables = tables;
        this.domain = domain;
        this.cassandraConsistencyLevel = null;
        this.importMetadata = importMetadata;
    }

    private TxnRead(TableMetadatas tables, @Nonnull TxnNamedRead[] items, @Nullable ConsistencyLevel cassandraConsistencyLevel, @Nullable ImportMetadata importMetadata)
    {
        super(items, items[0].key().domain());
        this.tables = tables;
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read: %s", cassandraConsistencyLevel);
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        this.domain = items[0].key().domain();
        this.importMetadata = importMetadata;
        // TODO (expected): relax this condition, require only that it holds for each equal byte[]
        //  right now this means we don't permit two different range queries in the same transaction touching adjacent ranges
        //  this is a pretty weak restriction and doesn't interfere with current CQL capabilities, but should be addressed eventually
        require(domain == Domain.Key || ((Ranges)keys()).mergeTouching() == keys());
    }

    private TxnRead(TableMetadatas tables, @Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel cassandraConsistencyLevel, @Nullable ImportMetadata importMetadata)
    {
        super(items, items.get(0).key().domain());
        this.tables = tables;
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read: %s", cassandraConsistencyLevel);
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        this.domain = items.get(0).key().domain();
        this.importMetadata = importMetadata;
        require(domain == Domain.Key || ((Ranges)keys()).mergeTouching() == keys());
    }

    private static void sortReads(List<TxnNamedRead> reads)
    {
        if (reads.size() > 1)
            reads.sort(TXN_NAMED_READ_KEY_COMPARATOR);
    }

    public static TxnRead createTxnRead(TableMetadatas tables, @Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel consistencyLevel, Domain domain)
    {
        if (items.isEmpty())
            return empty(domain);
        sortReads(items);
        return new TxnRead(tables, items, consistencyLevel, null);
    }

    public static TxnRead createTxnRead(TableMetadatas tables, @Nonnull List<TxnNamedRead> items, @Nullable ConsistencyLevel consistencyLevel, Domain domain, @Nullable ImportMetadata importMetadata)
    {
        if (items.isEmpty())
            return new TxnRead(TableMetadatas.none(), domain, importMetadata);
        sortReads(items);
        return new TxnRead(tables, items, consistencyLevel, importMetadata);
    }

    public static TxnRead createSerialRead(List<SinglePartitionReadCommand> readCommands, ConsistencyLevel consistencyLevel, TableMetadatasAndKeys.KeyCollector keyCollector)
    {
        List<TxnNamedRead> reads = new ArrayList<>(readCommands.size());
        for (int i = 0; i < readCommands.size(); i++)
        {
            SinglePartitionReadCommand readCommand = readCommands.get(i);
            reads.add(new TxnNamedRead(txnDataName(USER, i), keyCollector.collect(readCommand.metadata(), readCommand.partitionKey()), readCommand, keyCollector.tables));
        }
        sortReads(reads);
        return new TxnRead(keyCollector.tables, reads, consistencyLevel, null);
    }

    public static TxnRead createCasRead(SinglePartitionReadCommand readCommand, ConsistencyLevel consistencyLevel, TableMetadatasAndKeys tablesAndKeys)
    {
        TxnNamedRead read = new TxnNamedRead(txnDataName(CAS_READ), (PartitionKey) tablesAndKeys.keys.get(0), readCommand, tablesAndKeys.tables);
        return new TxnRead(tablesAndKeys.tables, ImmutableList.of(read), consistencyLevel, null);
    }

    // A read that declares it will read from keys but doesn't actually read any data so dependent transactions will
    // still be applied first
    public static TxnRead createNoOpRead(Keys keys)
    {
        List<TxnNamedRead> reads = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++)
            reads.add(new TxnNamedRead(txnDataName(USER, i), keys.get(i), null));
        return new TxnRead(TableMetadatas.none(), reads, null, null);
    }

    public static TxnRead createRangeRead(TableMetadatas tables, PartitionRangeReadCommand command, AbstractBounds<PartitionPosition> range, ConsistencyLevel consistencyLevel)
    {
        return new TxnRead(tables, ImmutableList.of(new TxnNamedRead(txnDataName(USER), range, command, tables)), consistencyLevel, null);
    }

    // SSTable imports are performed through read range txn's. The value of the reads are not returned back to the client, rather
    // the ImportTxn has custom logic to move SSTables in the pending directory to the live set. Because these txn's are performed
    // using range reads, clients can see a window of inconsistency for read txn's that are concurrent with the ImportTxn. However,
    // data will always be consistent.
    public static TxnRead createImport(TableMetadatas tables, TokenRange range, UUID importID, TimeUUID planId, long streamingEpoch)
    {
        ImportMetadata importMetadata = new ImportMetadata(importID, planId, streamingEpoch);
        return new TxnRead(tables, ImmutableList.of(new TxnNamedRead(txnDataName(USER), range, null)), null, importMetadata);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (TxnNamedRead read : items)
            size += read.estimatedSizeOnHeap();
        return size;
    }

    @Override
    int compareNonKeyFields(TxnNamedRead left, TxnNamedRead right)
    {
        return Integer.compare(left.txnDataName(), right.txnDataName());
    }

    ReadCommand deserialize(int i)
    {
        return get(i).deserialize(tables);
    }

    @Override
    Seekable getKey(TxnNamedRead read)
    {
        return read.key();
    }

    @Override
    TxnNamedRead[] newArray(int size)
    {
        return new TxnNamedRead[size];
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return itemKeys;
    }

    public ConsistencyLevel cassandraConsistencyLevel()
    {
        return cassandraConsistencyLevel;
    }

    @Override
    public Read slice(Ranges ranges)
    {
        return select(itemKeys.slice(ranges, Minimal));
    }

    @Override
    public Read intersecting(Participants<?> participants)
    {
        return select(itemKeys.intersecting(participants, Minimal));
    }

    private Read select(Seekables<?, ?> select)
    {
        if (select == keys())
            return this;

        List<TxnNamedRead> reads = new ArrayList<>(select.size());
        switch (select.domain())
        {
            case Key:
            {
                Keys keys = (Keys) select;
                int i = 0, j = 0;
                while (i < select.size() && j < items.length)
                {
                    Key key = keys.get(i);
                    TxnNamedRead read = items[j];
                    int c = key.compareTo((Key)read.key());
                    if (c < 0) ++i;
                    else if (c > 0) ++j;
                    else
                    {
                        reads.add(read);
                        ++j;
                    }
                }
                break;
            }
            case Range:
            {
                Ranges ranges = (Ranges) select;
                int i = 0, j = 0;
                while (i < select.size() && j < items.length)
                {
                    Range range = ranges.get(i);
                    TxnNamedRead read = items[j];
                    int c = range.compareIntersecting((Range) read.key());
                    if (c < 0) ++i;
                    else if (c > 0) ++j;
                    else
                    {
                        reads.add(read.slice(range));
                        ++j;
                    }
                }
                break;
            }
            default:
                throw new UnhandledEnum(select.domain());
        }

        return createTxnRead(tables, reads, cassandraConsistencyLevel, select.domain(), importMetadata);
    }

    @Override
    public Read merge(Read read)
    {
        TxnRead that = (TxnRead)read;
        List<TxnNamedRead> reads = new ArrayList<>(items.length);

        switch (domain)
        {
            default: throw new UnhandledEnum(domain);
            case Key:
            {
                int i = 0, j = 0;
                while (i < items.length && j < that.items.length)
                {
                    TxnNamedRead r1 = this.items[i], r2 = that.items[j];
                    int c = compareKey(r1, r2);
                    if (c <= 0)
                    {
                        reads.add(r1);
                        ++i;
                        if (c == 0)
                            ++j;
                    }
                    else
                    {
                        reads.add(r2);
                        ++j;
                    }
                }
                break;
            }
            case Range:
            {
                int i = 0, j = 0;
                TxnNamedRead pending = null;
                while (i < items.length && j < that.items.length)
                {
                    TxnNamedRead r1 = this.items[i], r2 = that.items[j];
                    int c = compareRange(r1, r2);
                    TxnNamedRead add;
                    if (c == 0)
                    {
                        add = r1.merge(r2);
                        ++i;
                        ++j;
                    }
                    else if (c < 0)
                    {
                        add = r1;
                        ++i;
                    }
                    else
                    {
                        add = r2;
                        ++j;
                    }

                    if (pending == null) pending = add;
                    else
                    {
                        c = compareRange(pending, add);
                        if (c < 0)
                        {
                            reads.add(pending);
                            pending = add;
                        }
                        else
                        {
                            require(c == 0);
                            pending = pending.merge(add);
                        }
                    }
                }
                if (pending != null)
                    reads.add(pending);
                break;
            }
        }

        return createTxnRead(tables, reads, cassandraConsistencyLevel, that.domain, importMetadata);
    }

    public void unmemoize()
    {
        for (TxnNamedRead read : items)
            read.unmemoize();
    }

    @Override
    public AsyncChain<Data> read(SafeCommandStore safeStore, Seekable key, Timestamp executeAt)
    {
        return readDirect(safeStore.commandStore(), key, executeAt);
    }

    @Override
    public AsyncChain<Data> readDirect(CommandStore commandStore, Seekable key, Timestamp executeAt)
    {
        if (key == null)
            return AsyncChains.success(new TxnData());

        AccordExecutor executor = ((AccordCommandStore)commandStore).executor();

        if (isUsedForImport())
            return items[0].performSSTableImport(executor, importMetadata, executeAt);

        if (items.length == 1 && key.equals(items[0].key()))
            return items[0].read(executor, tables, cassandraConsistencyLevel, key, executeAt);

        List<AsyncChain<Data>> results = new ArrayList<>();
            forEachWithKey(key, read -> results.add(read.read(executor, tables, cassandraConsistencyLevel, key, executeAt)));

        if (results.isEmpty())
            return AsyncChains.success(new TxnData());

        return AsyncChains.reduce(results, Data::merge);
    }

    // Metadata that is needed to be kept track of for ImportTxns
    public static class ImportMetadata
    {
        private final UUID importID;
        private final TimeUUID planId;
        private final Long streamingEpoch;

        public ImportMetadata(UUID importID, TimeUUID planId, Long streamingEpoch)
        {
            this.importID = importID;
            this.planId = planId;
            this.streamingEpoch = streamingEpoch;
        }

        public UUID getImportID()
        {
            return importID;
        }

        public TimeUUID getPlanId()
        {
            return planId;
        }

        public Long getStreamingEpoch()
        {
            return streamingEpoch;
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof ImportMetadata && equals((ImportMetadata) that);
        }

        public boolean equals(ImportMetadata that)
        {
            return Objects.equals(this.importID, that.importID)
                   && Objects.equals(this.planId, that.planId)
                   && (Objects.equals(this.streamingEpoch, that.streamingEpoch));
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public static final VersionedSerializer<ImportMetadata, Version> serializer = new VersionedSerializer<>()
        {
            @Override
            public void serialize(ImportMetadata importMetadata, DataOutputPlus out, Version version) throws IOException
            {
                UUIDSerializer.serializer.serialize(importMetadata.importID, out);
                TimeUUID.Serializer.instance.serialize(importMetadata.planId, out);
                out.writeLong(importMetadata.streamingEpoch);
            }

            public void skip(DataInputPlus in, Version version) throws IOException
            {
                UUIDSerializer.serializer.skip(in);
                TimeUUID.Serializer.instance.skip(in);
                in.readLong();
            }

            @Override
            public ImportMetadata deserialize(DataInputPlus in, Version version) throws IOException
            {
                UUID importID = UUIDSerializer.serializer.deserialize(in);
                TimeUUID planId = TimeUUID.Serializer.instance.deserialize(in);
                Long streamingEpoch = in.readLong();
                return new ImportMetadata(importID, planId, streamingEpoch);
            }

            @Override
            public long serializedSize(ImportMetadata importMetadata, Version version)
            {
                long size = 0;
                size += UUIDSerializer.serializer.serializedSize(importMetadata.importID);
                size += TimeUUID.Serializer.instance.serializedSize(importMetadata.planId);
                size += TypeSizes.LONG_SIZE;
                return size;
            }
        };
    }

    @Override
    public boolean isUsedForImport()
    {
        return importMetadata != null;
    }

    public static final ParameterisedVersionedSerializer<TxnRead, TableMetadatasAndKeys, Version> serializer = new ParameterisedVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnRead read, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
        {
            if (read.isUsedForImport())
            {
                out.write(TYPE_IMPORT);
                serializeArray(read.items, tablesAndKeys, out, version, TxnNamedRead.serializer);
                serializeNullable(read.cassandraConsistencyLevel, out, consistencyLevelSerializer);
                serializeNullable(read.importMetadata, out, version, ImportMetadata.serializer);
            }
            else if (read.items.length > 0)
            {
                out.write(TYPE_NOT_EMPTY);
                serializeArray(read.items, tablesAndKeys, out, version, TxnNamedRead.serializer);
                serializeNullable(read.cassandraConsistencyLevel, out, consistencyLevelSerializer);
            }
            else
            {
                out.write(read.domain == Domain.Key ? TYPE_EMPTY_KEY : TYPE_EMPTY_RANGE);
            }
        }

        public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            byte type = in.readByte();
            switch (type)
            {
                case TYPE_EMPTY_KEY:
                case TYPE_EMPTY_RANGE:
                    return;
                case TYPE_NOT_EMPTY:
                    skipArray(tablesAndKeys, in, version, TxnNamedRead.serializer);
                    deserializeNullable(in, consistencyLevelSerializer);
                    return;
                case TYPE_IMPORT:
                    skipArray(tablesAndKeys, in, version, TxnNamedRead.serializer);
                    deserializeNullable(in, consistencyLevelSerializer);
                    deserializeNullable(in, version, ImportMetadata.serializer);
                    return;
                default:
                    throw new IllegalStateException("Unhandled type " + type);
            }
        }


        @Override
        public TxnRead deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            byte type = in.readByte();
            switch (type)
            {
                default:
                    throw new IllegalStateException("Unhandled type " + type);
                case TYPE_EMPTY_KEY:
                    return EMPTY_KEY;
                case TYPE_EMPTY_RANGE:
                    return EMPTY_RANGE;
                case TYPE_NOT_EMPTY:
                {
                    TxnNamedRead[] items = deserializeArray(tablesAndKeys, in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
                    ConsistencyLevel consistencyLevel = deserializeNullable(in, consistencyLevelSerializer);
                    return new TxnRead(tablesAndKeys.tables, items, consistencyLevel, null);
                }
                case TYPE_IMPORT:
                {
                    TxnNamedRead[] items = deserializeArray(tablesAndKeys, in, version, TxnNamedRead.serializer, TxnNamedRead[]::new);
                    ConsistencyLevel consistencyLevel = deserializeNullable(in, consistencyLevelSerializer);
                    ImportMetadata importMetadata = deserializeNullable(in, version, ImportMetadata.serializer);
                    return new TxnRead(tablesAndKeys.tables, items, consistencyLevel, importMetadata);
                }
            }
        }

        @Override
        public long serializedSize(TxnRead read, TableMetadatasAndKeys tablesAndKeys, Version version)
        {
            long size = 1; // type
            if (read.isUsedForImport())
            {
                size += serializedArraySize(read.items, tablesAndKeys, version, TxnNamedRead.serializer);
                size += serializedNullableSize(read.cassandraConsistencyLevel, consistencyLevelSerializer);
                size += serializedNullableSize(read.importMetadata, version, ImportMetadata.serializer);
            }
            else if (read.items.length > 0)
            {
                size += serializedArraySize(read.items, tablesAndKeys, version, TxnNamedRead.serializer);
                size += serializedNullableSize(read.cassandraConsistencyLevel, consistencyLevelSerializer);
            }
            return size;
        }
    };
}
