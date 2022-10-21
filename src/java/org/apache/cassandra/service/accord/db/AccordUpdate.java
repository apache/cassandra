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

package org.apache.cassandra.service.accord.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.utils.CollectionSerializer.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializedSizeMap;

public class AccordUpdate implements Update
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordUpdate(Keys.EMPTY, (ByteBuffer[]) null, (ByteBuffer[]) null));;

    private final Keys keys;
    private final ByteBuffer[] updates;
    private final ByteBuffer[] predicates;

    public static abstract class UpdatePredicate
    {
        public enum Type
        {
            EXISTS("EXISTS"),
            NOT_EXISTS("NOT EXISTS"),
            EQUAL("=="),
            NOT_EQUAL("!="),
            GREATER_THAN(">"),
            GREATER_THAN_OR_EQUAL(">="),
            LESS_THAN("<"),
            LESS_THAN_OR_EQUAL("<=");

            final String symbol;

            Type(String symbol)
            {
                this.symbol = symbol;
            }
        }

        final Type type;
        final TableMetadata table;
        final DecoratedKey key;
        final Clustering<?> clustering;
        final PartitionKey accordKey;

        public UpdatePredicate(Type type, TableMetadata table, DecoratedKey key, Clustering<?> clustering)
        {
            this.type = type;
            this.table = table;
            this.key = key;
            this.clustering = clustering;
            this.accordKey = new PartitionKey(table.id, key);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UpdatePredicate predicate = (UpdatePredicate) o;
            return type == predicate.type && table.equals(predicate.table) && key.equals(predicate.key) && clustering.equals(predicate.clustering);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, table, key, clustering);
        }

        @Override
        public String toString()
        {
            return table.keyspace + '.' + table.name + " ["
                   + key + ", " + clustering.toString(table) + "] " + type.symbol;
        }

        public PartitionKey partitionKey()
        {
            return accordKey;
        }

        public Type type()
        {
            return type;
        }

        public boolean applies(FilteredPartition partition)
        {
            return applies(partition == null ? null : partition.getRow(clustering));
        }

        public boolean supportedByRead(SinglePartitionReadCommand read)
        {
            return read.metadata().id.equals(table.id)
                   && read.partitionKey().equals(key)
                   && read.clusteringIndexFilter().selects(clustering);
        }

        protected abstract boolean applies(Row row);

        protected abstract void serializeBody(DataOutputPlus out, int version) throws IOException;
        protected abstract long serializedBodySize(int version);
    }

    public static class ExistsPredicate extends UpdatePredicate
    {
        private static final Set<Type> TYPES = ImmutableSet.of(Type.EXISTS, Type.NOT_EXISTS);

        public ExistsPredicate(Type type, TableMetadata table, DecoratedKey key, Clustering<?> clustering)
        {
            super(type, table, key, clustering);
            Preconditions.checkArgument(TYPES.contains(type));
        }

        @Override
        protected boolean applies(Row row)
        {
            boolean exists = row != null && !row.isEmpty();
            switch (type())
            {
                case EXISTS:
                    return exists;
                case NOT_EXISTS:
                    return !exists;
                default:
                    throw new IllegalStateException();
            }
        }

        @Override
        protected void serializeBody(DataOutputPlus out, int version)
        {

        }

        @Override
        protected long serializedBodySize(int version)
        {
            return 0;
        }

        public static ExistsPredicate deserialize(UpdatePredicate.Type type, TableMetadata table, DecoratedKey key, Clustering<?> clustering, DataInputPlus in, int version)
        {
            Preconditions.checkArgument(TYPES.contains(type));
            return new ExistsPredicate(type, table, key, clustering);
        }
    }

    public static class ValuePredicate extends UpdatePredicate
    {
        private static final Set<Type> TYPES = ImmutableSet.of(Type.EQUAL,
                                                               Type.NOT_EQUAL,
                                                               Type.GREATER_THAN,
                                                               Type.GREATER_THAN_OR_EQUAL,
                                                               Type.LESS_THAN,
                                                               Type.LESS_THAN_OR_EQUAL);

        private final ColumnMetadata column;
        // TODO: add cell path
        private final ByteBuffer value;

        public ValuePredicate(Type type, TableMetadata table, DecoratedKey key, Clustering<?> clustering,
                              ColumnMetadata column, ByteBuffer value)
        {
            super(type, table, key, clustering);
            Preconditions.checkArgument(TYPES.contains(type));
            this.column = column;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            ValuePredicate that = (ValuePredicate) o;
            return column.equals(that.column) && value.equals(that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), column, value);
        }

        @Override
        public String toString()
        {
            return table.keyspace + '.' + table.name + " ["
                   + key + ", " + clustering.toString(table) + ", " + column.name.toString() + "] "
                   + type.symbol + ' ' + column.type.getString(value);
        }

        private <T> int compare(Cell<T> cell)
        {
            return column.type.compare(cell.value(), cell.accessor(), value, ByteBufferAccessor.instance);
        }

        @Override
        public boolean supportedByRead(SinglePartitionReadCommand read)
        {
            return super.supportedByRead(read) && read.columnFilter().fetches(column);
        }

        @Override
        protected boolean applies(Row row)
        {
            if (row == null)
                return false;
            Cell<?> cell = row.getCell(column);
            int cmp = compare(cell);
            switch (type())
            {
                case EQUAL:
                    return cmp == 0;
                case NOT_EQUAL:
                    return cmp != 0;
                case GREATER_THAN:
                    return cmp > 0;
                case GREATER_THAN_OR_EQUAL:
                    return cmp >= 0;
                case LESS_THAN:
                    return cmp < 0;
                case LESS_THAN_OR_EQUAL:
                    return cmp <= 0;
                default:
                    throw new IllegalStateException();
            }
        }

        @Override
        protected void serializeBody(DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithVIntLength(column.name.bytes, out);
            ByteBufferUtil.writeWithVIntLength(value, out);
        }

        @Override
        protected long serializedBodySize(int version)
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(column.name.bytes)
                 + ByteBufferUtil.serializedSizeWithVIntLength(value);
        }

        public static ValuePredicate deserialize(UpdatePredicate.Type type, TableMetadata table, DecoratedKey key, Clustering<?> clustering, DataInputPlus in, int version) throws IOException
        {
            Preconditions.checkArgument(TYPES.contains(type));
            ColumnMetadata column = table.getColumn(ByteBufferUtil.readWithVIntLength(in));
            ByteBuffer value = ByteBufferUtil.readWithVIntLength(in);
            return new ValuePredicate(type, table, key, clustering, column, value);
        }
    }

    public abstract static class AbstractUpdate
    {
        interface Serializer<T extends AbstractUpdate>
        {
            void serializeBody(T update, DataOutputPlus out, int version) throws IOException;
            T deserializeBody(Kind kind, DataInputPlus in, int version) throws IOException;
            long serializedBodySize(T update, int version);
        }
        enum Kind { SIMPLE, APPEND, INCREMENT }
        public abstract Kind kind();
        public abstract PartitionKey partitionKey();
        public abstract PartitionUpdate apply(FilteredPartition partition);
        public abstract Serializer serializer();

        public static final IVersionedSerializer<AbstractUpdate> serializer = new IVersionedSerializer<AbstractUpdate>()
        {
            @Override
            public void serialize(AbstractUpdate updater, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(updater.kind().ordinal());
                updater.serializer().serializeBody(updater, out, version);
            }

            @Override
            public AbstractUpdate deserialize(DataInputPlus in, int version) throws IOException
            {
                Kind kind = Kind.values()[in.readInt()];
                switch (kind)
                {
                    case SIMPLE:
                        return SimpleUpdate.serializer.deserializeBody(kind, in, version);
                    case APPEND:
                        return AppendingUpdate.serializer.deserializeBody(kind, in, version);
                    case INCREMENT:
                        return IncrementingUpdate.serializer.deserializeBody(kind, in, version);
                    default: throw new IllegalArgumentException();
                }
            }

            @Override
            public long serializedSize(AbstractUpdate updater, int version)
            {
                return TypeSizes.INT_SIZE + updater.serializer().serializedBodySize(updater, version);
            }
        };
    }

    public static class SimpleUpdate extends AbstractUpdate
    {
        private final PartitionUpdate update;
        private final PartitionKey accordKey;

        public SimpleUpdate(PartitionUpdate update)
        {
            this.update = update;
            this.accordKey = new PartitionKey(update.metadata().id, update.partitionKey());
        }

        @Override
        public Kind kind()
        {
            return Kind.SIMPLE;
        }

        @Override
        public PartitionKey partitionKey()
        {
            return accordKey;
        }

        @Override
        public PartitionUpdate apply(FilteredPartition partition)
        {
            return update;
        }

        @Override
        public Serializer serializer()
        {
            return serializer;
        }

        static final Serializer<SimpleUpdate> serializer = new Serializer<SimpleUpdate>()
        {
            @Override
            public void serializeBody(SimpleUpdate update, DataOutputPlus out, int version) throws IOException
            {
                PartitionUpdate.serializer.serialize(update.update, out, version);
            }

            @Override
            public SimpleUpdate deserializeBody(Kind kind, DataInputPlus in, int version) throws IOException
            {
                Preconditions.checkArgument(kind == Kind.SIMPLE);
                return new SimpleUpdate(PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE));
            }

            @Override
            public long serializedBodySize(SimpleUpdate update, int version)
            {
                return PartitionUpdate.serializer.serializedSize(update.update, version);
            }
        };
    }

    private static final IVersionedSerializer<String> strSerializer = new IVersionedSerializer<String>()
    {
        @Override
        public void serialize(String s, DataOutputPlus out, int version) throws IOException { out.writeUTF(s); }
        @Override
        public String deserialize(DataInputPlus in, int version) throws IOException { return in.readUTF(); }
        @Override
        public long serializedSize(String s, int version) { return TypeSizes.sizeof(s); }
    };

    private static final IVersionedSerializer<Integer> intSerializer = new IVersionedSerializer<Integer>()
    {
        @Override
        public void serialize(Integer i, DataOutputPlus out, int version) throws IOException { out.writeInt(i); }

        @Override
        public Integer deserialize(DataInputPlus in, int version) throws IOException { return in.readInt(); }

        @Override
        public long serializedSize(Integer t, int version) { return TypeSizes.INT_SIZE; }
    };

    public static class AppendingUpdate extends AbstractUpdate
    {
        private final PartitionKey key;
        private final Map<String, String> appends;

        public AppendingUpdate(PartitionKey key, Map<String, String> appends)
        {
            Preconditions.checkArgument(appends != null && !appends.isEmpty());
            TableMetadata metadata = Schema.instance.getTableMetadata(key.tableId());
            Preconditions.checkArgument(metadata != null);
            for (String column : appends.keySet())
            {
                ColumnMetadata cdef = metadata.getColumn(new ColumnIdentifier(column, true));
                Preconditions.checkArgument(cdef != null);
                Preconditions.checkArgument(cdef.type == UTF8Type.instance);
                Preconditions.checkArgument(appends.get(column) != null);
            }

            this.key = key;
            this.appends = appends;
        }

        @Override
        public Kind kind()
        {
            return Kind.APPEND;
        }

        @Override
        public PartitionKey partitionKey()
        {
            return key;
        }

        private static <T> String getString(Cell<T> cell)
        {
            return cell == null || cell.isTombstone() ? "" : UTF8Type.instance.getString(cell.value(), cell.accessor());
        }

        private static void apply(Row.Builder builder, Row current, ColumnMetadata cdef, String append)
        {
            Preconditions.checkArgument(append != null);
            String value = getString(current.getCell(cdef)) + append;
            builder.addCell(BufferCell.live(cdef, 0, UTF8Type.instance.fromString(value)));
        }

        private static Row apply(Row.Builder builder, Row current, Columns columns, Map<ColumnMetadata, String> appendMap)
        {
            if (columns.isEmpty())
                return current;

            builder.newRow(current.clustering());
            for (ColumnMetadata column : columns)
                apply(builder, current, column, appendMap.get(column));

            return builder.build();
        }

        @Override
        public PartitionUpdate apply(FilteredPartition partition)
        {
            Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
            TableMetadata metadata = Schema.instance.getTableMetadata(key.tableId());
            RegularAndStaticColumns.Builder cbuilder = RegularAndStaticColumns.builder();
            Map<ColumnMetadata, String> appendMap = new HashMap<>();
            for (Map.Entry<String, String> entry : appends.entrySet())
            {
                ColumnMetadata cdef = metadata.getColumn(new ColumnIdentifier(entry.getKey(), true));
                cbuilder.add(cdef);
                appendMap.put(cdef, entry.getValue());
            }

            RegularAndStaticColumns columns = cbuilder.build();
            Row staticRow = apply(rowBuilder, partition.staticRow(), columns.statics, appendMap);


            Preconditions.checkArgument(metadata != null);
            PartitionUpdate.Builder updateBuilder = new PartitionUpdate.Builder(metadata,
                                                                                partition.partitionKey(),
                                                                                columns,
                                                                                staticRow,
                                                                                partition.rowCount());
            for (Row row : partition)
                updateBuilder.add(apply(rowBuilder, row, columns.regulars, appendMap));

            return updateBuilder.build();
        }

        @Override
        public Serializer serializer()
        {
            return serializer;
        }

        static final Serializer<AppendingUpdate> serializer = new Serializer<AppendingUpdate>()
        {
            @Override
            public void serializeBody(AppendingUpdate update, DataOutputPlus out, int version) throws IOException
            {
                PartitionKey.serializer.serialize(update.key, out, version);
                serializeMap(strSerializer, strSerializer, update.appends, out, version);
            }

            @Override
            public AppendingUpdate deserializeBody(Kind kind, DataInputPlus in, int version) throws IOException
            {
                Preconditions.checkArgument(kind == Kind.APPEND);
                return new AppendingUpdate(PartitionKey.serializer.deserialize(in, version),
                                           deserializeMap(strSerializer, strSerializer, Maps::newHashMapWithExpectedSize, in, version));
            }

            @Override
            public long serializedBodySize(AppendingUpdate update, int version)
            {
                return PartitionKey.serializer.serializedSize(update.key) +
                       serializedSizeMap(strSerializer, strSerializer, update.appends, version);
            }
        };
    }

    public static class IncrementingUpdate extends AbstractUpdate
    {
        private final PartitionKey key;
        private final Map<String, Integer> increments;

        public IncrementingUpdate(PartitionKey key, Map<String, Integer> increments)
        {
            Preconditions.checkArgument(increments != null && !increments.isEmpty());
            TableMetadata metadata = Schema.instance.getTableMetadata(key.tableId());
            Preconditions.checkArgument(metadata != null);

            for (String column : increments.keySet())
            {
                ColumnMetadata cdef = metadata.getColumn(new ColumnIdentifier(column, true));
                Preconditions.checkArgument(cdef != null);
                Preconditions.checkArgument(cdef.type == Int32Type.instance);
                Integer val = increments.get(column);
                Preconditions.checkArgument(val != null && val != 0);
            }
            this.key = key;
            this.increments = increments;
        }

        @Override
        public Kind kind()
        {
            return Kind.INCREMENT;
        }

        @Override
        public PartitionKey partitionKey()
        {
            return key;
        }

        private static <T> int getInt(Cell<T> cell)
        {
            return cell == null || cell.isTombstone() ? 0 : Int32Type.instance.compose(cell.value(), cell.accessor());
        }

        private static void apply(Row.Builder builder, Row current, ColumnMetadata cdef, int increment)
        {
            int value = getInt(current.getCell(cdef)) + increment;
            builder.addCell(BufferCell.live(cdef, 0, Int32Type.instance.decompose(value)));
        }

        private static Row apply(Row.Builder builder, Row current, Columns columns, Map<ColumnMetadata, Integer> appendMap)
        {
            if (columns.isEmpty())
                return current;

            builder.newRow(current.clustering());
            for (ColumnMetadata column : columns)
                apply(builder, current, column, appendMap.get(column));

            return builder.build();
        }

        @Override
        public PartitionUpdate apply(FilteredPartition partition)
        {
            Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
            TableMetadata metadata = Schema.instance.getTableMetadata(key.tableId());
            RegularAndStaticColumns.Builder cbuilder = RegularAndStaticColumns.builder();
            Map<ColumnMetadata, Integer> incrementMap = new HashMap<>();
            for (Map.Entry<String, Integer> entry : increments.entrySet())
            {
                ColumnMetadata cdef = metadata.getColumn(new ColumnIdentifier(entry.getKey(), true));
                cbuilder.add(cdef);
                incrementMap.put(cdef, entry.getValue());
            }

            RegularAndStaticColumns columns = cbuilder.build();
            Row staticRow = apply(rowBuilder, partition.staticRow(), columns.statics, incrementMap);


            Preconditions.checkArgument(metadata != null);
            PartitionUpdate.Builder updateBuilder = new PartitionUpdate.Builder(metadata,
                                                                                partition.partitionKey(),
                                                                                columns,
                                                                                staticRow,
                                                                                partition.rowCount());
            for (Row row : partition)
                updateBuilder.add(apply(rowBuilder, row, columns.regulars, incrementMap));

            return updateBuilder.build();
        }

        @Override
        public Serializer serializer()
        {
            return serializer;
        }

        static final Serializer<IncrementingUpdate> serializer = new Serializer<IncrementingUpdate>()
        {
            @Override
            public void serializeBody(IncrementingUpdate update, DataOutputPlus out, int version) throws IOException
            {
                PartitionKey.serializer.serialize(update.key, out, version);
                serializeMap(strSerializer, intSerializer, update.increments, out, version);
            }

            @Override
            public IncrementingUpdate deserializeBody(Kind kind, DataInputPlus in, int version) throws IOException
            {
                Preconditions.checkArgument(kind == Kind.INCREMENT);
                return new IncrementingUpdate(PartitionKey.serializer.deserialize(in, version),
                                           deserializeMap(strSerializer, intSerializer, Maps::newHashMapWithExpectedSize, in, version));
            }

            @Override
            public long serializedBodySize(IncrementingUpdate update, int version)
            {
                return PartitionKey.serializer.serializedSize(update.key) +
                       serializedSizeMap(strSerializer, intSerializer, update.increments, version);
            }
        };
    }

    private static Keys keysFrom(List<AbstractUpdate> updates, List<UpdatePredicate> predicates)
    {
        Set<Key> keys = new HashSet<>();
        for (AbstractUpdate update : updates)
            keys.add(update.partitionKey());
        for (UpdatePredicate predicate : predicates)
            keys.add(new PartitionKey(predicate.table.id, predicate.key));

        return new Keys(keys);
    }

    public AccordUpdate(List<AbstractUpdate> updates, List<UpdatePredicate> predicates)
    {
        this.keys = keysFrom(updates, predicates);
        this.updates = serialize(updates, AbstractUpdate.serializer);
        this.predicates = serialize(predicates, predicateSerializer);
    }

    public AccordUpdate(Keys keys, ByteBuffer[] updates, ByteBuffer[] predicates)
    {
        this.keys = keys;
        this.updates = updates;
        this.predicates = predicates;
    }

    @Override
    public String toString()
    {
        return "AccordUpdate{" +
               "updates=" + Arrays.toString(updates) +
               ", predicates=" + Arrays.toString(predicates) +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordUpdate update = (AccordUpdate) o;
        return Arrays.equals(updates, update.updates) && Arrays.equals(predicates, update.predicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(updates, predicates);
    }

    @Override
    public Keys keys()
    {
        return keys;
    }

    @Override
    public Write apply(Data data)
    {
        AccordData read = data != null ? (AccordData) data : new AccordData();
        for (ByteBuffer bytes : predicates)
        {
            UpdatePredicate predicate = deserialize(bytes, predicateSerializer);
            if (!predicate.applies(read.get(predicate.partitionKey())))
                return AccordWrite.EMPTY;
        }
        NavigableMap<PartitionKey, PartitionUpdate> updateMap = new TreeMap<>();
        for (AbstractUpdate updater : deserialize(updates, AbstractUpdate.serializer))
        {
            PartitionUpdate update = updater.apply(read.get(updater.partitionKey()));
            PartitionKey key = AccordKey.of(update);
            if (updateMap.containsKey(key))
                update = PartitionUpdate.merge(Lists.newArrayList(updateMap.get(key), update));
            updateMap.put(key, update);
        }
        return new AccordWrite(new ArrayList<>(updateMap.values()));
    }

    UpdatePredicate getPredicate(int i)
    {
        return deserialize(predicates[i], predicateSerializer);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE + ObjectSizes.sizeOfReferenceArray(updates.length) + ObjectSizes.sizeOfReferenceArray(predicates.length);
        for (ByteBuffer buffer : updates)
            size += ByteBufferUtil.estimatedSizeOnHeap(buffer);
        for (ByteBuffer buffer : predicates)
            size += ByteBufferUtil.estimatedSizeOnHeap(buffer);
        return size;
    }

    static final IVersionedSerializer<UpdatePredicate> predicateSerializer = new IVersionedSerializer<UpdatePredicate>()
    {
        @Override
        public void serialize(UpdatePredicate predicate, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(predicate.type.ordinal());
            predicate.table.id.serialize(out);
            ByteBufferUtil.writeWithVIntLength(predicate.key.getKey(), out);

            Clustering.serializer.serialize(predicate.clustering, out, version, predicate.table.comparator.subtypes());

            predicate.serializeBody(out, version);
        }

        @Override
        public UpdatePredicate deserialize(DataInputPlus in, int version) throws IOException
        {
            UpdatePredicate.Type type = UpdatePredicate.Type.values()[in.readInt()];
            TableId tableId = TableId.deserialize(in);
            TableMetadata table = Schema.instance.getTableMetadata(tableId);
            DecoratedKey key = table.partitioner.decorateKey(ByteBufferUtil.readWithVIntLength(in));
            Clustering<?> clustering = Clustering.serializer.deserialize(in, version, table.comparator.subtypes());
            switch (type)
            {
                case EXISTS:
                case NOT_EXISTS:
                    return ExistsPredicate.deserialize(type, table, key, clustering, in, version);
                case EQUAL:
                case NOT_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    return ValuePredicate.deserialize(type, table, key, clustering, in, version);
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public long serializedSize(UpdatePredicate predicate, int version)
        {
            long size = TypeSizes.sizeof(predicate.type.ordinal());
            size += predicate.table.id.serializedSize();
            size += ByteBufferUtil.serializedSizeWithVIntLength(predicate.key.getKey());
            size += Clustering.serializer.serializedSize(predicate.clustering, version, predicate.table.comparator.subtypes());
            return size + predicate.serializedBodySize(version);
        }
    };

    public static final IVersionedSerializer<AccordUpdate> serializer = new IVersionedSerializer<AccordUpdate>()
    {
        @Override
        public void serialize(AccordUpdate update, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.keys.serialize(update.keys, out, version);
            out.writeInt(update.updates.length);
            for (ByteBuffer buffer : update.updates)
                ByteBufferUtil.writeWithVIntLength(buffer, out);

            out.writeInt(update.predicates.length);
            for (ByteBuffer buffer : update.predicates)
                ByteBufferUtil.writeWithVIntLength(buffer, out);
        }

        @Override
        public AccordUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);
            int numUpdate = in.readInt();
            ByteBuffer[] updates = new ByteBuffer[numUpdate];
            for (int i=0; i<numUpdate; i++)
                updates[i] = ByteBufferUtil.readWithVIntLength(in);

            int numPredicate = in.readInt();
            ByteBuffer[] predicates = new ByteBuffer[numPredicate];
            for (int i=0; i<numPredicate; i++)
                predicates[i] = ByteBufferUtil.readWithVIntLength(in);

            return new AccordUpdate(keys, updates, predicates);
        }

        @Override
        public long serializedSize(AccordUpdate update, int version)
        {
            long size = KeySerializers.keys.serializedSize(update.keys, version);

            size += TypeSizes.sizeof(update.updates.length);
            for (ByteBuffer buffer : update.updates)
                size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);

            size += TypeSizes.sizeof(update.predicates.length);
            for (ByteBuffer buffer : update.predicates)
                size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);

            return size;
        }
    };

    private static <T> ByteBuffer serialize(T item, IVersionedSerializer<T> serializer)
    {
        int version = MessagingService.current_version;
        long size = serializer.serializedSize(item, version) + TypeSizes.INT_SIZE;
        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeInt(version);
            serializer.serialize(item, out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> ByteBuffer[] serialize(List<T> items, IVersionedSerializer<T> serializer)
    {
        ByteBuffer[] result = new ByteBuffer[items.size()];
        for (int i=0,mi=items.size(); i<mi; i++)
            result[i] = serialize(items.get(i), serializer);
        return result;
    }

    private static <T> T deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer)
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            int version = in.readInt();
            return serializer.deserialize(in, version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> List<T> deserialize(ByteBuffer[] buffers, IVersionedSerializer<T> serializer)
    {
        List<T> result = new ArrayList<>(buffers.length);
        for (ByteBuffer bytes : buffers)
            result.add(deserialize(bytes, serializer));
        return result;
    }
}
