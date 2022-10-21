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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import accord.primitives.Ranges;
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
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static java.lang.Math.toIntExact;
import static org.apache.cassandra.utils.CollectionSerializer.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializer.serializedSizeMap;

public class AccordUpdate implements Update
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordUpdate(Keys.EMPTY, null, null));

    // TODO (soon): extends AbstractKeyIndexed; pack updates+predicates into one ByteBuffer
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
            final Kind[] kinds = Kind.values();
            @Override
            public void serialize(AbstractUpdate updater, DataOutputPlus out, int version) throws IOException
            {
                out.writeUnsignedVInt(updater.kind().ordinal());
                updater.serializer().serializeBody(updater, out, version);
            }

            @Override
            public AbstractUpdate deserialize(DataInputPlus in, int version) throws IOException
            {
                Kind kind = kinds[(int)in.readUnsignedVInt()];
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
                return TypeSizes.sizeofUnsignedVInt(updater.kind().ordinal())
                       + updater.serializer().serializedBodySize(updater, version);
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

    public AccordUpdate(List<AbstractUpdate> updates, List<UpdatePredicate> predicates)
    {
        updates.sort(Comparator.comparing(AbstractUpdate::partitionKey));
        predicates.sort(Comparator.comparing(UpdatePredicate::partitionKey));
        this.keys = Keys.of(updates, AbstractUpdate::partitionKey).union(Keys.of(predicates, UpdatePredicate::partitionKey));
        this.updates = toSerializedValuesArray(keys, updates, AbstractUpdate::partitionKey, AbstractUpdate.serializer);
        this.predicates = toSerializedValuesArray(keys, predicates, UpdatePredicate::partitionKey, predicateSerializer);
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
        AccordData read = data != null ? (AccordData) data : new AccordData(Collections.emptyList());
        for (int i = 0 ; i < predicates.length ; ++i)
        {
            if (predicates[i] == null)
                continue;

            List<UpdatePredicate> test = deserialize(predicates[i], predicateSerializer);
            for (int j = 0, mj = test.size() ; j < mj ; ++j)
            {
                if (!test.get(j).applies(read.get((PartitionKey) keys.get(i))))
                    return AccordWrite.EMPTY;
            }
        }
        NavigableMap<PartitionKey, PartitionUpdate> updateMap = new TreeMap<>();
        for (AbstractUpdate updater : deserialize(updates, AbstractUpdate.serializer))
        {
            PartitionUpdate update = updater.apply(read.get(updater.partitionKey()));
            PartitionKey key = PartitionKey.of(update);
            if (updateMap.containsKey(key))
                update = PartitionUpdate.merge(Lists.newArrayList(updateMap.get(key), update));
            updateMap.put(key, update);
        }
        return new AccordWrite(new ArrayList<>(updateMap.values()));
    }

    @Override
    public Update slice(Ranges ranges)
    {
        Keys keys = this.keys.slice(ranges);
        return new AccordUpdate(keys, select(this.keys, keys, updates), select(this.keys, keys, predicates));
    }

    private static ByteBuffer[] select(Keys in, Keys out, ByteBuffer[] from)
    {
        ByteBuffer[] result = new ByteBuffer[out.size()];
        int j = 0;
        for (int i = 0 ; i < in.size() ; ++i)
        {
            j = in.findNext(out.get(i), j);
            result[i] = from[j];
        }
        return result;
    }

    @Override
    public Update merge(Update update)
    {
        // TODO: special method for linear merging keyed and non-keyed lists simultaneously
        AccordUpdate that = (AccordUpdate) update;
        Keys keys = this.keys.union(that.keys);
        ByteBuffer[] updates = merge(this.keys, that.keys, this.updates, that.updates, keys.size());
        ByteBuffer[] predicates = merge(this.keys, that.keys, this.predicates, that.predicates, keys.size());
        return new AccordUpdate(keys, updates, predicates);
    }

    private static ByteBuffer[] merge(Keys leftKeys, Keys rightKeys, ByteBuffer[] left, ByteBuffer[] right, int outputSize)
    {
        ByteBuffer[] out = new ByteBuffer[outputSize];
        int l = 0, r = 0, o = 0;
        while (l < leftKeys.size() && r < rightKeys.size())
        {
            int c = leftKeys.get(l).compareTo(rightKeys.get(r));
            if (c < 0) { out[o++] = left[l++]; }
            else if (c > 0) { out[o++] = right[r++]; }
            else if (ByteBufferUtil.compareUnsigned(left[l], right[r]) != 0) { throw new IllegalStateException("The same keys have different values in each input"); }
            else { out[o++] = left[l++]; r++; }
        }
        while (l < leftKeys.size()) { out[o++] = left[l]; }
        while (r < rightKeys.size()) { out[o++] = right[r++]; }
        return out;
    }

    List<UpdatePredicate> getPredicate(int i)
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
            for (ByteBuffer buffer : update.updates)
                ByteBufferUtil.writeWithVIntLength(buffer, out);
            for (ByteBuffer buffer : update.predicates)
                ByteBufferUtil.writeWithVIntLength(buffer, out);
        }

        @Override
        public AccordUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);

            ByteBuffer[] updates = new ByteBuffer[keys.size()];
            for (int i=0; i<keys.size(); i++)
                updates[i] = ByteBufferUtil.readWithVIntLength(in);

            ByteBuffer[] predicates = new ByteBuffer[keys.size()];
            for (int i=0; i<keys.size(); i++)
                predicates[i] = ByteBufferUtil.readWithVIntLength(in);

            return new AccordUpdate(keys, updates, predicates);
        }

        @Override
        public long serializedSize(AccordUpdate update, int version)
        {
            long size = KeySerializers.keys.serializedSize(update.keys, version);
            for (ByteBuffer buffer : update.updates)
                size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);
            for (ByteBuffer buffer : update.predicates)
                size += ByteBufferUtil.serializedSizeWithVIntLength(buffer);
            return size;
        }
    };

    private static <T> ByteBuffer toSerializedValues(List<T> items, int start, int end, IVersionedSerializer<T> serializer, int version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(version) + TypeSizes.sizeofUnsignedVInt(end - start);
        for (int i = start ; i < end ; ++i)
            size += serializer.serializedSize(items.get(i), version);

        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeUnsignedVInt(version);
            out.writeUnsignedVInt(end - start);
            for (int i = start ; i < end ; ++i)
                serializer.serialize(items.get(i), out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> ByteBuffer[] toSerializedValuesArray(Keys keys, List<T> items, Function<? super T, ? extends Key> toKey, IVersionedSerializer<T> serializer)
    {
        ByteBuffer[] result = new ByteBuffer[keys.size()];
        int i = 0, mi = items.size(), ki = 0;
        while (i < mi)
        {
            Key key = toKey.apply(items.get(i));
            int j = i + 1;
            while (j < mi && toKey.apply(items.get(j)).equals(key))
                ++j;

            int nextki = keys.findNext(key, ki);
            Arrays.fill(result, ki, nextki, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ki = nextki;
            result[ki++] = toSerializedValues(items, i, j, serializer, MessagingService.current_version);
            i = j;
        }
        Arrays.fill(result, ki, result.length, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return result;
    }

    private static <T> List<T> deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer)
    {
        if (!bytes.hasRemaining())
            return Collections.emptyList();

        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            int version = toIntExact(in.readUnsignedVInt());
            int count = toIntExact(in.readUnsignedVInt());
            switch (count)
            {
                case 0: throw new IllegalStateException();
                case 1: return Collections.singletonList(serializer.deserialize(in, version));
                default:
                    List<T> result = new ArrayList<>();
                    for (int i = 0 ; i < count ; ++i)
                        result.add(serializer.deserialize(in, version));
                    return result;
            }
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
            result.addAll(deserialize(bytes, serializer));
        return result;
    }
}
