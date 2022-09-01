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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
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
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

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

    private static Keys keysFrom(List<PartitionUpdate> updates, List<UpdatePredicate> predicates)
    {
        Set<Key> keys = new HashSet<>();
        for (PartitionUpdate update : updates)
            keys.add(new PartitionKey(update.metadata().id, update.partitionKey()));
        for (UpdatePredicate predicate : predicates)
            keys.add(new PartitionKey(predicate.table.id, predicate.key));

        return new Keys(keys);
    }

    public AccordUpdate(List<PartitionUpdate> updates, List<UpdatePredicate> predicates)
    {
        this.keys = keysFrom(updates, predicates);
        this.updates = serialize(updates, updateSerializer);
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
               "updates=" + updates +
               ", predicates=" + predicates +
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
        AccordData read = (AccordData) data;
        for (ByteBuffer bytes : predicates)
        {
            UpdatePredicate predicate = deserialize(bytes, predicateSerializer);
            if (!predicate.applies(read.get(predicate.partitionKey())))
                return AccordWrite.EMPTY;
        }
        return new AccordWrite(deserialize(updates, updateSerializer));
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

    static final IVersionedSerializer<UpdatePredicate> predicateSerializer = new IVersionedSerializer<>()
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

    private static final IVersionedSerializer<PartitionUpdate> updateSerializer = new IVersionedSerializer<PartitionUpdate>()
    {
        @Override
        public void serialize(PartitionUpdate upd, DataOutputPlus out, int version) throws IOException
        {
            PartitionUpdate.serializer.serialize(upd, out, version);
        }

        @Override
        public PartitionUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            return PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE);
        }

        @Override
        public long serializedSize(PartitionUpdate upd, int version)
        {
            return PartitionUpdate.serializer.serializedSize(upd, version);
        }
    };

    public static final IVersionedSerializer<AccordUpdate> serializer = new IVersionedSerializer<>()
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
