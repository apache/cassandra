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
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import accord.api.Data;
import accord.api.Update;
import accord.api.Write;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AccordUpdate implements Update
{
    private final List<PartitionUpdate> updates;
    private final List<UpdatePredicate> predicates;

    public static abstract class UpdatePredicate implements AccordKey
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

        public UpdatePredicate(Type type, TableMetadata table, DecoratedKey key, Clustering<?> clustering)
        {
            this.type = type;
            this.table = table;
            this.key = key;
            this.clustering = clustering;
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

        @Override
        public TableId tableId()
        {
            return table.id;
        }

        @Override
        public PartitionPosition partitionKey()
        {
            return key;
        }

        public Type type()
        {
            return type;
        }

        public boolean applies(FilteredPartition partition)
        {
            return applies(partition.getRow(clustering));
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

    public AccordUpdate(List<PartitionUpdate> updates, List<UpdatePredicate> predicates)
    {
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
        return updates.equals(update.updates) && predicates.equals(update.predicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(updates, predicates);
    }

    @Override
    public Write apply(Data data)
    {
        AccordData read = (AccordData) data;
        for (UpdatePredicate predicate : predicates)
        {
            if (!predicate.applies(read.get(predicate)))
                return AccordWrite.EMPTY;
        }
        return new AccordWrite(updates);
    }

    PartitionUpdate getPartitionUpdate(int i)
    {
        return updates.get(i);
    }

    UpdatePredicate getPredicate(int i)
    {
        return predicates.get(i);
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

    public static final IVersionedSerializer<AccordUpdate> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordUpdate update, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(update.updates.size());
            for (PartitionUpdate upd : update.updates)
                PartitionUpdate.serializer.serialize(upd, out, version);

            out.writeInt(update.predicates.size());
            for (UpdatePredicate predicate : update.predicates)
                predicateSerializer.serialize(predicate, out, version);
        }

        @Override
        public AccordUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            int numUpdate = in.readInt();
            List<PartitionUpdate> updates = new ArrayList<>(numUpdate);
            for (int i=0; i<numUpdate; i++)
                updates.add(PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE));

            int numPredicate = in.readInt();
            List<UpdatePredicate> predicates = new ArrayList<>(numPredicate);
            for (int i=0; i<numPredicate; i++)
                predicates.add(predicateSerializer.deserialize(in, version));

            return new AccordUpdate(updates, predicates);
        }

        @Override
        public long serializedSize(AccordUpdate update, int version)
        {
            long size = TypeSizes.sizeof(update.updates.size());
            for (PartitionUpdate upd : update.updates)
                size += PartitionUpdate.serializer.serializedSize(upd, version);

            size += TypeSizes.sizeof(update.predicates.size());
            for (UpdatePredicate predicate : update.predicates)
                size += predicateSerializer.serializedSize(predicate, version);

            return size;
        }
    };
}
