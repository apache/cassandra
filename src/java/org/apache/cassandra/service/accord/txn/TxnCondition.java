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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.ColumnCondition.Bound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.cassandra.service.accord.AccordSerializers.clusteringSerializer;
import static org.apache.cassandra.service.accord.AccordSerializers.deserializeCqlCollectionAsTerm;
import static org.apache.cassandra.service.accord.txn.TxnRead.SERIAL_READ;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializedListSize;

public abstract class TxnCondition
{
    private interface ConditionSerializer<T extends TxnCondition>
    {
        void serialize(T condition, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version, Kind kind) throws IOException;
        long serializedSize(T condition, int version);
    }

    public enum Kind
    {
        NONE("n/a", null),
        AND("AND", null),
        OR("OR", null),
        IS_NOT_NULL("IS NOT NULL", null),
        IS_NULL("IS NULL", null),
        EQUAL("=", Operator.EQ),
        NOT_EQUAL("!=", Operator.NEQ),
        GREATER_THAN(">", Operator.GT),
        GREATER_THAN_OR_EQUAL(">=", Operator.GTE),
        LESS_THAN("<", Operator.LT),
        LESS_THAN_OR_EQUAL("<=", Operator.LTE),
        COLUMN_CONDITIONS("COLUMN_CONDITIONS", null);

        @Nonnull
        private final String symbol;
        @Nullable
        private final Operator operator;

        Kind(String symbol, Operator operator)
        {
            this.symbol = symbol;
            this.operator = operator;
        }

        @SuppressWarnings("rawtypes")
        private ConditionSerializer serializer()
        {
            switch (this)
            {
                case IS_NOT_NULL:
                case IS_NULL:
                    return Exists.serializer;
                case EQUAL:
                case NOT_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return Value.serializer;
                case AND:
                case OR:
                    return BooleanGroup.serializer;
                case NONE:
                    return None.serializer;
                case COLUMN_CONDITIONS:
                    return ColumnConditionsAdapter.serializer;
                default:
                    throw new IllegalArgumentException("No serializer exists for kind " + this);
            }
        }
    }

    protected final Kind kind;

    public TxnCondition(Kind kind)
    {
        this.kind = kind;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnCondition condition = (TxnCondition) o;
        return kind == condition.kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind);
    }

    public Kind kind()
    {
        return kind;
    }

    public abstract boolean applies(TxnData data);

    private static class None extends TxnCondition
    {
        private static final None instance = new None();

        private None()
        {
            super(Kind.NONE);
        }

        @Override
        public String toString()
        {
            return kind.toString();
        }

        @Override
        public boolean applies(TxnData data)
        {
            return true;
        }

        private static final ConditionSerializer<None> serializer = new ConditionSerializer<None>()
        {
            @Override
            public void serialize(None condition, DataOutputPlus out, int version) {}
            @Override
            public None deserialize(DataInputPlus in, int version, Kind kind) { return instance; }
            @Override
            public long serializedSize(None condition, int version) { return 0; }
        };
    }

    public static TxnCondition none()
    {
        return None.instance;
    }

    public static class Exists extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.IS_NOT_NULL, Kind.IS_NULL);

        public final TxnReference reference;

        public Exists(TxnReference reference, Kind kind)
        {
            super(kind);
            Preconditions.checkArgument(KINDS.contains(kind), "Kind " + kind + " cannot be used with an existence condition");
            this.reference = reference;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Exists exists = (Exists) o;
            return reference.equals(exists.reference);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), reference);
        }

        @Override
        public String toString()
        {
            return reference.toString() + ' ' + kind.toString();
        }

        @Override
        public boolean applies(TxnData data)
        {
            FilteredPartition partition = reference.getPartition(data);
            boolean exists = partition != null && !partition.isEmpty();

            Row row = null;
            if (exists)
            {
                row = reference.getRow(partition);
                exists = row != null && !row.isEmpty();
            }

            if (exists && reference.selectsColumn())
            {
                ColumnData columnData = reference.getColumnData(row);

                if (columnData == null)
                {
                    exists = false;
                }
                else if (columnData.column().isComplex())
                {
                    if (reference.isElementSelection() || reference.isFieldSelection())
                    {
                        Cell<?> cell = (Cell<?>) columnData;
                        exists = !cell.isTombstone();
                    }
                    else
                    {
                        // TODO: Is this even necessary, given the partition is already filtered?
                        if (!((ComplexColumnData) columnData).complexDeletion().isLive())
                            exists = false;
                    }
                }
                else if (reference.isElementSelection())
                {
                    // This is frozen, so check if the Cell is a tombstone and that the element is present.
                    Cell<?> cell = (Cell<?>) columnData;
                    ByteBuffer element = reference.getFrozenCollectionElement(cell);
                    exists = element != null && !cell.isTombstone();
                }
                else if (reference.isFieldSelection())
                {
                    // This is frozen, so check if the Cell is a tombstone and that the field is present.
                    Cell<?> cell = (Cell<?>) columnData;
                    ByteBuffer fieldValue = reference.getFrozenFieldValue(cell);
                    exists = fieldValue != null && !cell.isTombstone();
                }
                else
                {
                    Cell<?> cell = (Cell<?>) columnData;
                    exists = !cell.isTombstone();
                }
            }

            switch (kind())
            {
                case IS_NOT_NULL:
                    return exists;
                case IS_NULL:
                    return !exists;
                default:
                    throw new IllegalStateException();
            }
        }

        private static final ConditionSerializer<Exists> serializer = new ConditionSerializer<Exists>()
        {
            @Override
            public void serialize(Exists condition, DataOutputPlus out, int version) throws IOException
            {
                TxnReference.serializer.serialize(condition.reference, out, version);
            }

            @Override
            public Exists deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new Exists(TxnReference.serializer.deserialize(in, version), kind);
            }

            @Override
            public long serializedSize(Exists condition, int version)
            {
                return TxnReference.serializer.serializedSize(condition.reference, version);
            }
        };
    }

    public static class ColumnConditionsAdapter extends TxnCondition {
        @Nonnull
        public final Collection<Bound> bounds;

        @Nonnull
        public final Clustering<?> clustering;

        public ColumnConditionsAdapter(Clustering<?> clustering, Collection<Bound> bounds)
        {
            super(Kind.COLUMN_CONDITIONS);
            checkNotNull(bounds);
            checkNotNull(clustering);
            this.bounds = bounds;
            this.clustering = clustering;
        }

        @Override
        public boolean applies(@Nonnull TxnData data)
        {
            checkNotNull(data);
            FilteredPartition partition = data.get(SERIAL_READ);
            Row row = partition.getRow(clustering);
            for (Bound bound : bounds)
            {
                if (!bound.appliesTo(row))
                    return false;
            }
            return true;
        }

        private static final ConditionSerializer<ColumnConditionsAdapter> serializer = new ConditionSerializer<ColumnConditionsAdapter>()
        {
            @Override
            public void serialize(ColumnConditionsAdapter condition, DataOutputPlus out, int version) throws IOException
            {
                clusteringSerializer.serialize(condition.clustering, out, version);
                serializeCollection(condition.bounds, out, version, Bound.serializer);
            }

            @Override
            public ColumnConditionsAdapter deserialize(DataInputPlus in, int version, Kind ignored) throws IOException
            {
                Clustering<?> clustering = clusteringSerializer.deserialize(in, version);
                List<Bound> bounds = deserializeList(in, version, Bound.serializer);
                return new ColumnConditionsAdapter(clustering, bounds);
            }

            @Override
            public long serializedSize(ColumnConditionsAdapter condition, int version)
            {
                return clusteringSerializer.serializedSize(condition.clustering, version)
                    + serializedCollectionSize(condition.bounds, version, Bound.serializer);
            }
        };
    }

    public static class Value extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.EQUAL, Kind.NOT_EQUAL,
                                                               Kind.GREATER_THAN, Kind.GREATER_THAN_OR_EQUAL,
                                                               Kind.LESS_THAN, Kind.LESS_THAN_OR_EQUAL);

        private final TxnReference reference;
        private final ByteBuffer value;
        private final ProtocolVersion version;

        public Value(TxnReference reference, Kind kind, ByteBuffer value, ProtocolVersion version)
        {
            super(kind);
            Preconditions.checkArgument(KINDS.contains(kind), "Kind " + kind + " cannot be used with a value condition");
            Preconditions.checkArgument(reference.selectsColumn(), "Reference " + reference + " does not select a column");
            this.reference = reference;
            this.value = value;
            this.version = version;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Value value1 = (Value) o;
            return reference.equals(value1.reference) && value.equals(value1.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), reference, value);
        }

        @Override
        public String toString()
        {
            return reference.toString() + ' ' + kind.symbol + " 0x" + ByteBufferUtil.bytesToHex(value);
        }

        @Override
        public boolean applies(TxnData data)
        {
            ColumnMetadata column = reference.column();

            if (column.isPartitionKey())
                return ColumnCondition.Bound.compareWithOperator(kind.operator, column.type, value, reference.getPartitionKey(data));
            else if (column.isClusteringColumn())
                return ColumnCondition.Bound.compareWithOperator(kind.operator, column.type, value, reference.getClusteringKey(data));

            if (column.isComplex())
            {
                AbstractType<?> type = column.type;
                Row row = reference.getRow(data);
                if (row == null) return false;

                if (type.isCollection())
                {
                    if (reference.selectsPath())
                    {
                        return new ColumnCondition.ElementAccessBound(column, reference.path().get(0), kind.operator, Collections.singletonList(value)).appliesTo(row);
                    }
                    else
                    {
                        Term.Terminal term = deserializeCqlCollectionAsTerm(value, type);
                        return ColumnCondition.MultiCellCollectionBound.appliesTo(column, kind.operator, Collections.singletonList(term), row);
                    }
                }
                else if (type.isUDT())
                {
                    if (reference.isFieldSelection())
                    {
                        Cell<?> cell = (Cell<?>) reference.getColumnData(data);
                        if (cell == null) return false;
                        return ColumnCondition.Bound.compareWithOperator(kind.operator, reference.getFieldSelectionType(), value, cell.buffer());
                    }

                    return new ColumnCondition.MultiCellUdtBound(column, kind.operator, Collections.singletonList(value)).appliesTo(row);
                }

                throw new UnsupportedOperationException("Unsupported complex type: " + type);
            }

            Cell<?> cell = (Cell<?>) reference.getColumnData(data);
            if (cell == null) return false;

            if (reference.isElementSelection())
            {
                // Frozen...otherwise we would have fallen into the complex logic above.
                ByteBuffer element = reference.getFrozenCollectionElement(cell);
                AbstractType<?> comparator = ((CollectionType<?>) column.type).valueComparator();
                return ColumnCondition.Bound.compareWithOperator(kind.operator, comparator, value, element);
            }
            else if (reference.isFieldSelection())
            {
                ByteBuffer fieldValue = reference.getFrozenFieldValue(cell);
                return ColumnCondition.Bound.compareWithOperator(kind.operator, reference.getFieldSelectionType(), value, fieldValue);
            }

            return ColumnCondition.Bound.compareWithOperator(kind.operator, column.type, value, cell.buffer());
        }

        private static final ConditionSerializer<Value> serializer = new ConditionSerializer<Value>()
        {
            @Override
            public void serialize(Value condition, DataOutputPlus out, int version) throws IOException
            {
                TxnReference.serializer.serialize(condition.reference, out, version);
                ByteBufferUtil.writeWithVIntLength(condition.value, out);
                out.writeUTF(condition.version.name());
            }

            @Override
            public Value deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                TxnReference reference = TxnReference.serializer.deserialize(in, version);
                ByteBuffer value = ByteBufferUtil.readWithVIntLength(in);
                ProtocolVersion protocolVersion = ProtocolVersion.valueOf(in.readUTF());
                return new Value(reference, kind, value, protocolVersion);
            }

            @Override
            public long serializedSize(Value condition, int version)
            {
                long size = 0;
                size += TxnReference.serializer.serializedSize(condition.reference, version);
                size += ByteBufferUtil.serializedSizeWithVIntLength(condition.value);
                size += TypeSizes.sizeof(condition.version.name());
                return size;
            }
        };
    }

    public static class BooleanGroup extends TxnCondition
    {
        private static final Set<Kind> KINDS = ImmutableSet.of(Kind.AND, Kind.OR);

        public final List<TxnCondition> conditions;

        public BooleanGroup(Kind kind, List<TxnCondition> conditions)
        {
            super(kind);
            Preconditions.checkArgument(KINDS.contains(kind), "Kind " + kind + " cannot be used at the root of a boolean condition");
            this.conditions = conditions;
        }

        @Override
        public String toString()
        {
            return '(' + conditions.stream().map(Objects::toString).reduce((a, b) -> a + ' ' + kind.symbol  + ' ' + b).orElse("") + ')';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            BooleanGroup that = (BooleanGroup) o;
            return Objects.equals(conditions, that.conditions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), conditions);
        }

        @Override
        public boolean applies(TxnData data)
        {
            switch (kind())
            {
                case AND:
                    return Iterables.all(conditions, c -> c.applies(data));
                case OR:
                    return Iterables.any(conditions, c -> c.applies(data));
                default:
                    throw new IllegalStateException();
            }
        }

        private static final ConditionSerializer<BooleanGroup> serializer = new ConditionSerializer<BooleanGroup>()
        {
            @Override
            public void serialize(BooleanGroup condition, DataOutputPlus out, int version) throws IOException
            {
                serializeList(condition.conditions, out, version, TxnCondition.serializer);
            }

            @Override
            public BooleanGroup deserialize(DataInputPlus in, int version, Kind kind) throws IOException
            {
                return new BooleanGroup(kind, deserializeList(in, version, TxnCondition.serializer));
            }

            @Override
            public long serializedSize(BooleanGroup condition, int version)
            {
                return serializedListSize(condition.conditions, version, TxnCondition.serializer);
            }
        };
    }

    public static final IVersionedSerializer<TxnCondition> serializer = new IVersionedSerializer<TxnCondition>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(TxnCondition condition, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(condition.kind.ordinal());
            condition.kind.serializer().serialize(condition, out, version);
        }

        @Override
        public TxnCondition deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedVInt32()];
            return kind.serializer().deserialize(in, version, kind);
        }

        @SuppressWarnings("unchecked")
        @Override
        public long serializedSize(TxnCondition condition, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(condition.kind.ordinal()) + condition.kind.serializer().serializedSize(condition, version);
        }
    };
}
