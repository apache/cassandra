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
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.ColumnCondition.Bound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.service.accord.AccordSerializers.clusteringSerializer;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.CAS_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializedListSize;

public abstract class TxnCondition
{
    public static class SerializedTxnCondition extends AbstractParameterisedUnversionedSerialized<TxnCondition, TableMetadatas>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SerializedTxnCondition(null));

        protected SerializedTxnCondition(@Nullable ByteBuffer latestVersionBytes)
        {
            super(latestVersionBytes);
        }

        @Override
        protected ParameterisedUnversionedSerializer<TxnCondition, TableMetadatas> serializer()
        {
            return serializer;
        }

        protected SerializedTxnCondition(TxnCondition condition, TableMetadatas param)
        {
            this(serializer.serializeUnchecked(condition, param));
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(unsafeBytes());
        }
    }

    private interface ConditionSerializer<T extends TxnCondition>
    {
        void serialize(T condition, TableMetadatas tables, DataOutputPlus out) throws IOException;
        T deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException;
        long serializedSize(T condition, TableMetadatas tables);
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

    public abstract void collect(TableMetadatas.Collector collector);

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
        public void collect(TableMetadatas.Collector collector)
        {
        }

        @Override
        public boolean applies(TxnData data)
        {
            return true;
        }

        private static final ConditionSerializer<None> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(None condition, TableMetadatas tables, DataOutputPlus out) {}
            @Override
            public None deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) { return instance; }
            @Override
            public long serializedSize(None condition, TableMetadatas tables) { return 0; }
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
        public void collect(TableMetadatas.Collector collector)
        {
            reference.collect(collector);
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

        private boolean applies(FilteredPartition partition, boolean exists, TxnReference.ColumnReference ref)
        {
            ColumnMetadata column = ref.column();
            if (column.isPartitionKey()) return exists;
            Row row = null;
            if (exists)
            {
                row = reference.getRow(partition);
                exists = row != null && !row.isEmpty();
            }

            if (exists)
            {
                ColumnData columnData = ref.getColumnData(row);

                if (columnData == null)
                {
                    exists = false;
                }
                else if (columnData.column().isComplex())
                {
                    if (ref.isElementSelection())
                    {
                        Cell<?> cell = (Cell<?>) columnData;
                        exists = !cell.isTombstone();
                        // Collections don't support NULL but meangingless null types are supported, so byte[0] is allowed!
                        // This is NULL when touched, so need to still check each value
                        if (exists)
                        {
                            CollectionType<?> type = (CollectionType<?>) column.type.unwrap();
                            switch (type.kind)
                            {
                                case MAP:
                                {
                                    exists = !type.nameComparator().isNull(cell.path().get(0));
                                    if (exists)
                                        exists = !type.valueComparator().isNull(cell.buffer());
                                }
                                break;
                                case SET:
                                {
                                    exists = !type.nameComparator().isNull(cell.path().get(0));
                                }
                                break;
                                case LIST:
                                {
                                    exists = !type.valueComparator().isNull(cell.buffer());
                                }
                                break;
                                default:
                                    throw new UnsupportedOperationException(type.kind.name());
                            }
                        }
                    }
                    else if (ref.isFieldSelection())
                    {
                        Cell<?> cell = (Cell<?>) columnData;
                        exists = exists(cell, ref.getFieldSelectionType());
                    }
                    else
                    {
                        // TODO: Is this even necessary, given the partition is already filtered?
                        if (!((ComplexColumnData) columnData).complexDeletion().isLive())
                            exists = false;
                    }
                }
                else if (ref.isElementSelection())
                {
                    // This is frozen, so check if the Cell is a tombstone and that the element is present.
                    Cell<?> cell = (Cell<?>) columnData;
                    exists = exists(cell, column.type);
                    if (exists)
                    {
                        ByteBuffer element = ref.getFrozenCollectionElement(cell);
                        exists = !ref.getFrozenCollectionElementType().isNull(element);
                    }
                }
                else if (ref.isFieldSelection())
                {
                    // This is frozen, so check if the Cell is a tombstone and that the field is present.
                    Cell<?> cell = (Cell<?>) columnData;
                    exists = exists(cell, column.type);
                    if (exists)
                    {
                        ByteBuffer fieldValue = ref.getFrozenFieldValue(cell);
                        exists = !ref.getFieldSelectionType().isNull(fieldValue);
                    }
                }
                else
                {
                    Cell<?> cell = (Cell<?>) columnData;
                    exists = exists(cell, column.type);
                }
            }
            return exists;
        }

        @Override
        public boolean applies(TxnData data)
        {
            FilteredPartition partition = reference.getPartition(data);
            boolean exists = partition != null && !partition.isEmpty();

            if (reference.kind == TxnReference.Kind.COLUMN)
                exists = applies(partition, exists, reference.asColumn());

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

        private static boolean exists(Cell<?> cell, AbstractType<?> type)
        {
            return !cell.isTombstone() && !type.unwrap().isNull(cell.buffer());
        }

        private static final ConditionSerializer<Exists> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(Exists condition, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                TxnReference.serializer.serialize(condition.reference, tables, out);
            }

            @Override
            public Exists deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException
            {
                return new Exists(TxnReference.serializer.deserialize(tables, in), kind);
            }

            @Override
            public long serializedSize(Exists condition, TableMetadatas tables)
            {
                return TxnReference.serializer.serializedSize(condition.reference, tables);
            }
        };
    }

    public static class ColumnConditionsAdapter extends TxnCondition
    {
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
        public void collect(TableMetadatas.Collector collector)
        {
            for (Bound bound : bounds)
            {
                TableMetadata table = bound.table;
                if (table != null)
                    collector.add(table);
            }
        }

        @Override
        public boolean applies(@Nonnull TxnData data)
        {
            checkNotNull(data);
            TxnDataKeyValue value = (TxnDataKeyValue)data.get(txnDataName(CAS_READ));
            Row row = value != null ? value.getRow(clustering) : null;
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
            public void serialize(ColumnConditionsAdapter condition, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                clusteringSerializer.serialize(condition.clustering, out);
                serializeCollection(condition.bounds, tables, out, Bound.serializer);
            }

            @Override
            public ColumnConditionsAdapter deserialize(TableMetadatas tables, DataInputPlus in, Kind ignored) throws IOException
            {
                Clustering<?> clustering = clusteringSerializer.deserialize(in);
                List<Bound> bounds = deserializeList(tables, in, Bound.serializer);
                return new ColumnConditionsAdapter(clustering, bounds);
            }

            @Override
            public long serializedSize(ColumnConditionsAdapter condition, TableMetadatas tables)
            {
                return clusteringSerializer.serializedSize(condition.clustering)
                    + serializedCollectionSize(condition.bounds, tables, Bound.serializer);
            }
        };
    }

    public static class Value extends TxnCondition
    {
        private static final EnumSet<Kind> KINDS = EnumSet.of(Kind.EQUAL, Kind.NOT_EQUAL,
                                                              Kind.GREATER_THAN, Kind.GREATER_THAN_OR_EQUAL,
                                                              Kind.LESS_THAN, Kind.LESS_THAN_OR_EQUAL);

        private final TxnReference.ColumnReference reference;
        private final ByteBuffer value;
        private final ProtocolVersion version;

        public Value(TxnReference.ColumnReference reference, Kind kind, ByteBuffer value, ProtocolVersion version)
        {
            super(kind);
            Invariants.requireArgument(KINDS.contains(kind), "Kind " + kind + " cannot be used with a value condition");
            this.reference = reference;
            this.value = value;
            this.version = version;
        }

        public static EnumSet<Kind> supported()
        {
            return EnumSet.copyOf(KINDS);
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
        public void collect(TableMetadatas.Collector collector)
        {
            reference.collect(collector);
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

        private Bound getBounds(TxnData data)
        {
            ColumnMetadata column = reference.column();
            TableMetadata table = reference.table();
            if (column.isPartitionKey())
            {
                ByteBuffer bb = reference.getPartitionKey(data);
                return new ColumnCondition.SimpleBound(column, table, kind.operator, value)
                {
                    @Override
                    protected ByteBuffer rowValue(Row row)
                    {
                        return bb;
                    }
                };
            }
            else if (column.isClusteringColumn())
                return new ColumnCondition.SimpleClusteringBound(column, table, kind.operator, value);
            AbstractType<?> type = column.type;
            if (type.isCollection())
            {
                if (reference.selectsPath())
                    return new ColumnCondition.ElementOrFieldAccessBound(column, table, reference.path().get(0), kind.operator, value);
                if (type.isMultiCell())
                    return new ColumnCondition.MultiCellBound(column, table, kind.operator, value);
            }
            else if (type.isUDT())
            {
                if (reference.isFieldSelection())
                {
                    UserType ut = (UserType) type;
                    return new ColumnCondition.ElementOrFieldAccessBound(column, table, ut.fieldName(reference.path()).bytes, kind.operator, value);
                }
                if (type.isMultiCell())
                    return new ColumnCondition.MultiCellBound(column, table, kind.operator, value);
            }
            return new ColumnCondition.SimpleBound(column, table, kind.operator, value);
        }

        @Override
        public boolean applies(TxnData data)
        {
            Bound bounds = getBounds(data);
            if (reference.column().type.unwrap().isNull(bounds.value))
                return false;
            Row row = reference.getRow(data);
            if (bounds.isNull(row))
                return false;
            return bounds.appliesTo(row);
        }

        private static final ConditionSerializer<Value> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(Value condition, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                TxnReference.serializer.serialize(condition.reference, tables, out);
                ByteBufferUtil.writeWithVIntLength(condition.value, out);
                out.writeUTF(condition.version.name());
            }

            @Override
            public Value deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException
            {
                TxnReference.ColumnReference reference = TxnReference.serializer.deserialize(tables, in).asColumn();
                ByteBuffer value = ByteBufferUtil.readWithVIntLength(in);
                ProtocolVersion protocolVersion = ProtocolVersion.valueOf(in.readUTF());
                return new Value(reference, kind, value, protocolVersion);
            }

            @Override
            public long serializedSize(Value condition, TableMetadatas tables)
            {
                long size = 0;
                size += TxnReference.serializer.serializedSize(condition.reference, tables);
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
        public void collect(TableMetadatas.Collector collector)
        {
            for (TxnCondition condition : conditions)
                condition.collect(collector);
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

        private static final ConditionSerializer<BooleanGroup> serializer = new ConditionSerializer<>()
        {
            @Override
            public void serialize(BooleanGroup condition, TableMetadatas tables, DataOutputPlus out) throws IOException
            {
                serializeList(condition.conditions, tables, out, TxnCondition.serializer);
            }

            @Override
            public BooleanGroup deserialize(TableMetadatas tables, DataInputPlus in, Kind kind) throws IOException
            {
                return new BooleanGroup(kind, deserializeList(tables, in, TxnCondition.serializer));
            }

            @Override
            public long serializedSize(BooleanGroup condition, TableMetadatas tables)
            {
                return serializedListSize(condition.conditions, tables, TxnCondition.serializer);
            }
        };
    }

    public static final ParameterisedUnversionedSerializer<TxnCondition, TableMetadatas> serializer = new ParameterisedUnversionedSerializer<>()
    {
        @SuppressWarnings("unchecked")
        @Override
        public void serialize(TxnCondition condition, TableMetadatas tables, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(condition.kind.ordinal());
            condition.kind.serializer().serialize(condition, tables, out);
        }

        @Override
        public TxnCondition deserialize(TableMetadatas tables, DataInputPlus in) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedVInt32()];
            return kind.serializer().deserialize(tables, in, kind);
        }

        @SuppressWarnings("unchecked")
        @Override
        public long serializedSize(TxnCondition condition, TableMetadatas tables)
        {
            long size = TypeSizes.sizeofUnsignedVInt(condition.kind.ordinal());
            size += condition.kind.serializer().serializedSize(condition, tables);
            return size;
        }
    };
}
