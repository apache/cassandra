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
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import accord.utils.VIntCoding;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.AbstractCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;
import static org.apache.cassandra.service.accord.AccordSerializers.columnMetadataSerializer;

public abstract class TxnReference
{
    public final Kind kind;
    protected final int tuple;

    private TxnReference(Kind kind, int tuple)
    {
        this.kind = kind;
        this.tuple = tuple;
    }

    /**
     * Creates a reference to a "row".  This method isn't directly used by the main logic and instead
     * exists to aid in testing.
     */
    public static RowReference row(int tuple)
    {
        return new RowReference(tuple);
    }

    /**
     * Creates a reference to a "column".  This method isn't directly used by the main logic and instead
     * exists to aid in testing.
     */
    public static ColumnReference column(int tuple, TableMetadata table, ColumnMetadata column)
    {
        return column(tuple, table, column, null);
    }

    /**
     * Creates a reference to a "column".  This method isn't directly used by the main logic and instead
     * exists to aid in testing.
     */
    public static ColumnReference column(int tuple, TableMetadata table, ColumnMetadata column, CellPath path)
    {
        Invariants.nonNull(table, "table is null");
        Invariants.nonNull(column, "column is null");
        return new ColumnReference(tuple, table, column, path);
    }

    public static TxnReference columnOrRow(int tuple,
                                           @Nullable TableMetadata table,
                                           @Nullable ColumnMetadata column,
                                           @Nullable CellPath path)
    {
        if (column == null)
        {
            Invariants.require(table == null, "Column is null but table isn't; unknown reference type");
            Invariants.require(path == null, "Column is null but path isn't; unknown reference type");
            return row(tuple);
        }
        return column(tuple, table, column, path);
    }

    public abstract void collect(TableMetadatas.Collector collector);

    public RowReference asRow()
    {
        Invariants.require(kind == Kind.ROW, "Expected to be a row but was a %s", kind);
        return (RowReference) this;
    }

    public ColumnReference asColumn()
    {
        Invariants.require(kind == Kind.COLUMN, "Expected to be a column but was a %s", kind);
        return (ColumnReference) this;
    }

    public TxnDataKeyValue getPartition(TxnData data)
    {
        return (TxnDataKeyValue)data.get(tuple);
    }

    public Row getRow(TxnData data)
    {
        FilteredPartition partition = getPartition(data);
        return partition != null ? getRow(partition) : null;
    }

    public Row getRow(FilteredPartition partition)
    {
        if (kind == Kind.COLUMN && asColumn().column.isStatic())
            return partition.staticRow();
        assert partition.rowCount() <= 1 : "Multi-row references are not allowed";
        if (partition.rowCount() == 0)
            return null;
        return partition.getAtIdx(0);
    }

    public static final UnversionedSerializer<RowReference> rowSerializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(RowReference reference, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(reference.tuple);
        }

        @Override
        public RowReference deserialize(DataInputPlus in) throws IOException
        {
            int tuple = in.readUnsignedVInt32();
            return row(tuple);
        }

        @Override
        public long serializedSize(RowReference reference)
        {
            return VIntCoding.sizeOfUnsignedVInt(reference.tuple);
        }
    };

    public static final ParameterisedUnversionedSerializer<ColumnReference, TableMetadatas> columnSerializer = new ParameterisedUnversionedSerializer<ColumnReference, TableMetadatas>()
    {
        @Override
        public void serialize(ColumnReference reference, TableMetadatas tables, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(reference.tuple);
            tables.serialize(reference.table, out);
            columnMetadataSerializer.serialize(reference.column, reference.table, out);
            out.writeBoolean(reference.path != null);
            if (reference.path != null)
                CollectionType.cellPathSerializer.serialize(reference.path, out);
        }

        @Override
        public ColumnReference deserialize(TableMetadatas tables, DataInputPlus in) throws IOException
        {
            int tuple = in.readUnsignedVInt32();
            TableMetadata table = tables.deserialize(in);
            ColumnMetadata column = columnMetadataSerializer.deserialize(table, in);
            CellPath path = in.readBoolean() ? CollectionType.cellPathSerializer.deserialize(in) : null;
            return TxnReference.column(tuple, table, column, path);
        }

        @Override
        public long serializedSize(ColumnReference reference, TableMetadatas tables)
        {
            long size = 0;
            size += VIntCoding.sizeOfUnsignedVInt(reference.tuple);
            size += tables.serializedSize(reference.table);
            size += columnMetadataSerializer.serializedSize(reference.column, reference.table);
            size += TypeSizes.BOOL_SIZE;
            if (reference.path != null)
                size += CollectionType.cellPathSerializer.serializedSize(reference.path);
            return size;
        }
    };


    static final ParameterisedUnversionedSerializer<TxnReference, TableMetadatas> serializer = new ParameterisedUnversionedSerializer<>()
    {
        @Override
        public void serialize(TxnReference reference, TableMetadatas tables, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(TinyEnumSet.encode(reference.kind));
            switch (reference.kind)
            {
                case ROW:
                    rowSerializer.serialize(reference.asRow(), out);
                    break;
                case COLUMN:
                    columnSerializer.serialize(reference.asColumn(), tables, out);
                    break;
                default:
                    throw new UnhandledEnum(reference.kind);
            }
        }

        @Override
        public TxnReference deserialize(TableMetadatas tables, DataInputPlus in) throws IOException
        {
            TinyEnumSet<TxnReference.Kind> kind = new TinyEnumSet<>(in.readUnsignedVInt32());
            if (kind.contains(Kind.ROW)) return rowSerializer.deserialize(in);
            if (kind.contains(Kind.COLUMN)) return columnSerializer.deserialize(tables, in);
            throw Invariants.illegalArgument("Unexpected kind: " + kind);
        }

        @Override
        public long serializedSize(TxnReference reference, TableMetadatas tables)
        {
            long size = VIntCoding.sizeOfUnsignedVInt(TinyEnumSet.encode(reference.kind));
            switch (reference.kind)
            {
                case ROW:
                    size += rowSerializer.serializedSize(reference.asRow());
                    break;
                case COLUMN:
                    size += columnSerializer.serializedSize(reference.asColumn(), tables);
                    break;
                default:
                    throw new UnhandledEnum(reference.kind);
            }
            return size;
        }
    };

    public enum Kind { ROW, COLUMN }

    public static class RowReference extends TxnReference
    {
        public RowReference(int tuple)
        {
            super(Kind.ROW, tuple);
        }

        @Override
        public void collect(TableMetadatas.Collector collector)
        {
            // no-op
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            RowReference that = (RowReference) o;
            return tuple == that.tuple;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tuple);
        }

        @Override
        public String toString()
        {
            return Integer.toString(tuple);
        }
    }

    public static class ColumnReference extends TxnReference
    {
        private final TableMetadata table;
        private final ColumnMetadata column;
        @Nullable
        private final CellPath path;

        public ColumnReference(int tuple, TableMetadata table, ColumnMetadata column, @Nullable CellPath path)
        {
            super(Kind.COLUMN, tuple);
            this.table = table;
            this.column = column;
            this.path = path;
        }

        public ColumnMetadata column()
        {
            return column;
        }

        public TableMetadata table()
        {
            return table;
        }

        @Nullable
        public CellPath path()
        {
            return path;
        }

        public boolean selectsPath()
        {
            return path != null;
        }

        public boolean isElementSelection()
        {
            return selectsPath() && column.type.isCollection();
        }

        public boolean isFieldSelection()
        {
            return selectsPath() && column.type.isUDT();
        }

        public ByteBuffer getPartitionKey(TxnData data)
        {
            FilteredPartition partition = getPartition(data);
            if (partition == null) return null;
            return partition.metadata().partitionKeyColumns().size() == 1
                   ? partition.partitionKey().getKey()
                   : ((CompositeType) partition.metadata().partitionKeyType).split(partition.partitionKey().getKey())[column.position()];
        }

        @Override
        public void collect(TableMetadatas.Collector collector)
        {
            collector.add(table);
        }

        public ByteBuffer getClusteringKey(TxnData data)
        {
            Row row = getRow(data);
            if (row == null)
                return null;
            return row.clustering().bufferAt(column.position());
        }

        public ColumnData getColumnData(Row row)
        {
            if (column.isClusteringColumn())
                return new ClusteringColumnData(column, row.clustering().bufferAt(column.position()));
            if (column.isComplex() && path == null)
                return row.getComplexColumnData(column);

            if (path != null && column.type.isMultiCell())
            {
                if (column.type.isCollection())
                {
                    CollectionType<?> collectionType = (CollectionType<?>) column.type;

                    if (collectionType.kind == CollectionType.Kind.LIST)
                        return row.getComplexColumnData(column).getCellByIndex(ByteBufferUtil.toInt(path.get(0)));
                }

                return row.getCell(column, path);
            }

            return row.getCell(column);
        }

        public ColumnData getColumnData(TxnData data)
        {
            Row row = getRow(data);
            return row != null ? getColumnData(row) : null;
        }

        public ByteBuffer getFrozenCollectionElement(Cell<?> collection)
        {
            CollectionType<?> collectionType = (CollectionType<?>) column.type.unwrap();
            return collectionType.getSerializer().getSerializedValue(collection.buffer(), path.get(0), collectionType.nameComparator());
        }

        public AbstractType<?> getFrozenCollectionElementType()
        {
            CollectionType<?> type = (CollectionType<?>) column.type.unwrap();
            if (type instanceof ListType) return Int32Type.instance; // by index is the only thing supported right now; see getFrozenCollectionElement
            return type.nameComparator();
        }

        public ByteBuffer getFrozenFieldValue(Cell<?> udt)
        {
            UserType userType = (UserType) column.type.unwrap();
            int field = ByteBufferUtil.getUnsignedShort(path.get(0), 0);
            List<ByteBuffer> tuple = userType.unpack(udt.buffer());
            return tuple.size() > field ? tuple.get(field) : null;
        }

        public AbstractType<?> getFieldSelectionType()
        {
            assert isFieldSelection() : "No field selection type exists";
            UserType userType = (UserType) column.type;
            int field = ByteBufferUtil.getUnsignedShort(path.get(0), 0);
            return userType.fieldType(field);
        }

        public ByteBuffer toByteBuffer(TxnData data, AbstractType<?> receiver)
        {
            // TODO: confirm all references can be satisfied as part of the txn condition
            AbstractType<?> type = column().type;

            // Modify the type we'll check if the reference is to a collection element.
            if (selectsPath())
            {
                if (type.isCollection())
                {
                    CollectionType<?> collectionType = (CollectionType<?>) type;
                    type = collectionType.kind == SET ? collectionType.nameComparator() : collectionType.valueComparator();
                }
                else if (type.isUDT())
                    type = getFieldSelectionType();
            }

            // Account for frozen collection and reversed clustering key references:
            AbstractType<?> receiveType = type.isFrozenCollection() ? receiver.freeze().unwrap() : receiver.unwrap();
            if (!(receiveType == type.unwrap()))
                throw new IllegalArgumentException("Receiving type " + receiveType + " does not match " + type.unwrap());

            if (column().isPartitionKey())
                return getPartitionKey(data);
            else if (column().isClusteringColumn())
                return getClusteringKey(data);

            ColumnData columnData = getColumnData(data);

            if (columnData == null)
                return null;

            if (selectsComplex())
            {
                ComplexColumnData complex = (ComplexColumnData) columnData;

                if (type instanceof CollectionType)
                {
                    CollectionType<?> col = (CollectionType<?>) type;
                    return col.serializeForNativeProtocol(complex.iterator());
                }
                else if (type instanceof UserType)
                {
                    UserType udt = (UserType) type;
                    return udt.serializeForNativeProtocol(complex.iterator());
                }

                throw new UnsupportedOperationException("Unsupported complex type: " + type);
            }
            else if (selectsFrozenCollectionElement())
            {
                // If a path is selected for a non-frozen collection, the element will already be materialized.
                return getFrozenCollectionElement((Cell<?>) columnData);
            }
            else if (selectsFrozenUDTField())
            {
                return getFrozenFieldValue((Cell<?>) columnData);
            }

            Cell<?> cell = (Cell<?>) columnData;
            return selectsSetElement() ? cell.path().get(0) : cell.buffer();
        }

        private boolean selectsComplex()
        {
            return column.isComplex() && path == null;
        }

        private boolean selectsSetElement()
        {
            return selectsPath() && column.type instanceof SetType;
        }

        private boolean selectsFrozenCollectionElement()
        {
            return selectsPath() && column.type.isFrozenCollection();
        }

        private boolean selectsFrozenUDTField()
        {
            return selectsPath() && column.type.isUDT() && !column.type.isMultiCell();
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            ColumnReference that = (ColumnReference) o;
            return tuple == that.tuple && Objects.equals(table, that.table) && Objects.equals(column, that.column) && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tuple, table, column, path);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder().append(tuple);
            sb.append(':').append(column.ksName).append('.').append(column.cfName).append('.').append(column.name.toString());
            if (path != null)
                sb.append('#').append(path);
            return sb.toString();
        }
    }

    private static class ClusteringColumnData extends AbstractCell<ByteBuffer>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new ClusteringColumnData(null, null));
        private final ByteBuffer value;

        private ClusteringColumnData(ColumnMetadata column, ByteBuffer value)
        {
            super(column);
            this.value = value;
        }

        @Override
        public ByteBuffer value()
        {
            return value;
        }

        @Override
        public ValueAccessor<ByteBuffer> accessor()
        {
            return ByteBufferAccessor.instance;
        }

        @Override
        public boolean isTombstone()
        {
            return false;
        }

        @Override
        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE;
        }

        @Override
        public long unsharedHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(value);
        }

        @Override
        public long timestamp()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int ttl()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CellPath path()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cell<?> withUpdatedColumn(ColumnMetadata newColumn)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cell<?> withUpdatedValue(ByteBuffer newValue)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cell<?> withUpdatedTimestamp(long newTimestamp)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cell<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, long newLocalDeletionTime)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cell<?> withSkippedValue()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int localDeletionTimeAsUnsignedInt()
        {
            throw new UnsupportedOperationException();
        }
    }
}
