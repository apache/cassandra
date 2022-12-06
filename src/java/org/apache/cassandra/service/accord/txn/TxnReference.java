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
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.CollectionType.Kind.SET;
import static org.apache.cassandra.service.accord.AccordSerializers.columnMetadataSerializer;

public class TxnReference
{
    private final TxnDataName tuple;
    private final ColumnMetadata column;
    private final CellPath path;

    public TxnReference(TxnDataName tuple, ColumnMetadata column, CellPath path)
    {
        this.tuple = tuple;
        this.column = column;
        this.path = path;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnReference reference = (TxnReference) o;
        return tuple.equals(reference.tuple) && Objects.equals(column, reference.column) && Objects.equals(path, reference.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tuple, column, path);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder().append(tuple);
        if (column != null)
            sb.append(':').append(column.ksName).append('.').append(column.cfName).append('.').append(column.name.toString());
        if (path != null)
            sb.append(path);
        return sb.toString();
    }

    public ColumnMetadata column()
    {
        return column;
    }
    
    public CellPath path()
    {
        return path;
    }
    
    public boolean selectsColumn()
    {
        return column != null;
    }

    public boolean selectsPath()
    {
        return selectsColumn() && path != null;
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
        return partition == null ? null : partition.partitionKey().getKey();
    }

    public ByteBuffer getClusteringKey(TxnData data)
    {
        Row row = getRow(data);
        if (row == null)
            return null;
        ByteBuffer[] clusteringKeys = row.clustering().getBufferArray();
        return clusteringKeys[column.position()];
    }

    public FilteredPartition getPartition(TxnData data)
    {
        return data.get(tuple);
    }
    
    public Row getRow(TxnData data)
    {
        FilteredPartition partition = getPartition(data);
        return partition != null ? getRow(partition) : null;
    }

    public Row getRow(FilteredPartition partition)
    {
        if (column != null && column.isStatic())
            return partition.staticRow();
        assert partition.rowCount() <= 1 : "Multi-row references are not allowed";
        if (partition.rowCount() == 0)
            return null;
        return partition.getAtIdx(0);
    }

    public ColumnData getColumnData(Row row)
    {
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
        CollectionType<?> collectionType = (CollectionType<?>) column.type;
        return collectionType.getSerializer().getSerializedValue(collection.buffer(), path.get(0), collectionType.nameComparator());
    }

    public ByteBuffer getFrozenFieldValue(Cell<?> udt)
    {
        UserType userType = (UserType) column.type;
        int field = ByteBufferUtil.getUnsignedShort(path.get(0), 0);
        return userType.split(ByteBufferAccessor.instance, udt.buffer())[field];
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
                return col.serializeForNativeProtocol(complex.iterator(), ProtocolVersion.CURRENT);
            }
            else if (type instanceof UserType)
            {
                UserType udt = (UserType) type;
                return udt.serializeForNativeProtocol(complex.iterator(), ProtocolVersion.CURRENT);
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

    static final IVersionedSerializer<TxnReference> serializer = new IVersionedSerializer<TxnReference>()
    {
        @Override
        public void serialize(TxnReference reference, DataOutputPlus out, int version) throws IOException
        {
            TxnDataName.serializer.serialize(reference.tuple, out, version);
            out.writeBoolean(reference.column != null);
            if (reference.column != null)
                columnMetadataSerializer.serialize(reference.column, out, version);
            out.writeBoolean(reference.path != null);
            if (reference.path != null)
                CollectionType.cellPathSerializer.serialize(reference.path, out);
        }

        @Override
        public TxnReference deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnDataName name = TxnDataName.serializer.deserialize(in, version);
            ColumnMetadata column = in.readBoolean() ? columnMetadataSerializer.deserialize(in, version) : null;
            CellPath path = in.readBoolean() ? CollectionType.cellPathSerializer.deserialize(in) : null;
            return new TxnReference(name, column, path);
        }

        @Override
        public long serializedSize(TxnReference reference, int version)
        {
            long size = 0;
            size += TxnDataName.serializer.serializedSize(reference.tuple, version);
            size += TypeSizes.BOOL_SIZE;
            if (reference.column != null)
                size += columnMetadataSerializer.serializedSize(reference.column, version);
            if (reference.path != null)
                size += CollectionType.cellPathSerializer.serializedSize(reference.path);
            return size;
        }
    };
}
