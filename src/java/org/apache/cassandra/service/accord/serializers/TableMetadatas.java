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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;

import accord.utils.Invariants;
import accord.utils.SortedArrays;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;

import static accord.utils.SortedArrays.Search.FAST;

public abstract class TableMetadatas extends AbstractList<TableId>
{
    private static final Comparator<Object> comparingId = Comparator.comparing(v -> ((TableMetadata) v).id);
    public static class Collector extends AbstractSortedCollector<TableMetadata, Complete>
    {
        @Override
        Comparator<Object> comparator()
        {
            return comparingId;
        }

        @Override
        Complete empty()
        {
            return TableMetadatas.none();
        }

        @Override
        Complete of(TableMetadata one)
        {
            return TableMetadatas.of(one);
        }

        @Override
        Complete copy(Object[] array, int count)
        {
            TableMetadata[] result = new TableMetadata[count];
            System.arraycopy(array, 0, result, 0, count);
            return TableMetadatas.ofSortedUnique(result);
        }

        @Override
        Complete copyBtree(Object[] btree, int count)
        {
            TableMetadata[] result = new TableMetadata[count];
            int i = 0;
            for (TableMetadata v : BTree.<TableMetadata>iterable(btree))
                result[i++] = v;
            return TableMetadatas.ofSortedUnique(result);
        }
    }

    public abstract int indexOf(TableMetadata find);
    public abstract int indexOf(TableId find);
    public abstract TableId get(TableId tableId);

    public abstract void serialize(TableMetadata table, DataOutputPlus out) throws IOException;
    public abstract TableMetadata deserialize(DataInputPlus in) throws IOException;
    public abstract long serializedSize(TableMetadata table);

    public abstract void serializeSelf(DataOutputPlus out) throws IOException;
    public abstract long serializedSelfSize();

    public static Complete none()
    {
        return Multi.NONE;
    }

    public static Complete of(TableMetadata metadata)
    {
        return new One(metadata);
    }

    public static Complete ofSortedUnique(TableMetadata ... metadatas)
    {
        if (metadatas.length == 0)
            return none();
        if (metadatas.length == 1)
            return new One(metadatas[0]);
        Invariants.requireStrictlyOrdered(comparingId, metadatas);
        return new Multi(metadatas);
    }

    public static abstract class Complete extends TableMetadatas
    {
        public abstract TableMetadata getMetadata(TableId tableId);
    }

    static class One extends Complete
    {
        final TableMetadata table;

        One(TableMetadata table)
        {
            this.table = table;
        }

        @Override
        public TableId get(int index)
        {
            Invariants.require(index == 0);
            return table.id;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public int indexOf(TableMetadata find)
        {
            int c = find.id == table.id ? 0 : find.id.compareTo(table.id);
            if (c == 0) return 0;
            else if (c < 0) return -1;
            else return -2;
        }

        @Override
        public void serialize(TableMetadata table, DataOutputPlus out) throws IOException
        {
        }

        @Override
        public void serializeSelf(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(1);
            table.id.serializeCompactComparable(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in) throws IOException
        {
            return table;
        }

        @Override
        public long serializedSize(TableMetadata table)
        {
            return 0;
        }

        @Override
        public long serializedSelfSize()
        {
            return TypeSizes.sizeofUnsignedVInt(1) + table.id.serializedCompactComparableSize();
        }

        @Override
        public int indexOf(TableId tableId)
        {
            if (tableId.equals(table.id))
                return 0;
            return -1;
        }

        @Override
        public TableId get(TableId tableId)
        {
            if (tableId.equals(table.id))
                return table.id;
            return null;
        }

        @Override
        public TableMetadata getMetadata(TableId tableId)
        {
            if (tableId.equals(table.id))
                return table;
            return null;
        }
    }

    static class Multi extends Complete
    {
        static final Complete NONE = new Multi();

        final TableMetadata[] tables;

        Multi(TableMetadata ... tables)
        {
            this.tables = tables;
        }

        @Override
        public TableId get(int index)
        {
            return tables[index].id;
        }

        @Override
        public int size()
        {
            return tables.length;
        }

        @Override
        public int indexOf(TableMetadata find)
        {
            return Arrays.binarySearch(tables, find, comparingId);
        }

        @Override
        public int indexOf(TableId find)
        {
            return SortedArrays.binarySearch(tables, 0, tables.length, find, (id, metadata) -> id.compareTo(metadata.id), FAST);
        }

        @Override
        public void serialize(TableMetadata table, DataOutputPlus out) throws IOException
        {
            int i = indexOf(table);
            if (i < 0)
                throw new IllegalStateException("TableMetadata for " + table + " not found in " + this);
            out.writeUnsignedVInt32(i);
        }

        @Override
        public void serializeSelf(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(tables.length);
            for (TableMetadata table : tables)
                table.id.serializeCompactComparable(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in) throws IOException
        {
            return tables[in.readUnsignedVInt32()];
        }

        @Override
        public long serializedSize(TableMetadata table)
        {
            int i = indexOf(table);
            if (i < 0)
                throw new IllegalStateException("TableMetadata for " + table + " not found in " + this);
            return TypeSizes.sizeofUnsignedVInt(indexOf(table));
        }

        @Override
        public long serializedSelfSize()
        {
            long size = TypeSizes.sizeofUnsignedVInt(tables.length);
            for (TableMetadata table : tables)
                size += table.id.serializedCompactComparableSize();
            return size;
        }

        @Override
        public TableId get(TableId tableId)
        {
            int i = indexOf(tableId);
            return i >= 0 ? tables[i].id : null;
        }

        @Override
        public TableMetadata getMetadata(TableId tableId)
        {
            int i = indexOf(tableId);
            return i >= 0 ? tables[i] : null;
        }
    }

    static class WithUnknown extends TableMetadatas
    {
        final TableId[] ids;
        final TableMetadata[] metadatas;

        WithUnknown(TableId[] ids, TableMetadata[] metadatas)
        {
            this.ids = ids;
            this.metadatas = metadatas;
        }

        @Override
        public TableId get(int index)
        {
            return ids[index];
        }

        @Override
        public int size()
        {
            return ids.length;
        }

        @Override
        public int indexOf(TableMetadata find)
        {
            return indexOf(find.id);
        }

        @Override
        public int indexOf(TableId find)
        {
            return Arrays.binarySearch(ids, find);
        }

        @Override
        public void serialize(TableMetadata table, DataOutputPlus out) throws IOException
        {
            if (ids.length == 1)
                return;

            int i = indexOf(table);
            if (i < 0)
                throw new IllegalStateException("TableMetadata for " + table + " not found in " + this);
            out.writeUnsignedVInt32(i);
        }

        @Override
        public void serializeSelf(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(ids.length);
            for (TableId id : ids)
                id.serializeCompactComparable(out);
        }

        @Override
        public TableMetadata deserialize(DataInputPlus in) throws IOException
        {
            if (ids.length == 1)
                return metadatas[0];

            int index = in.readUnsignedVInt32();
            TableMetadata metadata = metadatas[index];
            if (metadata == null)
                throw new UnknownTableException("Unknown table", ids[index]);
            return metadata;
        }

        @Override
        public long serializedSize(TableMetadata table)
        {
            if (ids.length == 1)
                return 0;

            int i = indexOf(table);
            if (i < 0)
                throw new IllegalStateException("TableMetadata for " + table + " not found in " + this);
            return TypeSizes.sizeofUnsignedVInt(indexOf(table));
        }

        @Override
        public long serializedSelfSize()
        {
            long size = TypeSizes.sizeofUnsignedVInt(ids.length);
            for (TableId id : ids)
                size += id.serializedCompactComparableSize();
            return size;
        }

        @Override
        public TableId get(TableId tableId)
        {
            int index = indexOf(tableId);
            return get(index);
        }
    }

    public static TableMetadatas deserializeSelf(DataInputPlus in) throws IOException
    {
        int count = in.readUnsignedVInt32();
        if (count == 0)
            return none();

        if (count == 1)
        {
            TableId id = TableId.deserializeCompactComparable(in);
            TableMetadata metadata = Schema.instance.getTableMetadata(id);
            if (metadata == null)
                return new WithUnknown(new TableId[] { id}, new TableMetadata[] { null });
            return new One(metadata);
        }

        TableId[] ids = null;
        TableMetadata[] metadatas = new TableMetadata[count];
        int i;
        for (i = 0 ; i < count ; ++i)
        {
            TableId id = TableId.deserializeCompactComparable(in);
            TableMetadata metadata = Schema.instance.getTableMetadata(id);
            metadatas[i] = metadata;
            if (ids != null) ids[i] = id;
            else if (metadata == null)
            {
                ids = new TableId[count];
                for (int j = 0 ; j < i ; ++j)
                    ids[j] = metadatas[j].id;
            }
        }
        if (ids == null)
            return new Multi(metadatas);
        return new WithUnknown(ids, metadatas);
    }
}
