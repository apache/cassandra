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
import java.util.Comparator;

import accord.api.Key;
import accord.api.Sliceable;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.utils.Invariants;
import accord.utils.VIntCoding;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas.Multi;
import org.apache.cassandra.utils.btree.BTreeSet;

import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;

public class TableMetadatasAndKeys extends IVersionedWithKeysSerializer.AbstractWithKeysSerializer implements Sliceable<TableMetadatasAndKeys>
{
    public static class KeyCollector extends AbstractSortedCollector<PartitionKey, Keys>
    {
        private static final Comparator<Object> comparator = Comparator.comparing(v -> ((PartitionKey) v));

        public final TableMetadatas tables;

        public KeyCollector(TableMetadatas tables)
        {
            this.tables = tables;
        }

        public TableMetadatasAndKeys buildTablesAndKeys()
        {
            return new TableMetadatasAndKeys(tables, build());
        }

        @Override
        Comparator<Object> comparator()
        {
            return comparator;
        }

        public PartitionKey collect(TableMetadata table, DecoratedKey key)
        {
            TableId tableId = tables.get(table.id);
            if (count == 1)
            {
                PartitionKey one = (PartitionKey) buffer;
                if (one.prefix() == table && one.partitionKey().equals(key))
                    return one;
            }
            return collect(new PartitionKey(tableId, key));
        }

        @Override
        Keys empty()
        {
            return Keys.EMPTY;
        }

        @Override
        Keys of(PartitionKey one)
        {
            return Keys.of(one);
        }

        @Override
        Keys copy(Object[] array, int count)
        {
            Key[] result = new Key[count];
            System.arraycopy(array, 0, result, 0, count);
            return Keys.ofSortedUnique(result);
        }

        @Override
        Keys copyBtree(Object[] btree, int count)
        {
            return Keys.ofSortedUnique(new BTreeSet<>(btree, comparator()));
        }
    }

    private static final TableMetadatasAndKeys NO_KEYS = new TableMetadatasAndKeys(Multi.NONE, Keys.EMPTY);
    private static final TableMetadatasAndKeys NO_RANGES = new TableMetadatasAndKeys(Multi.NONE, Ranges.EMPTY);

    public static TableMetadatasAndKeys none(Routable.Domain domain)
    {
        return domain.isKey() ? NO_KEYS : NO_RANGES;
    }

    public final TableMetadatas tables;
    public final Seekables keys;

    public TableMetadatasAndKeys(TableMetadatas tables, Seekables keys)
    {
        this.tables = tables;
        this.keys = keys;
    }

    public void serializeKeys(Keys keys, DataOutputPlus out) throws IOException
    {
        serializeSubsetInternal(keys, this.keys, out);
    }

    public Keys deserializeKeys(DataInputPlus in) throws IOException
    {
        return (Keys)deserializeSubsetInternal(this.keys, in);
    }

    public void skipKeys(DataInputPlus in) throws IOException
    {
        skipSubsetInternal(this.keys.size(), in);
    }

    public void serializeSeekable(Seekable seekable, DataOutputPlus out) throws IOException
    {
        int index = keys.indexOf(seekable);
        if (index >= 0) out.writeUnsignedVInt32(1 + index);
        else
        {
            Invariants.require(seekable.domain() == Range);
            out.writeUnsignedVInt32(0);
            KeySerializers.seekable.serialize(seekable, out);
        }
    }

    public void serializeKey(PartitionKey key, DataOutputPlus out) throws IOException
    {
        int index = keys.indexOf(key);
        Invariants.require(index >= 0);
        out.writeUnsignedVInt32(index);
    }

    public Seekable deserializeSeekable(DataInputPlus in) throws IOException
    {
        int offset = in.readUnsignedVInt32();
        Seekable key;
        if (offset > 0) key = (Seekable) keys.get(offset - 1);
        else key = KeySerializers.seekable.deserialize(in);
        return key;
    }

    public void skipSeekable(DataInputPlus in) throws IOException
    {
        int offset = in.readUnsignedVInt32();
        if (offset <= 0) KeySerializers.seekable.skip(in);
    }

    public PartitionKey deserializeKey(DataInputPlus in) throws IOException
    {
        int offset = in.readUnsignedVInt32();
        return (PartitionKey) keys.get(offset);
    }

    public long serializedKeysSize(Keys keys)
    {
        return serializedSubsetSizeInternal(keys, this.keys);
    }

    public long serializedSeekableSize(Seekable seekable)
    {
        int i = keys.indexOf(seekable);
        Invariants.require(i >= 0 || seekable.domain() == Range);
        return VIntCoding.sizeOfUnsignedVInt(1 + i);
    }

    public long serializedKeySize(PartitionKey key)
    {
        int i = keys.indexOf(key);
        Invariants.require(i >= 0);
        return VIntCoding.sizeOfUnsignedVInt(i);
    }

    public TableMetadatasAndKeys slice(Ranges ranges)
    {
        return new TableMetadatasAndKeys(tables, keys.slice(ranges, Minimal));
    }

    @Override
    public TableMetadatasAndKeys intersecting(Participants<?> participants)
    {
        return new TableMetadatasAndKeys(tables, keys.intersecting(participants, Minimal));
    }

    @Override
    public TableMetadatasAndKeys merge(TableMetadatasAndKeys merge)
    {
        Invariants.require(tables.equals(merge.tables));
        return new TableMetadatasAndKeys(tables, keys.with(merge.keys));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadatasAndKeys that = (TableMetadatasAndKeys) o;
        return tables.equals(that.tables) && keys.equals(that.keys);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "{tables=" + tables + ",keys=" + keys + '}';
    }

    public static final UnversionedSerializer<TableMetadatasAndKeys> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out) throws IOException
        {
            tablesAndKeys.tables.serializeSelf(out);
            KeySerializers.seekables.serialize(tablesAndKeys.keys, out);
        }

        @Override
        public TableMetadatasAndKeys deserialize(DataInputPlus in) throws IOException
        {
            TableMetadatas tables = TableMetadatas.deserializeSelf(in);
            Seekables keys = KeySerializers.seekables.deserialize(in);
            return new TableMetadatasAndKeys(tables, keys);
        }

        @Override
        public long serializedSize(TableMetadatasAndKeys tablesAndKeys)
        {
            return tablesAndKeys.tables.serializedSelfSize()
                   + KeySerializers.seekables.serializedSize(tablesAndKeys.keys);
        }
    };
}
