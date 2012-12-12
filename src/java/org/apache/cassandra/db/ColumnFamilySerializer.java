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
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class ColumnFamilySerializer implements IVersionedSerializer<ColumnFamily>, ISSTableSerializer<ColumnFamily>
{
    /*
     * Serialized ColumnFamily format:
     *
     * [serialized for intra-node writes only, e.g. returning a query result]
     * <cf nullability boolean: false if the cf is null>
     * <cf id>
     *
     * [in sstable only]
     * <column bloom filter>
     * <sparse column index, start/finish columns every ColumnIndexSizeInKB of data>
     *
     * [always present]
     * <local deletion time>
     * <client-provided deletion time>
     * <column count>
     * <columns, serialized individually>
    */
    public void serialize(ColumnFamily cf, DataOutput dos, int version)
    {
        try
        {
            if (cf == null)
            {
                dos.writeBoolean(false);
                return;
            }

            dos.writeBoolean(true);
            serializeCfId(cf.id(), dos, version);

            if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
            {
                SuperColumns.serializeSuperColumnFamily(cf, dos, version);
                return;
            }

            DeletionInfo.serializer().serialize(cf.deletionInfo(), dos, version);
            ColumnSerializer columnSerializer = Column.serializer();
            int count = cf.getColumnCount();
            dos.writeInt(count);
            int written = 0;
            for (Column column : cf)
            {
                columnSerializer.serialize(column, dos);
                written++;
            }
            assert count == written: "Column family had " + count + " columns, but " + written + " written";
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ColumnFamily deserialize(DataInput dis, int version) throws IOException
    {
        return deserialize(dis, ColumnSerializer.Flag.LOCAL, TreeMapBackedSortedColumns.factory(), version);
    }

    public ColumnFamily deserialize(DataInput dis, ColumnSerializer.Flag flag, ISortedColumns.Factory factory, int version) throws IOException
    {
        if (!dis.readBoolean())
            return null;

        ColumnFamily cf = ColumnFamily.create(deserializeCfId(dis, version), factory);
        int expireBefore = (int) (System.currentTimeMillis() / 1000);

        if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
        {
            SuperColumns.deserializerSuperColumnFamily(dis, cf, flag, expireBefore, version);
        }
        else
        {
            cf.delete(DeletionInfo.serializer().deserialize(dis, version, cf.getComparator()));

            ColumnSerializer columnSerializer = Column.serializer();
            int size = dis.readInt();
            for (int i = 0; i < size; ++i)
            {
                cf.addColumn(columnSerializer.deserialize(dis, flag, expireBefore));
            }
        }
        return cf;
    }

    public long contentSerializedSize(ColumnFamily cf, TypeSizes typeSizes, int version)
    {
        long size = 0L;

        if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
        {
            size += SuperColumns.serializedSize(cf, typeSizes, version);
        }
        else
        {
            size += DeletionInfo.serializer().serializedSize(cf.deletionInfo(), typeSizes, version);
            size += typeSizes.sizeof(cf.getColumnCount());
            for (Column column : cf)
                size += column.serializedSize(typeSizes);
        }
        return size;
    }

    public long serializedSize(ColumnFamily cf, TypeSizes typeSizes, int version)
    {
        if (cf == null)
        {
            return typeSizes.sizeof(false);
        }
        else
        {
            return typeSizes.sizeof(true)  /* nullness bool */
                 + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
                 + contentSerializedSize(cf, typeSizes, version);
        }
    }

    public long serializedSize(ColumnFamily cf, int version)
    {
        return serializedSize(cf, TypeSizes.NATIVE, version);
    }

    public void serializeForSSTable(ColumnFamily cf, DataOutput dos)
    {
        // Column families shouldn't be written directly to disk, use ColumnIndex.Builder instead
        throw new UnsupportedOperationException();
    }

    public ColumnFamily deserializeFromSSTable(DataInput dis, Descriptor.Version version)
    {
        throw new UnsupportedOperationException();
    }

    public void deserializeColumnsFromSSTable(DataInput dis, ColumnFamily cf, int size, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
    {
        Iterator<OnDiskAtom> iter = cf.metadata().getOnDiskIterator(dis, size, flag, expireBefore, version);
        while (iter.hasNext())
            cf.addAtom(iter.next());
    }

    public void deserializeFromSSTable(DataInput dis, ColumnFamily cf, ColumnSerializer.Flag flag, Descriptor.Version version) throws IOException
    {
        cf.delete(DeletionInfo.serializer().deserializeFromSSTable(dis, version));
        int size = dis.readInt();
        int expireBefore = (int) (System.currentTimeMillis() / 1000);
        deserializeColumnsFromSSTable(dis, cf, size, flag, expireBefore, version);
    }

    public void serializeCfId(UUID cfId, DataOutput dos, int version) throws IOException
    {
        if (version < MessagingService.VERSION_12) // try to use CF's old id where possible (CASSANDRA-3794)
        {
            Integer oldId = Schema.instance.convertNewCfId(cfId);

            if (oldId == null)
                throw new IOException("Can't serialize ColumnFamily ID " + cfId + " to be used by version " + version +
                                      ", because int <-> uuid mapping could not be established (CF was created in mixed version cluster).");

            dos.writeInt(oldId);
        }
        else
            UUIDSerializer.serializer.serialize(cfId, dos, version);
    }

    public UUID deserializeCfId(DataInput dis, int version) throws IOException
    {
        // create a ColumnFamily based on the cf id
        UUID cfId = (version < MessagingService.VERSION_12)
                     ? Schema.instance.convertOldCfId(dis.readInt())
                     : UUIDSerializer.serializer.deserialize(dis, version);

        if (Schema.instance.getCF(cfId) == null)
            throw new UnknownColumnFamilyException("Couldn't find cfId=" + cfId, cfId);

        return cfId;
    }

    public int cfIdSerializedSize(UUID cfId, TypeSizes typeSizes, int version)
    {
        if (version < MessagingService.VERSION_12) // try to use CF's old id where possible (CASSANDRA-3794)
        {
            Integer oldId = Schema.instance.convertNewCfId(cfId);

            if (oldId == null)
                throw new RuntimeException("Can't serialize ColumnFamily ID " + cfId + " to be used by version " + version +
                        ", because int <-> uuid mapping could not be established (CF was created in mixed version cluster).");

            return typeSizes.sizeof(oldId);
        }

        return typeSizes.sizeof(cfId);
    }
}
