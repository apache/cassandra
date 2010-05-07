package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataOutput;
import java.io.IOException;
import java.io.DataInput;
import java.util.Collection;

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.db.marshal.AbstractType;

public class ColumnFamilySerializer implements ICompactSerializer2<ColumnFamily>
{
    /*
     * Serialized ColumnFamily format:
     *
     * [serialized for intra-node writes only, e.g. returning a query result]
     * <cf name>
     * <cf type [super or standard]>
     * <cf comparator name>
     * <cf subcolumn comparator name>
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
    public void serialize(ColumnFamily columnFamily, DataOutput dos)
    {
        try
        {
            if (columnFamily == null)
            {
                dos.writeUTF(""); // not a legal CF name
                return;
            }

            dos.writeUTF(columnFamily.name());
            dos.writeInt(columnFamily.id());
            dos.writeUTF(columnFamily.type_.name());
            dos.writeUTF(columnFamily.getComparatorName());
            dos.writeUTF(columnFamily.getSubComparatorName());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        serializeForSSTable(columnFamily, dos);
    }

    public void serializeForSSTable(ColumnFamily columnFamily, DataOutput dos)
    {
        try
        {
            dos.writeInt(columnFamily.localDeletionTime.get());
            dos.writeLong(columnFamily.markedForDeleteAt.get());

            Collection<IColumn> columns = columnFamily.getSortedColumns();
            dos.writeInt(columns.size());
            for (IColumn column : columns)
            {
                columnFamily.getColumnSerializer().serialize(column, dos);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void serializeWithIndexes(ColumnFamily columnFamily, DataOutput dos)
    {
        ColumnIndexer.serialize(columnFamily, dos);
        serializeForSSTable(columnFamily, dos);
    }

    public ColumnFamily deserialize(DataInput dis) throws IOException
    {
        String cfName = dis.readUTF();
        if (cfName.isEmpty())
            return null;
        int id = dis.readInt();
        ColumnFamilyType cfType = ColumnFamilyType.create(dis.readUTF());
        ColumnFamily cf = deserializeFromSSTableNoColumns(cfName, cfType, readComparator(dis), readComparator(dis), id, dis);
        deserializeColumns(dis, cf);
        return cf;
    }

    private void deserializeColumns(DataInput dis, ColumnFamily cf) throws IOException
    {
        int size = dis.readInt();
        for (int i = 0; i < size; ++i)
        {
            IColumn column = cf.getColumnSerializer().deserialize(dis);
            cf.addColumn(column);
        }
    }

    private AbstractType readComparator(DataInput dis) throws IOException
    {
        String className = dis.readUTF();
        if (className.equals(""))
        {
            return null;
        }

        try
        {
            return (AbstractType)Class.forName(className).getConstructor().newInstance();
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Unable to load comparator class '" + className + "'.  probably this means you have obsolete sstables lying around", e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private ColumnFamily deserializeFromSSTableNoColumns(String name, ColumnFamilyType type, AbstractType comparator, AbstractType subComparator, int id, DataInput input) throws IOException
    {
        ColumnFamily cf = new ColumnFamily(name, type, comparator, subComparator, id);
        return deserializeFromSSTableNoColumns(cf, input);
    }

    public ColumnFamily deserializeFromSSTableNoColumns(ColumnFamily cf, DataInput input) throws IOException
    {
        cf.delete(input.readInt(), input.readLong());
        return cf;
    }

    public ColumnFamily deserializeFromSSTable(SSTableReader sstable, DataInput file) throws IOException
    {
        ColumnFamily cf = sstable.makeColumnFamily();
        deserializeFromSSTableNoColumns(cf, file);
        deserializeColumns(file, cf);
        return cf;
    }
}
