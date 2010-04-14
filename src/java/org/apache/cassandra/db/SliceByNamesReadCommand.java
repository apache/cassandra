/**
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.FBUtilities;

public class SliceByNamesReadCommand extends ReadCommand
{
    public final SortedSet<byte[]> columnNames;

    public SliceByNamesReadCommand(String table, byte[] key, ColumnParent column_parent, Collection<byte[]> columnNames)
    {
        this(table, key, new QueryPath(column_parent), columnNames);
    }

    public SliceByNamesReadCommand(String table, byte[] key, QueryPath path, Collection<byte[]> columnNames)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE_BY_NAMES);
        this.columnNames = new TreeSet<byte[]>(getComparator());
        this.columnNames.addAll(columnNames);
    }

    @Override
    public ReadCommand copy()
    {
        ReadCommand readCommand= new SliceByNamesReadCommand(table, key, queryPath, columnNames);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }
    
    @Override
    public Row getRow(Table table) throws IOException
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(QueryFilter.getNamesFilter(dk, queryPath, columnNames));
    }

    @Override
    public String toString()
    {
        return "SliceByNamesReadCommand(" +
               "table='" + table + '\'' +
               ", key=" + FBUtilities.bytesToHex(key) +
               ", columnParent='" + queryPath + '\'' +
               ", columns=[" + getComparator().getString(columnNames) + "]" +
               ')';
    }

}

class SliceByNamesReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos) throws IOException
    {
        SliceByNamesReadCommand realRM = (SliceByNamesReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        FBUtilities.writeShortByteArray(realRM.key, dos);
        realRM.queryPath.serialize(dos);
        dos.writeInt(realRM.columnNames.size());
        if (realRM.columnNames.size() > 0)
        {
            for (byte[] cName : realRM.columnNames)
            {
                FBUtilities.writeShortByteArray(cName, dos);
            }
        }
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        byte[] key = FBUtilities.readShortByteArray(dis);
        QueryPath columnParent = QueryPath.deserialize(dis);

        int size = dis.readInt();
        List<byte[]> columns = new ArrayList<byte[]>();
        for (int i = 0; i < size; ++i)
        {
            columns.add(FBUtilities.readShortByteArray(dis));
        }
        SliceByNamesReadCommand rm = new SliceByNamesReadCommand(table, key, columnParent, columns);
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
