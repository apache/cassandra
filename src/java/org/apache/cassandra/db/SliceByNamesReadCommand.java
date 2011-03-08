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
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceByNamesReadCommand extends ReadCommand
{
    public final SortedSet<ByteBuffer> columnNames;

    public SliceByNamesReadCommand(String table, ByteBuffer key, ColumnParent column_parent, Collection<ByteBuffer> columnNames)
    {
        this(table, key, new QueryPath(column_parent), columnNames);
    }

    public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, Collection<ByteBuffer> columnNames)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE_BY_NAMES);
        this.columnNames = new TreeSet<ByteBuffer>(getComparator());
        this.columnNames.addAll(columnNames);
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand= new SliceByNamesReadCommand(table, key, queryPath, columnNames);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }
    
    public Row getRow(Table table) throws IOException
    {
        DecoratedKey<?> dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(QueryFilter.getNamesFilter(dk, queryPath, columnNames));
    }

    @Override
    public String toString()
    {
        return "SliceByNamesReadCommand(" +
               "table='" + table + '\'' +
               ", key=" + ByteBufferUtil.bytesToHex(key) +
               ", columnParent='" + queryPath + '\'' +
               ", columns=[" + getComparator().getString(columnNames) + "]" +
               ')';
    }

}

class SliceByNamesReadCommandSerializer extends ReadCommandSerializer
{
    @Override
    public void serialize(ReadCommand rm, DataOutputStream dos, int version) throws IOException
    {
        SliceByNamesReadCommand realRM = (SliceByNamesReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        ByteBufferUtil.writeWithShortLength(realRM.key, dos);
        realRM.queryPath.serialize(dos);
        dos.writeInt(realRM.columnNames.size());
        if (realRM.columnNames.size() > 0)
        {
            for (ByteBuffer cName : realRM.columnNames)
            {
                ByteBufferUtil.writeWithShortLength(cName, dos);
            }
        }
    }

    @Override
    public ReadCommand deserialize(DataInputStream dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
        QueryPath columnParent = QueryPath.deserialize(dis);

        int size = dis.readInt();
        List<ByteBuffer> columns = new ArrayList<ByteBuffer>();
        for (int i = 0; i < size; ++i)
        {
            columns.add(ByteBufferUtil.readWithShortLength(dis));
        }
        SliceByNamesReadCommand rm = new SliceByNamesReadCommand(table, key, columnParent, columns);
        rm.setDigestQuery(isDigest);
        return rm;
    }
}
