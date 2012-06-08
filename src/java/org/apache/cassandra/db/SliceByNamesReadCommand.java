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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceByNamesReadCommand extends ReadCommand
{
    public final NamesQueryFilter filter;

    public SliceByNamesReadCommand(String table, ByteBuffer key, ColumnParent column_parent, Collection<ByteBuffer> columnNames)
    {
        this(table, key, new QueryPath(column_parent), columnNames);
    }

    public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, Collection<ByteBuffer> columnNames)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE_BY_NAMES);
        SortedSet s = new TreeSet<ByteBuffer>(getComparator());
        s.addAll(columnNames);
        this.filter = new NamesQueryFilter(s);
    }

    public SliceByNamesReadCommand(String table, ByteBuffer key, QueryPath path, NamesQueryFilter filter)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE_BY_NAMES);
        this.filter = filter;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand= new SliceByNamesReadCommand(table, key, queryPath, filter);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table) throws IOException
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(new QueryFilter(dk, queryPath, filter));
    }

    @Override
    public String toString()
    {
        return "SliceByNamesReadCommand(" +
               "table='" + table + '\'' +
               ", key=" + ByteBufferUtil.bytesToHex(key) +
               ", columnParent='" + queryPath + '\'' +
               ", filter=" + filter +
               ')';
    }
}

class SliceByNamesReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand cmd, DataOutput dos, int version) throws IOException
    {
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        dos.writeBoolean(command.isDigestQuery());
        dos.writeUTF(command.table);
        ByteBufferUtil.writeWithShortLength(command.key, dos);
        command.queryPath.serialize(dos);
        NamesQueryFilter.serializer.serialize(command.filter, dos, version);
    }

    public SliceByNamesReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
        QueryPath columnParent = QueryPath.deserialize(dis);

        AbstractType<?> comparator = ColumnFamily.getComparatorFor(table, columnParent.columnFamilyName, columnParent.superColumnName);
        NamesQueryFilter filter = NamesQueryFilter.serializer.deserialize(dis, version, comparator);
        SliceByNamesReadCommand command = new SliceByNamesReadCommand(table, key, columnParent, filter);
        command.setDigestQuery(isDigest);
        return command;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        int size = sizes.sizeof(command.isDigestQuery());
        int keySize = command.key.remaining();

        size += sizes.sizeof(command.table);
        size += sizes.sizeof((short)keySize) + keySize;
        size += command.queryPath.serializedSize(sizes);
        size += NamesQueryFilter.serializer.serializedSize(command.filter, version);
        return size;
    }
}
