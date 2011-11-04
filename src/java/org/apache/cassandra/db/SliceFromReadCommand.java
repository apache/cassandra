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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.RepairCallback;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SliceFromReadCommand extends ReadCommand
{
    static final Logger logger = LoggerFactory.getLogger(SliceFromReadCommand.class);

    public final ByteBuffer start, finish;
    public final boolean reversed;
    public final int count;

    public SliceFromReadCommand(String table, ByteBuffer key, ColumnParent column_parent, ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(table, key, new QueryPath(column_parent), start, finish, reversed, count);
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE);
        this.start = start;
        this.finish = finish;
        this.reversed = reversed;
        this.count = count;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, queryPath, start, finish, reversed, count);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table) throws IOException
    {
        DecoratedKey<?> dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(QueryFilter.getSliceFilter(dk, queryPath, start, finish, reversed, count));
    }

    @Override
    public ReadCommand maybeGenerateRetryCommand(RepairCallback handler, Row row)
    {
        int maxLiveColumns = handler.getMaxLiveColumns();
        int liveColumnsInRow = row != null ? row.cf.getLiveColumnCount() : 0;

        assert maxLiveColumns <= count;
        if ((maxLiveColumns == count) && (liveColumnsInRow < count))
        {
            int retryCount = count + count - liveColumnsInRow;
            return new RetriedSliceFromReadCommand(table, key, queryPath, start, finish, reversed, count, retryCount);
        }

        return null;
    }

    @Override
    public void maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return;

        int liveColumnsInRow = row.cf.getLiveColumnCount();

        if (liveColumnsInRow > getRequestedCount())
        {
            int columnsToTrim = liveColumnsInRow - getRequestedCount();

            logger.debug("trimming {} live columns to the originally requested {}", row.cf.getLiveColumnCount(), getRequestedCount());

            Collection<IColumn> columns;
            if (reversed)
                columns = row.cf.getSortedColumns();
            else
                columns = row.cf.getReverseSortedColumns();

            Collection<ByteBuffer> toRemove = new HashSet<ByteBuffer>();

            Iterator<IColumn> columnIterator = columns.iterator();
            while (columnIterator.hasNext() && (toRemove.size() < columnsToTrim))
            {
                IColumn column = columnIterator.next();
                if (column.isLive())
                    toRemove.add(column.name());
            }

            for (ByteBuffer columnName : toRemove)
            {
                row.cf.remove(columnName);
            }
        }
    }

    protected int getRequestedCount()
    {
        return count;
    }

    @Override
    public String toString()
    {
        return "SliceFromReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + ByteBufferUtil.bytesToHex(key) + '\'' +
               ", column_parent='" + queryPath + '\'' +
               ", start='" + getComparator().getString(start) + '\'' +
               ", finish='" + getComparator().getString(finish) + '\'' +
               ", reversed=" + reversed +
               ", count=" + count +
               ')';
    }
}

class SliceFromReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand rm, DataOutput dos, int version) throws IOException
    {
        SliceFromReadCommand realRM = (SliceFromReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        ByteBufferUtil.writeWithShortLength(realRM.key, dos);
        realRM.queryPath.serialize(dos);
        ByteBufferUtil.writeWithShortLength(realRM.start, dos);
        ByteBufferUtil.writeWithShortLength(realRM.finish, dos);
        dos.writeBoolean(realRM.reversed);
        dos.writeInt(realRM.count);
    }

    public ReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        SliceFromReadCommand rm = new SliceFromReadCommand(dis.readUTF(),
                                                           ByteBufferUtil.readWithShortLength(dis),
                                                           QueryPath.deserialize(dis),
                                                           ByteBufferUtil.readWithShortLength(dis),
                                                           ByteBufferUtil.readWithShortLength(dis),
                                                           dis.readBoolean(),
                                                           dis.readInt());
        rm.setDigestQuery(isDigest);
        return rm;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        SliceFromReadCommand command = (SliceFromReadCommand) cmd;
        int size = DBConstants.boolSize;
        size += DBConstants.shortSize + FBUtilities.encodedUTF8Length(command.table);
        size += DBConstants.shortSize + command.key.remaining();
        size += command.queryPath.serializedSize();
        size += DBConstants.shortSize + command.start.remaining();
        size += DBConstants.shortSize + command.finish.remaining();
        size += DBConstants.boolSize;
        size += DBConstants.intSize;
        return size;
    }
}
