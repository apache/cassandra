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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.RepairCallback;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceFromReadCommand extends ReadCommand
{
    static final Logger logger = LoggerFactory.getLogger(SliceFromReadCommand.class);

    public final SliceQueryFilter filter;

    public SliceFromReadCommand(String table, ByteBuffer key, ColumnParent column_parent, ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(table, key, new QueryPath(column_parent), start, finish, reversed, count);
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, ByteBuffer start, ByteBuffer finish, boolean reversed, int count)
    {
        this(table, key, path, new SliceQueryFilter(start, finish, reversed, count));
    }

    public SliceFromReadCommand(String table, ByteBuffer key, QueryPath path, SliceQueryFilter filter)
    {
        super(table, key, path, CMD_TYPE_GET_SLICE);
        this.filter = filter;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, queryPath, filter);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table)
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(new QueryFilter(dk, queryPath, filter));
    }

    @Override
    public ReadCommand maybeGenerateRetryCommand(RepairCallback handler, Row row)
    {
        int maxLiveColumns = handler.getMaxLiveColumns();
        int liveColumnsInRow = row != null ? row.getLiveColumnCount() : 0;

        int count = filter.count;
        assert maxLiveColumns <= count;
        // We generate a retry if at least one node reply with count live columns but after merge we have less
        // than the total number of column we are interested in (which may be < count on a retry)
        if ((maxLiveColumns == count) && (liveColumnsInRow < getOriginalRequestedCount()))
        {
            // We asked t (= count) live columns and got l (=liveColumnsInRow) ones.
            // From that, we can estimate that on this row, for x requested
            // columns, only l/t end up live after reconciliation. So for next
            // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
            int retryCount = liveColumnsInRow == 0 ? count + 1 : ((count * count) / liveColumnsInRow) + 1;
            SliceQueryFilter newFilter = filter.withUpdatedCount(retryCount);
            return new RetriedSliceFromReadCommand(table, key, queryPath, newFilter, getOriginalRequestedCount());
        }

        return null;
    }

    @Override
    public void maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return;

        int liveColumnsInRow = row.cf.getLiveColumnCount();

        if (liveColumnsInRow > getOriginalRequestedCount())
        {
            int columnsToTrim = liveColumnsInRow - getOriginalRequestedCount();

            logger.debug("trimming {} live columns to the originally requested {}", row.cf.getLiveColumnCount(), getOriginalRequestedCount());

            Collection<IColumn> columns;
            if (filter.reversed)
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

    /**
     * The original number of columns requested by the user.
     * This can be different from count when the slice command is a retry (see
     * RetriedSliceFromReadCommand)
     */
    protected int getOriginalRequestedCount()
    {
        return filter.count;
    }

    @Override
    public String toString()
    {
        return "SliceFromReadCommand(" +
               "table='" + table + '\'' +
               ", key='" + ByteBufferUtil.bytesToHex(key) + '\'' +
               ", column_parent='" + queryPath + '\'' +
               ", filter='" + filter + '\'' +
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
        SliceQueryFilter.serializer.serialize(realRM.filter, dos, version);
    }

    public ReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);
        QueryPath path = QueryPath.deserialize(dis);
        SliceQueryFilter filter = SliceQueryFilter.serializer.deserialize(dis, version);
        SliceFromReadCommand rm = new SliceFromReadCommand(table, key, path, filter);
        rm.setDigestQuery(isDigest);
        return rm;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceFromReadCommand command = (SliceFromReadCommand) cmd;
        int keySize = command.key.remaining();

        int size = sizes.sizeof(cmd.isDigestQuery()); // boolean
        size += sizes.sizeof(command.table);
        size += sizes.sizeof((short) keySize) + keySize;
        size += command.queryPath.serializedSize(sizes);
        size += SliceQueryFilter.serializer.serializedSize(command.filter, version);
        return size;
    }
}
