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
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceFromReadCommand extends ReadCommand
{
    static final Logger logger = LoggerFactory.getLogger(SliceFromReadCommand.class);

    static final SliceFromReadCommandSerializer serializer = new SliceFromReadCommandSerializer();

    public final SliceQueryFilter filter;

    public SliceFromReadCommand(String table, ByteBuffer key, String cfName, SliceQueryFilter filter)
    {
        super(table, key, cfName, Type.GET_SLICES);
        this.filter = filter;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(table, key, cfName, filter);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Table table)
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return table.getRow(new QueryFilter(dk, cfName, filter));
    }

    @Override
    public ReadCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        int maxLiveColumns = resolver.getMaxLiveCount();

        int count = filter.count;
        assert maxLiveColumns <= count;
        // We generate a retry if at least one node reply with count live columns but after merge we have less
        // than the total number of column we are interested in (which may be < count on a retry)
        if (maxLiveColumns != count)
            return null;

        int liveCountInRow = row == null || row.cf == null ? 0 : filter.getLiveCount(row.cf);
        if (liveCountInRow < getOriginalRequestedCount())
        {
            // We asked t (= count) live columns and got l (=liveCountInRow) ones.
            // From that, we can estimate that on this row, for x requested
            // columns, only l/t end up live after reconciliation. So for next
            // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
            int retryCount = liveCountInRow == 0 ? count + 1 : ((count * count) / liveCountInRow) + 1;
            SliceQueryFilter newFilter = filter.withUpdatedCount(retryCount);
            return new RetriedSliceFromReadCommand(table, key, cfName, newFilter, getOriginalRequestedCount());
        }

        return null;
    }

    @Override
    public void maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return;

        filter.trim(row.cf, getOriginalRequestedCount());
    }

    public IDiskAtomFilter filter()
    {
        return filter;
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
               ", cfName='" + cfName + '\'' +
               ", filter='" + filter + '\'' +
               ')';
    }
}

class SliceFromReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand rm, DataOutput dos, int version) throws IOException
    {
        serialize(rm, null, dos, version);
    }

    public void serialize(ReadCommand rm, ByteBuffer superColumn, DataOutput dos, int version) throws IOException
    {
        SliceFromReadCommand realRM = (SliceFromReadCommand)rm;
        dos.writeBoolean(realRM.isDigestQuery());
        dos.writeUTF(realRM.table);
        ByteBufferUtil.writeWithShortLength(realRM.key, dos);

        if (version < MessagingService.VERSION_20)
            new QueryPath(realRM.cfName, superColumn).serialize(dos);
        else
            dos.writeUTF(realRM.cfName);

        SliceQueryFilter.serializer.serialize(realRM.filter, dos, version);
    }

    public ReadCommand deserialize(DataInput dis, int version) throws IOException
    {
        boolean isDigest = dis.readBoolean();
        String table = dis.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(dis);

        String cfName;
        ByteBuffer sc = null;
        if (version < MessagingService.VERSION_20)
        {
            QueryPath path = QueryPath.deserialize(dis);
            cfName = path.columnFamilyName;
            sc = path.superColumnName;
        }
        else
        {
            cfName = dis.readUTF();
        }

        CFMetaData metadata = Schema.instance.getCFMetaData(table, cfName);
        SliceQueryFilter filter;
        if (version < MessagingService.VERSION_20)
        {
            filter = SliceQueryFilter.serializer.deserialize(dis, version);

            if (metadata.cfType == ColumnFamilyType.Super)
                filter = SuperColumns.fromSCSliceFilter((CompositeType)metadata.comparator, sc, filter);
        }
        else
        {
            filter = SliceQueryFilter.serializer.deserialize(dis, version);
        }

        ReadCommand command = new SliceFromReadCommand(table, key, cfName, filter);
        command.setDigestQuery(isDigest);
        return command;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        return serializedSize(cmd, null, version);
    }

    public long serializedSize(ReadCommand cmd, ByteBuffer superColumn, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceFromReadCommand command = (SliceFromReadCommand) cmd;
        int keySize = command.key.remaining();

        int size = sizes.sizeof(cmd.isDigestQuery()); // boolean
        size += sizes.sizeof(command.table);
        size += sizes.sizeof((short) keySize) + keySize;

        if (version < MessagingService.VERSION_20)
        {
            size += new QueryPath(command.cfName, superColumn).serializedSize(sizes);
        }
        else
        {
            size += sizes.sizeof(command.cfName);
        }

        size += SliceQueryFilter.serializer.serializedSize(command.filter, version);
        return size;
    }
}
