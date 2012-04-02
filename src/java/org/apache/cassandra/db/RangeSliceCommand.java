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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class RangeSliceCommand implements IReadCommand
{
    public static final RangeSliceCommandSerializer serializer = new RangeSliceCommandSerializer();

    public final String keyspace;

    public final String column_family;
    public final ByteBuffer super_column;

    public final SlicePredicate predicate;
    public final List<IndexExpression> row_filter;

    public final AbstractBounds<RowPosition> range;
    public final int maxResults;
    public final boolean maxIsColumns;
    public final boolean isPaging;

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, SlicePredicate predicate, AbstractBounds<RowPosition> range, int maxResults)
    {
        this(keyspace, column_family, super_column, predicate, range, null, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, SlicePredicate predicate, AbstractBounds<RowPosition> range, int maxResults, boolean maxIsColumns, boolean isPaging)
    {
        this(keyspace, column_family, super_column, predicate, range, null, maxResults, maxIsColumns, false);
    }

    public RangeSliceCommand(String keyspace, ColumnParent column_parent, SlicePredicate predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults)
    {
        this(keyspace, column_parent.getColumn_family(), column_parent.super_column, predicate, range, row_filter, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, ColumnParent column_parent, SlicePredicate predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults, boolean maxIsColumns, boolean isPaging)
    {
        this(keyspace, column_parent.getColumn_family(), column_parent.super_column, predicate, range, row_filter, maxResults, maxIsColumns, isPaging);
    }

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, SlicePredicate predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults)
    {
        this(keyspace, column_family, super_column, predicate, range, row_filter, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, SlicePredicate predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults, boolean maxIsColumns, boolean isPaging)
    {
        this.keyspace = keyspace;
        this.column_family = column_family;
        this.super_column = super_column;
        this.predicate = predicate;
        this.range = range;
        this.row_filter = row_filter;
        this.maxResults = maxResults;
        this.maxIsColumns = maxIsColumns;
        this.isPaging = isPaging;
    }

    public MessageOut<RangeSliceCommand> createMessage()
    {
        return new MessageOut<RangeSliceCommand>(MessagingService.Verb.RANGE_SLICE, this, serializer);
    }

    @Override
    public String toString()
    {
        return "RangeSliceCommand{" +
               "keyspace='" + keyspace + '\'' +
               ", column_family='" + column_family + '\'' +
               ", super_column=" + super_column +
               ", predicate=" + predicate +
               ", range=" + range +
               ", row_filter =" + row_filter +
               ", maxResults=" + maxResults +
               ", maxIsColumns=" + maxIsColumns +
               '}';
    }

    public String getKeyspace()
    {
        return keyspace;
    }
}

class RangeSliceCommandSerializer implements IVersionedSerializer<RangeSliceCommand>
{
    public void serialize(RangeSliceCommand sliceCommand, DataOutput dos, int version) throws IOException
    {
        dos.writeUTF(sliceCommand.keyspace);
        dos.writeUTF(sliceCommand.column_family);
        ByteBuffer sc = sliceCommand.super_column;
        dos.writeInt(sc == null ? 0 : sc.remaining());
        if (sc != null)
            ByteBufferUtil.write(sc, dos);

        TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
        FBUtilities.serialize(ser, sliceCommand.predicate, dos);

        if (version >= MessagingService.VERSION_11)
        {
            if (sliceCommand.row_filter == null)
            {
                dos.writeInt(0);
            }
            else
            {
                dos.writeInt(sliceCommand.row_filter.size());
                for (IndexExpression expr : sliceCommand.row_filter)
                    FBUtilities.serialize(ser, expr, dos);
            }
        }
        AbstractBounds.serializer().serialize(sliceCommand.range, dos, version);
        dos.writeInt(sliceCommand.maxResults);
        if (version >= MessagingService.VERSION_11)
        {
            dos.writeBoolean(sliceCommand.maxIsColumns);
            dos.writeBoolean(sliceCommand.isPaging);
        }
    }

    public RangeSliceCommand deserialize(DataInput dis, int version) throws IOException
    {
        String keyspace = dis.readUTF();
        String columnFamily = dis.readUTF();

        int scLength = dis.readInt();
        ByteBuffer superColumn = null;
        if (scLength > 0)
        {
            byte[] buf = new byte[scLength];
            dis.readFully(buf);
            superColumn = ByteBuffer.wrap(buf);
        }

        TDeserializer dser = new TDeserializer(new TBinaryProtocol.Factory());
        SlicePredicate pred = new SlicePredicate();
        FBUtilities.deserialize(dser, pred, dis);

        List<IndexExpression> rowFilter = null;
        if (version >= MessagingService.VERSION_11)
        {
            int filterCount = dis.readInt();
            rowFilter = new ArrayList<IndexExpression>(filterCount);
            for (int i = 0; i < filterCount; i++)
            {
                IndexExpression expr = new IndexExpression();
                FBUtilities.deserialize(dser, expr, dis);
                rowFilter.add(expr);
            }
        }
        AbstractBounds<RowPosition> range = AbstractBounds.serializer().deserialize(dis, version).toRowBounds();

        int maxResults = dis.readInt();
        boolean maxIsColumns = false;
        boolean isPaging = false;
        if (version >= MessagingService.VERSION_11)
        {
            maxIsColumns = dis.readBoolean();
            isPaging = dis.readBoolean();
        }
        return new RangeSliceCommand(keyspace, columnFamily, superColumn, pred, range, rowFilter, maxResults, maxIsColumns, isPaging);
    }

    public long serializedSize(RangeSliceCommand rangeSliceCommand, int version)
    {
        int ksLength = FBUtilities.encodedUTF8Length(rangeSliceCommand.keyspace);
        long size = DBTypeSizes.NATIVE.sizeof(ksLength) + ksLength;
        int cfLength = FBUtilities.encodedUTF8Length(rangeSliceCommand.column_family);
        size += DBTypeSizes.NATIVE.sizeof(cfLength) + cfLength;

        ByteBuffer sc = rangeSliceCommand.super_column;
        if (sc != null)
        {
            size += DBTypeSizes.NATIVE.sizeof(sc.remaining());
            size += sc.remaining();
        }
        else
        {
            size += DBTypeSizes.NATIVE.sizeof(0);
        }

        TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            int predicateLength = ser.serialize(rangeSliceCommand.predicate).length;
            size += DBTypeSizes.NATIVE.sizeof(predicateLength);
            size += predicateLength;
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }

        if (version >= MessagingService.VERSION_11)
        {
            if (rangeSliceCommand.row_filter == null)
            {
                size += DBTypeSizes.NATIVE.sizeof(0);
            }
            else
            {
                size += DBTypeSizes.NATIVE.sizeof(rangeSliceCommand.row_filter.size());
                for (IndexExpression expr : rangeSliceCommand.row_filter)
                {
                    try
                    {
                        int filterLength = ser.serialize(expr).length;
                        size += DBTypeSizes.NATIVE.sizeof(filterLength);
                        size += filterLength;
                    }
                    catch (TException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        size += AbstractBounds.serializer().serializedSize(rangeSliceCommand.range, version);
        size += DBTypeSizes.NATIVE.sizeof(rangeSliceCommand.maxResults);
        if (version >= MessagingService.VERSION_11)
        {
            size += DBTypeSizes.NATIVE.sizeof(rangeSliceCommand.maxIsColumns);
            size += DBTypeSizes.NATIVE.sizeof(rangeSliceCommand.isPaging);
        }
        return size;
    }
}
