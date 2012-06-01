/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

public class RangeSliceCommand implements MessageProducer, IReadCommand
{
    private static final RangeSliceCommandSerializer serializer = new RangeSliceCommandSerializer();

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

    public Message getMessage(Integer version) throws IOException
    {
        if (version < MessagingService.VERSION_11 && row_filter != null && !row_filter.isEmpty())
        {
            // pre-1.1 versions use IndexScanCommand for index queries, so generate that instead
            return toIndexScanCommand().getMessage(version);
        }

        DataOutputBuffer dob = new DataOutputBuffer();
        serializer.serialize(this, dob, version);
        return new Message(FBUtilities.getBroadcastAddress(),
                           StorageService.Verb.RANGE_SLICE,
                           Arrays.copyOf(dob.getData(), dob.getLength()), version);
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

    public static RangeSliceCommand read(Message message) throws IOException
    {
        byte[] bytes = message.getMessageBody();
        FastByteArrayInputStream bis = new FastByteArrayInputStream(bytes);
        return serializer.deserialize(new DataInputStream(bis), message.getVersion());
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    // Convert to a equivalent IndexScanCommand for backward compatibility sake
    public IndexScanCommand toIndexScanCommand()
    {
        assert row_filter != null && !row_filter.isEmpty();
        if (maxIsColumns || isPaging)
            throw new IllegalStateException("Cannot proceed with range query as the remote end has a version < 1.1. Please update the full cluster first.");

        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace, column_family);
        try
        {
            if (!ThriftValidation.validateFilterClauses(cfm, row_filter))
                throw new IllegalStateException("Cannot proceed with non-indexed query as the remote end has a version < 1.1. Please update the full cluster first.");
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }

        RowPosition start = range.left;
        ByteBuffer startKey = ByteBufferUtil.EMPTY_BYTE_BUFFER;
        if (start instanceof DecoratedKey)
        {
            startKey = ((DecoratedKey)start).key;
        }

        IndexClause clause = new IndexClause(row_filter, startKey, maxResults);
        return new IndexScanCommand(keyspace, column_family, clause, predicate, range);
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
        throw new UnsupportedOperationException();
    }
}
