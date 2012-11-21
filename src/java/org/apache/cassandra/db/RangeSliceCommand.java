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
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.IReadCommand;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.cassandra.thrift.ThriftValidation;
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

    public final IDiskAtomFilter predicate;
    public final List<IndexExpression> row_filter;

    public final AbstractBounds<RowPosition> range;
    public final int maxResults;
    public final boolean countCQL3Rows;
    public final boolean isPaging;

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, int maxResults)
    {
        this(keyspace, column_family, super_column, predicate, range, null, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, ColumnParent column_parent, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults)
    {
        this(keyspace, column_parent.getColumn_family(), column_parent.super_column, predicate, range, row_filter, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults)
    {
        this(keyspace, column_family, super_column, predicate, range, row_filter, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, String column_family, ByteBuffer super_column, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults, boolean countCQL3Rows, boolean isPaging)
    {
        this.keyspace = keyspace;
        this.column_family = column_family;
        this.super_column = super_column;
        this.predicate = predicate;
        this.range = range;
        this.row_filter = row_filter;
        this.maxResults = maxResults;
        this.countCQL3Rows = countCQL3Rows;
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
               ", countCQL3Rows=" + countCQL3Rows +
               '}';
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    // Convert to a equivalent IndexScanCommand for backward compatibility sake
    public IndexScanCommand toIndexScanCommand()
    {
        assert row_filter != null && !row_filter.isEmpty();
        if (countCQL3Rows || isPaging)
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
        // IndexScanCommand is deprecated so don't bother
        SlicePredicate pred = RangeSliceCommandSerializer.asSlicePredicate(predicate);
        return new IndexScanCommand(keyspace, column_family, clause, pred, range);
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }
}

class RangeSliceCommandSerializer implements IVersionedSerializer<RangeSliceCommand>
{
    // For compatibility with pre-1.2 sake. We should remove at some point.
    public static SlicePredicate asSlicePredicate(IDiskAtomFilter predicate)
    {
        SlicePredicate sp = new SlicePredicate();
        if (predicate instanceof NamesQueryFilter)
        {
            sp.setColumn_names(new ArrayList<ByteBuffer>(((NamesQueryFilter)predicate).columns));
        }
        else
        {
            SliceQueryFilter sqf = (SliceQueryFilter)predicate;
            sp.setSlice_range(new SliceRange(sqf.start(), sqf.finish(), sqf.reversed, sqf.count));
        }
        return sp;
    }

    public void serialize(RangeSliceCommand sliceCommand, DataOutput dos, int version) throws IOException
    {
        dos.writeUTF(sliceCommand.keyspace);
        dos.writeUTF(sliceCommand.column_family);
        ByteBuffer sc = sliceCommand.super_column;
        dos.writeInt(sc == null ? 0 : sc.remaining());
        if (sc != null)
            ByteBufferUtil.write(sc, dos);

        if (version < MessagingService.VERSION_12)
        {
            FBUtilities.serialize(new TSerializer(new TBinaryProtocol.Factory()), asSlicePredicate(sliceCommand.predicate), dos);
        }
        else
        {
            IDiskAtomFilter.Serializer.instance.serialize(sliceCommand.predicate, dos, version);
        }

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
                {
                    if (version < MessagingService.VERSION_12)
                    {
                        FBUtilities.serialize(new TSerializer(new TBinaryProtocol.Factory()), expr, dos);
                    }
                    else
                    {
                        ByteBufferUtil.writeWithShortLength(expr.column_name, dos);
                        dos.writeInt(expr.op.getValue());
                        ByteBufferUtil.writeWithShortLength(expr.value, dos);
                    }
                }
            }
        }
        AbstractBounds.serializer.serialize(sliceCommand.range, dos, version);
        dos.writeInt(sliceCommand.maxResults);
        if (version >= MessagingService.VERSION_11)
        {
            dos.writeBoolean(sliceCommand.countCQL3Rows);
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

        IDiskAtomFilter predicate;
        AbstractType<?> comparator = ColumnFamily.getComparatorFor(keyspace, columnFamily, superColumn);
        if (version < MessagingService.VERSION_12)
        {
            SlicePredicate pred = new SlicePredicate();
            FBUtilities.deserialize(new TDeserializer(new TBinaryProtocol.Factory()), pred, dis);
            predicate = ThriftValidation.asIFilter(pred, comparator);
        }
        else
        {
            predicate = IDiskAtomFilter.Serializer.instance.deserialize(dis, version, comparator);
        }

        List<IndexExpression> rowFilter = null;
        if (version >= MessagingService.VERSION_11)
        {
            int filterCount = dis.readInt();
            rowFilter = new ArrayList<IndexExpression>(filterCount);
            for (int i = 0; i < filterCount; i++)
            {
                IndexExpression expr;
                if (version < MessagingService.VERSION_12)
                {
                    expr = new IndexExpression();
                    FBUtilities.deserialize(new TDeserializer(new TBinaryProtocol.Factory()), expr, dis);
                }
                else
                {
                    expr = new IndexExpression(ByteBufferUtil.readWithShortLength(dis),
                                               IndexOperator.findByValue(dis.readInt()),
                                               ByteBufferUtil.readWithShortLength(dis));
                }
                rowFilter.add(expr);
            }
        }
        AbstractBounds<RowPosition> range = AbstractBounds.serializer.deserialize(dis, version).toRowBounds();

        int maxResults = dis.readInt();
        boolean countCQL3Rows = false;
        boolean isPaging = false;
        if (version >= MessagingService.VERSION_11)
        {
            countCQL3Rows = dis.readBoolean();
            isPaging = dis.readBoolean();
        }
        return new RangeSliceCommand(keyspace, columnFamily, superColumn, predicate, range, rowFilter, maxResults, countCQL3Rows, isPaging);
    }

    public long serializedSize(RangeSliceCommand rsc, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(rsc.keyspace);
        size += TypeSizes.NATIVE.sizeof(rsc.column_family);

        ByteBuffer sc = rsc.super_column;
        if (sc != null)
        {
            size += TypeSizes.NATIVE.sizeof(sc.remaining());
            size += sc.remaining();
        }
        else
        {
            size += TypeSizes.NATIVE.sizeof(0);
        }

        if (version < MessagingService.VERSION_12)
        {
            TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
            try
            {
                int predicateLength = ser.serialize(asSlicePredicate(rsc.predicate)).length;
                if (version < MessagingService.VERSION_12)
                    size += TypeSizes.NATIVE.sizeof(predicateLength);
                size += predicateLength;
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            size += IDiskAtomFilter.Serializer.instance.serializedSize(rsc.predicate, version);
        }

        if (version >= MessagingService.VERSION_11)
        {
            if (rsc.row_filter == null)
            {
                size += TypeSizes.NATIVE.sizeof(0);
            }
            else
            {
                size += TypeSizes.NATIVE.sizeof(rsc.row_filter.size());
                for (IndexExpression expr : rsc.row_filter)
                {
                    if (version < MessagingService.VERSION_12)
                    {
                        try
                        {
                            int filterLength = new TSerializer(new TBinaryProtocol.Factory()).serialize(expr).length;
                            size += TypeSizes.NATIVE.sizeof(filterLength);
                            size += filterLength;
                        }
                        catch (TException e)
                        {
                            throw new RuntimeException(e);
                        }
                    }
                    else
                    {
                        size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column_name);
                        size += TypeSizes.NATIVE.sizeof(expr.op.getValue());
                        size += TypeSizes.NATIVE.sizeofWithLength(expr.value);
                    }
                }
            }
        }
        size += AbstractBounds.serializer.serializedSize(rsc.range, version);
        size += TypeSizes.NATIVE.sizeof(rsc.maxResults);
        if (version >= MessagingService.VERSION_11)
        {
            size += TypeSizes.NATIVE.sizeof(rsc.countCQL3Rows);
            size += TypeSizes.NATIVE.sizeof(rsc.isPaging);
        }
        return size;
    }
}
