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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.pager.Pageable;

public class RangeSliceCommand extends AbstractRangeCommand implements Pageable
{
    public static final RangeSliceCommandSerializer serializer = new RangeSliceCommandSerializer();

    public final int maxResults;
    public final boolean countCQL3Rows;
    public final boolean isPaging;

    public RangeSliceCommand(String keyspace,
                             String columnFamily,
                             long timestamp,
                             IDiskAtomFilter predicate,
                             AbstractBounds<RowPosition> range,
                             int maxResults)
    {
        this(keyspace, columnFamily, timestamp, predicate, range, null, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace,
                             String columnFamily,
                             long timestamp,
                             IDiskAtomFilter predicate,
                             AbstractBounds<RowPosition> range,
                             List<IndexExpression> row_filter,
                             int maxResults)
    {
        this(keyspace, columnFamily, timestamp, predicate, range, row_filter, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace,
                             String columnFamily,
                             long timestamp,
                             IDiskAtomFilter predicate,
                             AbstractBounds<RowPosition> range,
                             List<IndexExpression> rowFilter,
                             int maxResults,
                             boolean countCQL3Rows,
                             boolean isPaging)
    {
        super(keyspace, columnFamily, timestamp, range, predicate, rowFilter);
        this.maxResults = maxResults;
        this.countCQL3Rows = countCQL3Rows;
        this.isPaging = isPaging;
    }

    public MessageOut<RangeSliceCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.RANGE_SLICE, this, serializer);
    }

    public AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> subRange)
    {
        return new RangeSliceCommand(keyspace,
                                     columnFamily,
                                     timestamp,
                                     predicate,
                                     subRange,
                                     rowFilter,
                                     maxResults,
                                     countCQL3Rows,
                                     isPaging);
    }

    public AbstractRangeCommand withUpdatedLimit(int newLimit)
    {
        return new RangeSliceCommand(keyspace,
                                     columnFamily,
                                     timestamp,
                                     predicate,
                                     keyRange,
                                     rowFilter,
                                     newLimit,
                                     countCQL3Rows,
                                     isPaging);
    }

    public int limit()
    {
        return maxResults;
    }

    public boolean countCQL3Rows()
    {
        return countCQL3Rows;
    }

    public List<Row> executeLocally()
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        ExtendedFilter exFilter = cfs.makeExtendedFilter(keyRange, predicate, rowFilter, maxResults, countCQL3Rows, isPaging, timestamp);
        if (cfs.indexManager.hasIndexFor(rowFilter))
            return cfs.search(exFilter);
        else
            return cfs.getRangeSlice(exFilter);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("keyspace", keyspace)
                      .add("columnFamily", columnFamily)
                      .add("predicate", predicate)
                      .add("keyRange", keyRange)
                      .add("rowFilter", rowFilter)
                      .add("maxResults", maxResults)
                      .add("counterCQL3Rows", countCQL3Rows)
                      .add("timestamp", timestamp)
                      .toString();
    }
}

class RangeSliceCommandSerializer implements IVersionedSerializer<RangeSliceCommand>
{
    public void serialize(RangeSliceCommand sliceCommand, DataOutputPlus out, int version) throws IOException
    {
        out.writeUTF(sliceCommand.keyspace);
        out.writeUTF(sliceCommand.columnFamily);
        out.writeLong(sliceCommand.timestamp);

        CFMetaData metadata = Schema.instance.getCFMetaData(sliceCommand.keyspace, sliceCommand.columnFamily);

        metadata.comparator.diskAtomFilterSerializer().serialize(sliceCommand.predicate, out, version);

        if (sliceCommand.rowFilter == null)
        {
            out.writeInt(0);
        }
        else
        {
            out.writeInt(sliceCommand.rowFilter.size());
            for (IndexExpression expr : sliceCommand.rowFilter)
            {
                expr.writeTo(out);
            }
        }
        MessagingService.validatePartitioner(sliceCommand.keyRange);
        AbstractBounds.rowPositionSerializer.serialize(sliceCommand.keyRange, out, version);
        out.writeInt(sliceCommand.maxResults);
        out.writeBoolean(sliceCommand.countCQL3Rows);
        out.writeBoolean(sliceCommand.isPaging);
    }

    public RangeSliceCommand deserialize(DataInput in, int version) throws IOException
    {
        String keyspace = in.readUTF();
        String columnFamily = in.readUTF();
        long timestamp = in.readLong();

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);

        IDiskAtomFilter predicate = metadata.comparator.diskAtomFilterSerializer().deserialize(in, version);

        List<IndexExpression> rowFilter;
        int filterCount = in.readInt();
        rowFilter = new ArrayList<>(filterCount);
        for (int i = 0; i < filterCount; i++)
        {
            rowFilter.add(IndexExpression.readFrom(in));
        }
        AbstractBounds<RowPosition> range = AbstractBounds.rowPositionSerializer.deserialize(in, MessagingService.globalPartitioner(), version);

        int maxResults = in.readInt();
        boolean countCQL3Rows = in.readBoolean();
        boolean isPaging = in.readBoolean();
        return new RangeSliceCommand(keyspace, columnFamily, timestamp, predicate, range, rowFilter, maxResults, countCQL3Rows, isPaging);
    }

    public long serializedSize(RangeSliceCommand rsc, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(rsc.keyspace);
        size += TypeSizes.NATIVE.sizeof(rsc.columnFamily);
        size += TypeSizes.NATIVE.sizeof(rsc.timestamp);

        CFMetaData metadata = Schema.instance.getCFMetaData(rsc.keyspace, rsc.columnFamily);

        IDiskAtomFilter filter = rsc.predicate;

        size += metadata.comparator.diskAtomFilterSerializer().serializedSize(filter, version);

        if (rsc.rowFilter == null)
        {
            size += TypeSizes.NATIVE.sizeof(0);
        }
        else
        {
            size += TypeSizes.NATIVE.sizeof(rsc.rowFilter.size());
            for (IndexExpression expr : rsc.rowFilter)
            {
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
                size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            }
        }
        size += AbstractBounds.rowPositionSerializer.serializedSize(rsc.keyRange, version);
        size += TypeSizes.NATIVE.sizeof(rsc.maxResults);
        size += TypeSizes.NATIVE.sizeof(rsc.countCQL3Rows);
        size += TypeSizes.NATIVE.sizeof(rsc.isPaging);
        return size;
    }
}
