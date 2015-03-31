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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PagedRangeCommand extends AbstractRangeCommand
{
    public static final IVersionedSerializer<PagedRangeCommand> serializer = new Serializer();

    public final Composite start;
    public final Composite stop;
    public final int limit;
    private final boolean countCQL3Rows;

    public PagedRangeCommand(String keyspace,
                             String columnFamily,
                             long timestamp,
                             AbstractBounds<RowPosition> keyRange,
                             SliceQueryFilter predicate,
                             Composite start,
                             Composite stop,
                             List<IndexExpression> rowFilter,
                             int limit,
                             boolean countCQL3Rows)
    {
        super(keyspace, columnFamily, timestamp, keyRange, predicate, rowFilter);
        this.start = start;
        this.stop = stop;
        this.limit = limit;
        this.countCQL3Rows = countCQL3Rows;
    }

    public MessageOut<PagedRangeCommand> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.PAGED_RANGE, this, serializer);
    }

    public AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> subRange)
    {
        Composite newStart = subRange.left.equals(keyRange.left) ? start : ((SliceQueryFilter)predicate).start();
        Composite newStop = subRange.right.equals(keyRange.right) ? stop : ((SliceQueryFilter)predicate).finish();
        return new PagedRangeCommand(keyspace,
                                     columnFamily,
                                     timestamp,
                                     subRange,
                                     (SliceQueryFilter)predicate,
                                     newStart,
                                     newStop,
                                     rowFilter,
                                     limit,
                                     countCQL3Rows);
    }

    public AbstractRangeCommand withUpdatedLimit(int newLimit)
    {
        return new PagedRangeCommand(keyspace,
                                     columnFamily,
                                     timestamp,
                                     keyRange,
                                     (SliceQueryFilter)predicate,
                                     start,
                                     stop,
                                     rowFilter,
                                     newLimit,
                                     countCQL3Rows);
    }

    public int limit()
    {
        return limit;
    }

    public boolean countCQL3Rows()
    {
        return countCQL3Rows;
    }

    public List<Row> executeLocally()
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        ExtendedFilter exFilter = cfs.makeExtendedFilter(keyRange, (SliceQueryFilter)predicate, start, stop, rowFilter, limit, countCQL3Rows(), timestamp);
        if (cfs.indexManager.hasIndexFor(rowFilter))
            return cfs.search(exFilter);
        else
            return cfs.getRangeSlice(exFilter);
    }

    @Override
    public String toString()
    {
        return String.format("PagedRange(%s, %s, %d, %s, %s, %s, %s, %s, %d)", keyspace, columnFamily, timestamp, keyRange, predicate, start, stop, rowFilter, limit);
    }

    private static class Serializer implements IVersionedSerializer<PagedRangeCommand>
    {
        public void serialize(PagedRangeCommand cmd, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(cmd.keyspace);
            out.writeUTF(cmd.columnFamily);
            out.writeLong(cmd.timestamp);

            MessagingService.validatePartitioner(cmd.keyRange);
            AbstractBounds.rowPositionSerializer.serialize(cmd.keyRange, out, version);

            CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);

            // SliceQueryFilter (the count is not used)
            SliceQueryFilter filter = (SliceQueryFilter)cmd.predicate;
            metadata.comparator.sliceQueryFilterSerializer().serialize(filter, out, version);

            // The start and stop of the page
            metadata.comparator.serializer().serialize(cmd.start, out);
            metadata.comparator.serializer().serialize(cmd.stop, out);

            out.writeInt(cmd.rowFilter.size());
            for (IndexExpression expr : cmd.rowFilter)
            {
                expr.writeTo(out);;
            }

            out.writeInt(cmd.limit);
            if (version >= MessagingService.VERSION_21)
                out.writeBoolean(cmd.countCQL3Rows);
        }

        public PagedRangeCommand deserialize(DataInput in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            long timestamp = in.readLong();

            AbstractBounds<RowPosition> keyRange =
                    AbstractBounds.rowPositionSerializer.deserialize(in, MessagingService.globalPartitioner(), version);

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);

            SliceQueryFilter predicate = metadata.comparator.sliceQueryFilterSerializer().deserialize(in, version);

            Composite start = metadata.comparator.serializer().deserialize(in);
            Composite stop =  metadata.comparator.serializer().deserialize(in);

            int filterCount = in.readInt();
            List<IndexExpression> rowFilter = new ArrayList<IndexExpression>(filterCount);
            for (int i = 0; i < filterCount; i++)
            {
                rowFilter.add(IndexExpression.readFrom(in));
            }

            int limit = in.readInt();
            boolean countCQL3Rows = version >= MessagingService.VERSION_21
                                  ? in.readBoolean()
                                  : predicate.compositesToGroup >= 0 || predicate.count != 1; // See #6857
            return new PagedRangeCommand(keyspace, columnFamily, timestamp, keyRange, predicate, start, stop, rowFilter, limit, countCQL3Rows);
        }

        public long serializedSize(PagedRangeCommand cmd, int version)
        {
            long size = 0;

            size += TypeSizes.NATIVE.sizeof(cmd.keyspace);
            size += TypeSizes.NATIVE.sizeof(cmd.columnFamily);
            size += TypeSizes.NATIVE.sizeof(cmd.timestamp);

            size += AbstractBounds.rowPositionSerializer.serializedSize(cmd.keyRange, version);

            CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);

            size += metadata.comparator.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter)cmd.predicate, version);

            size += metadata.comparator.serializer().serializedSize(cmd.start, TypeSizes.NATIVE);
            size += metadata.comparator.serializer().serializedSize(cmd.stop, TypeSizes.NATIVE);

            size += TypeSizes.NATIVE.sizeof(cmd.rowFilter.size());
            for (IndexExpression expr : cmd.rowFilter)
            {
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
                size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            }

            size += TypeSizes.NATIVE.sizeof(cmd.limit);
            if (version >= MessagingService.VERSION_21)
                size += TypeSizes.NATIVE.sizeof(cmd.countCQL3Rows);
            return size;
        }
    }
}
