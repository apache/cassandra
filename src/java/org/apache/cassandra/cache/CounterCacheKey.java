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
package org.apache.cassandra.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.*;

public final class CounterCacheKey extends CacheKey
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new CounterCacheKey(null, ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBuffer.allocate(1)));

    private final byte[] partitionKey;
    private final byte[] cellName;

    private CounterCacheKey(Pair<String, String> ksAndCFName, byte[] partitionKey, byte[] cellName)
    {
        super(ksAndCFName);
        this.partitionKey = partitionKey;
        this.cellName = cellName;
    }

    private CounterCacheKey(Pair<String, String> ksAndCFName, ByteBuffer partitionKey, ByteBuffer cellName)
    {
        this(ksAndCFName, ByteBufferUtil.getArray(partitionKey), ByteBufferUtil.getArray(cellName));
    }

    public static CounterCacheKey create(Pair<String, String> ksAndCFName, ByteBuffer partitionKey, Clustering clustering, ColumnDefinition c, CellPath path)
    {
        return new CounterCacheKey(ksAndCFName, partitionKey, makeCellName(clustering, c, path));
    }

    private static ByteBuffer makeCellName(Clustering clustering, ColumnDefinition c, CellPath path)
    {
        int cs = clustering.size();
        ByteBuffer[] values = new ByteBuffer[cs + 1 + (path == null ? 0 : path.size())];
        for (int i = 0; i < cs; i++)
            values[i] = clustering.get(i);
        values[cs] = c.name.bytes;
        if (path != null)
            for (int i = 0; i < path.size(); i++)
                values[cs + 1 + i] = path.get(i);
        return CompositeType.build(values);
    }

    public ByteBuffer partitionKey()
    {
        return ByteBuffer.wrap(partitionKey);
    }

    /**
     * Reads the value of the counter represented by this key.
     *
     * @param cfs the store for the table this is a key of.
     * @return the value for the counter represented by this key, or {@code null} if there
     * is not such counter.
     */
    public ByteBuffer readCounterValue(ColumnFamilyStore cfs)
    {
        CFMetaData metadata = cfs.metadata;
        assert metadata.ksAndCFName.equals(ksAndCFName);

        DecoratedKey key = cfs.decorateKey(partitionKey());

        int clusteringSize = metadata.comparator.size();
        List<ByteBuffer> buffers = CompositeType.splitName(ByteBuffer.wrap(cellName));
        assert buffers.size() >= clusteringSize + 1; // See makeCellName above

        Clustering clustering = Clustering.make(buffers.subList(0, clusteringSize).toArray(new ByteBuffer[clusteringSize]));
        ColumnDefinition column = metadata.getColumnDefinition(buffers.get(clusteringSize));
        // This can theoretically happen if a column is dropped after the cache is saved and we
        // try to load it. Not point if failing in any case, just skip the value.
        if (column == null)
            return null;

        CellPath path = column.isComplex() ? CellPath.create(buffers.get(buffers.size() - 1)) : null;

        int nowInSec = FBUtilities.nowInSeconds();
        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        if (path == null)
            builder.add(column);
        else
            builder.select(column, path);

        ClusteringIndexFilter filter = new ClusteringIndexNamesFilter(FBUtilities.singleton(clustering, metadata.comparator), false);
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(metadata, nowInSec, key, builder.build(), filter);
        try (ReadExecutionController controller = cmd.executionController();
             RowIterator iter = UnfilteredRowIterators.filter(cmd.queryMemtableAndDisk(cfs, controller), nowInSec))
        {
            ByteBuffer value = null;
            if (column.isStatic())
                value = iter.staticRow().getCell(column).value();
            else if (iter.hasNext())
                value = iter.next().getCell(column).value();

            return value;
        }
    }

    public void write(DataOutputPlus out)
    throws IOException
    {
        ByteBufferUtil.writeWithLength(partitionKey, out);
        ByteBufferUtil.writeWithLength(cellName, out);
    }

    public static CounterCacheKey read(Pair<String, String> ksAndCFName, DataInputPlus in)
    throws IOException
    {
        return new CounterCacheKey(ksAndCFName,
                                   ByteBufferUtil.readBytesWithLength(in),
                                   ByteBufferUtil.readBytesWithLength(in));
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
               + ObjectSizes.sizeOfArray(partitionKey)
               + ObjectSizes.sizeOfArray(cellName);
    }

    @Override
    public String toString()
    {
        return String.format("CounterCacheKey(%s, %s, %s)",
                             ksAndCFName,
                             ByteBufferUtil.bytesToHex(ByteBuffer.wrap(partitionKey)),
                             ByteBufferUtil.bytesToHex(ByteBuffer.wrap(cellName)));
    }

    @Override
    public int hashCode()
    {
        return Arrays.deepHashCode(new Object[]{ksAndCFName, partitionKey, cellName});
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CounterCacheKey))
            return false;

        CounterCacheKey cck = (CounterCacheKey) o;

        return ksAndCFName.equals(cck.ksAndCFName)
            && Arrays.equals(partitionKey, cck.partitionKey)
            && Arrays.equals(cellName, cck.cellName);
    }
}
