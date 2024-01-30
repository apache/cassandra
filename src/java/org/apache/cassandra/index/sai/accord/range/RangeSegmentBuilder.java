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

package org.apache.cassandra.index.sai.accord.range;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import accord.primitives.Route;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.service.accord.AccordKeyspace;

public class RangeSegmentBuilder extends SegmentBuilder
{
    private final RangeMemoryIndex memory;

    protected RangeSegmentBuilder(StorageAttachedIndex index, NamedMemoryLimiter limiter)
    {
        super(index.termType(), limiter);
        this.memory = new RangeMemoryIndex(index);
    }

    @Override
    public boolean isEmpty()
    {
        return memory.isEmpty();
    }

    @Override
    protected long addInternal(ByteBuffer term, int segmentRowId)
    {
        throw new UnsupportedOperationException();
    }

    private RowMapping rowMapping = RowMapping.create(OperationType.FLUSH);

    @Override
    public long add(ByteBuffer term, PrimaryKey key, long sstableRowId)
    {
        assert !flushed : "Cannot add to a flushed segment.";
        assert sstableRowId >= maxSSTableRowId;
        minSSTableRowId = minSSTableRowId < 0 ? sstableRowId : minSSTableRowId;
        maxSSTableRowId = sstableRowId;

        assert maxKey == null || maxKey.compareTo(key) <= 0;
        if (minKey == null)
            minKey = key;
        maxKey = key;

        if (rowCount == 0)
        {
            // use first global rowId in the segment as segment rowId offset
            segmentRowIdOffset = sstableRowId;
        }

        rowCount++;

        // segmentRowIdOffset should encode sstableRowId into Integer
        int segmentRowId = castToSegmentRowId(sstableRowId, segmentRowIdOffset);
        maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

        try
        {
            rowMapping.add(key, sstableRowId);
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            throw new RuntimeException();
        }

        Route<?> route;
        try
        {
            route = AccordKeyspace.deserializeRouteOrNull(term);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        long bytesAllocated = memory.add(key, route);
        totalBytesAllocated += bytesAllocated;
        return bytesAllocated;
    }

    @Override
    protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier) throws IOException
    {
        minTerm = memory.getMinTerm();
        maxTerm = memory.getMaxTerm();
        return memory.writeDirect(indexDescriptor, indexIdentifier, rowMapping::get);
    }
}
