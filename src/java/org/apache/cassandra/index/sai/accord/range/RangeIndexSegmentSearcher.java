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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.accord.SaiSerializer;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.PerColumnIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.segment.IndexSegmentSearcher;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.InMemoryKeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

public class RangeIndexSegmentSearcher extends IndexSegmentSearcher
{
    private static final IPartitioner partitioner = AccordKeyspace.Commands.partitioner;

    private final Map<Group, CheckpointIntervalArrayIndex.SegmentSearcher> map;

    public RangeIndexSegmentSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                                     PerColumnIndexFiles perIndexFiles,
                                     SegmentMetadata segmentMetadata,
                                     StorageAttachedIndex index) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, index);
        this.map = GroupToCheckpointIntervals.read(perIndexFiles, segmentMetadata);
    }

    @Override
    public long indexFileCacheSize()
    {
        return 0;
    }

    @Override
    public KeyRangeIterator search(Expression expression,
                                   AbstractBounds<PartitionPosition> keyRange,
                                   QueryContext context) throws IOException
    {
        assert expression.getIndexOperator() == Expression.IndexOperator.RANGE : String.format("Unsupported operator %s", expression.getIndexOperator());
        Optional<RowFilter.Expression> maybeStore = context.readCommand.rowFilter().getExpressions().stream().filter(e -> e.column().name.toString().equals("store_id") && e.operator() == Operator.EQ).findFirst();
        if (maybeStore.isEmpty())
            throw new AssertionError("store_id is not present in the expression: " + expression);
        int storeId = Int32Type.instance.compose(maybeStore.get().getIndexValue());

        AccordRoutingKey start = SaiSerializer.deserializeRoutingKey(expression.lower.value.raw);
        AccordRoutingKey end = SaiSerializer.deserializeRoutingKey(expression.upper.value.raw);
        var reader = map.get(new Group(storeId, start.table()));
        if (reader == null)
            return KeyRangeIterator.empty();
        TreeSet<PrimaryKey> pks = new TreeSet<>();
        reader.intersects(SaiSerializer.unwrap(start), SaiSerializer.unwrap(end), it ->
                                                                                  pks.add(index.keyFactory().create(partitioner.decorateKey(ByteBuffer.wrap(it.value)))));

        if (pks.isEmpty())
            return KeyRangeIterator.empty();
        return new InMemoryKeyRangeIterator(pks);
    }

    @Override
    public void close() throws IOException
    {
        map.values().forEach(r -> r.close());
        map.clear();
    }
}
