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

package org.apache.cassandra.index.sai.memory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Function;

public abstract class MemoryIndex implements MemtableOrdering
{
    protected final StorageAttachedIndex index;

    protected MemoryIndex(StorageAttachedIndex index)
    {
        this.index = index;
    }

    public abstract long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value);

    public abstract long update(DecoratedKey key, Clustering<?> clustering, ByteBuffer oldValue, ByteBuffer newValue);

    public abstract KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange);

    public abstract boolean isEmpty();

    public abstract ByteBuffer getMinTerm();

    public abstract ByteBuffer getMaxTerm();

    /**
     * Iterate all Term->PrimaryKeys mappings in sorted order
     */
    public abstract Iterator<Pair<ByteComparable, PrimaryKeys>> iterator();

    public abstract SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor,
                                                                     IndexIdentifier indexIdentifier,
                                                                     Function<PrimaryKey, Integer> postingTransformer) throws IOException;
}
