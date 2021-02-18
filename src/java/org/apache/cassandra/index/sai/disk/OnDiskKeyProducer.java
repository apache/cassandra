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
package org.apache.cassandra.index.sai.disk;

import java.util.Collections;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.io.util.RandomAccessReader;

/**
 * A reusable {@link Token} that lazily materializes an iterator of {@link DecoratedKey} from disk.
 */
@NotThreadSafe
public class OnDiskKeyProducer
{
    public static final long NO_OFFSET = -1;

    private final SSTableContext.KeyFetcher keyFetcher;
    private final RandomAccessReader reader;
    private final LongArray segmentRowIdToOffset;

    private final long maxPartitionOffset;

    private long lastOffset = NO_OFFSET;

    public OnDiskKeyProducer(SSTableContext.KeyFetcher keyFetcher, RandomAccessReader reader, LongArray segmentRowIdToOffset, long maxPartitionOffset)
    {
        this.keyFetcher = keyFetcher;
        this.reader = reader;
        this.segmentRowIdToOffset = segmentRowIdToOffset;
        this.maxPartitionOffset = maxPartitionOffset;
    }

    public Token produceToken(long token, long segmentRowId)
    {
        return new OnDiskToken(token, segmentRowId);
    }

    /**
     * Used to remove duplicated key offset due rows sharing the same key offset in wide partition schema.
     */
    private long getKeyOffset(long segmentRowId)
    {
        long offset = segmentRowIdToOffset.get(segmentRowId);

        if (offset == lastOffset)
        {
            return NO_OFFSET;
        }

        // Due to ZCS, index files may still contain partition offsets that are not part of partial SSTable.
        if (offset > maxPartitionOffset)
        {
            return NO_OFFSET;
        }

        // Catalog the last offset if it's valid:
        lastOffset = offset;

        return offset;
    }

    public class OnDiskToken extends Token
    {
        private final long segmentRowId;

        public OnDiskToken(long token, long segmentRowId)
        {
            super(token);
            this.segmentRowId = segmentRowId;
        }

        @Override
        public Iterator<DecoratedKey> keys()
        {
            long keyOffset = getKeyOffset(segmentRowId);
            DecoratedKey key = keyFetcher.apply(reader, keyOffset);
            return key == null ? Collections.emptyIterator() : Iterators.singletonIterator(key);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this).add("token", token).add("lastOffset", lastOffset).toString();
        }
    }
}
