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
package org.apache.cassandra.index.sai.disk.v1.vector;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

public class CheckpointingIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(CheckpointingIterator.class);

    private final QueryContext context;
    private final KeyRangeIterator union;
    private final Iterable<SSTableIndex> referencedIndexes;

    public CheckpointingIterator(KeyRangeIterator wrapped, Iterable<SSTableIndex> referencedIndexes, Iterable<SSTableIndex> referencedAnnIndexesInHybridSearch, QueryContext queryContext)
    {
        super(wrapped.getMinimum(), wrapped.getMaximum(), wrapped.getCount());

        this.union = wrapped;
        if (referencedAnnIndexesInHybridSearch != null)
            this.referencedIndexes = Iterables.concat(referencedIndexes, referencedAnnIndexesInHybridSearch);
        else
            this.referencedIndexes = referencedIndexes;
        this.context = queryContext;
    }

    protected PrimaryKey computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : endOfData();
        }
        finally
        {
            context.checkpoint();
        }
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        try
        {
            union.skipTo(nextKey);
        }
        finally
        {
            context.checkpoint();
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(union);
        referencedIndexes.forEach(CheckpointingIterator::releaseQuietly);
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(index.getIndexContext().logMessage(String.format("Failed to release index on SSTable %s", index.getSSTable())), e);
        }
    }
}
