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

package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator that consumes a chunk of {@link PrimaryKey}s from the {@link RangeIterator}, passes them to the
 * {@link Function} to filter the chunk of {@link PrimaryKey}s and then pass the results to next consumer.
 * The PKs are currently returned in {@link PrimaryKey} order, but that contract may change.
 */
@NotThreadSafe
public class OrderingFilterRangeIterator extends RangeIterator
{
    private final RangeIterator input;
    private final int chunkSize;
    private final Function<List<PrimaryKey>, RangeIterator> nextRangeFunction;
    private RangeIterator nextIterator;
    private ArrayList<PrimaryKey> nextKeys;

    public OrderingFilterRangeIterator(RangeIterator input, int chunkSize, Function<List<PrimaryKey>, RangeIterator> nextRangeFunction)
    {
        super(input);
        this.input = input;
        this.chunkSize = chunkSize;
        this.nextRangeFunction = nextRangeFunction;
        this.nextKeys = new ArrayList<>(chunkSize);
    }

    @Override
    public PrimaryKey computeNext()
    {
        if (nextIterator == null || !nextIterator.hasNext())
        {
            do
            {
                if (!input.hasNext())
                    return endOfData();
                nextKeys.clear();
                do
                {
                    nextKeys.add(input.next());
                }
                while (nextKeys.size() < chunkSize && input.hasNext());
                // Get the next iterator before closing this one to prevent releasing the resource.
                var previousIterator = nextIterator;
                // If this results in an exception, previousIterator is closed in close() method.
                nextIterator = nextRangeFunction.apply(nextKeys);
                if (previousIterator != null)
                    FileUtils.closeQuietly(previousIterator);
                // nextIterator might not have any rows due to shadowed primary keys
            } while (!nextIterator.hasNext());
        }
        return nextIterator.next();
    }

    @Override
    protected void performSkipTo(PrimaryKey nextToken)
    {
        input.skipTo(nextToken);
        // VSTODO is it valid to skipTo() on this iterator after nextIterator has been initialized? It seems like it
        // could result in missing a relevant local maxima within nextIterator because the iterator becomes
        // top k minus n where n is the number of skipped PKs. I'm not sure that this method is called when
        // nextIterator != null.
        if (nextIterator != null)
            nextIterator.skipTo(nextToken);
    }

    public void close() {
        FileUtils.closeQuietly(input);
        FileUtils.closeQuietly(nextIterator);
    }
}
