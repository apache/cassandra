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

package org.apache.cassandra.index.sai.iterators;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator that consumes a chunk of {@link PrimaryKey}s from the {@link KeyRangeIterator}, passes them to the
 * {@link Function} to filter the chunk of {@link PrimaryKey}s and then pass the results to next consumer.
 * The PKs are currently returned in {@link PrimaryKey} order, but that contract may change.
 */
@NotThreadSafe
public class KeyRangeOrderingIterator extends KeyRangeIterator
{
    private final KeyRangeIterator input;
    private final int chunkSize;
    private final Function<List<PrimaryKey>, KeyRangeIterator> nextRangeFunction;
    private final ArrayList<PrimaryKey> nextKeys;
    private KeyRangeIterator nextIterator;

    public KeyRangeOrderingIterator(KeyRangeIterator input, int chunkSize, Function<List<PrimaryKey>, KeyRangeIterator> nextRangeFunction)
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
            }
            while (!nextIterator.hasNext());
        }
        return nextIterator.next();
    }

    @Override
    protected void performSkipTo(PrimaryKey nextToken)
    {
        input.skipTo(nextToken);
        nextIterator.skipTo(nextToken);
    }

    public void close()
    {
        FileUtils.closeQuietly(input);
        FileUtils.closeQuietly(nextIterator);
    }
}
