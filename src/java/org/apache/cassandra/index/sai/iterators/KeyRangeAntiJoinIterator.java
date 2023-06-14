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

import java.io.IOException;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator wrapper that wraps two iterators (left and right) and returns the primary keys from the left iterator
 * that do not match the primary keys from the right iterator. The keys returned by the wrapped iterators must
 * follow token-clustering order.
 */
public class KeyRangeAntiJoinIterator extends KeyRangeIterator
{
    final KeyRangeIterator left;
    final KeyRangeIterator right;

    private PrimaryKey nextKeyToSkip = null;

    private KeyRangeAntiJoinIterator(KeyRangeIterator.Builder.Statistics statistics, KeyRangeIterator left, KeyRangeIterator right)
    {
        super(statistics);
        this.left = left;
        this.right = right;
    }

    public static KeyRangeAntiJoinIterator create(KeyRangeIterator left, KeyRangeIterator right)
    {
        AntiJoinStatistics statistics = new AntiJoinStatistics();
        statistics.update(left);
        return new KeyRangeAntiJoinIterator(statistics, left, right);
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        left.performSkipTo(nextKey);
        right.performSkipTo(nextKey);
    }

    public void close() throws IOException
    {
        FileUtils.closeQuietly(left);
        FileUtils.closeQuietly(right);
    }


    protected PrimaryKey computeNext()
    {
        if (nextKeyToSkip == null)
            nextKeyToSkip = right.nextOrNull();

        PrimaryKey key = left.nextOrNull();
        int cmp = compare(key, nextKeyToSkip);

        while (key != null && cmp >= 0)
        {
            if (cmp == 0)
            {
                key = left.nextOrNull();
                nextKeyToSkip = right.nextOrNull();
            }
            else
            {
                nextKeyToSkip = right.skipTo(key);
            }
            cmp = compare(key, nextKeyToSkip);
        }

        return key != null ? key : endOfData();
    }

    private int compare(PrimaryKey key1, PrimaryKey key2)
    {
        return (key1 == null || key2 == null) ? -1 : key1.compareTo(key2);
    }

    private static class AntiJoinStatistics extends KeyRangeIterator.Builder.Statistics
    {
        @Override
        public void update(KeyRangeIterator range)
        {
            min = nullSafeMax(min, range.getMinimum());
            max = nullSafeMin(max, range.getMaximum());
            count += range.getCount();
        }
    }


}
