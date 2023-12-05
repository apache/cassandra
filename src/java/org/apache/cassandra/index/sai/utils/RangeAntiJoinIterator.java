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

import java.io.IOException;

import org.apache.cassandra.io.util.FileUtils;

/**
 * An iterator wrapper that wraps two iterators (left and right) and returns the primary keys from the left iterator
 * that do not match the primary keys from the right iterator. The keys returned by the wrapped iterators must
 * follow token-clustering order.
 */
public class RangeAntiJoinIterator extends RangeIterator
{
    final RangeIterator left;
    final RangeIterator right;

    private PrimaryKey nextKeyToSkip = null;

    private RangeAntiJoinIterator(RangeIterator left, RangeIterator right)
    {
        super(left.getMinimum(), left.getMaximum(), left.getMaxKeys());
        this.left = left;
        this.right = right;
    }

    public static RangeAntiJoinIterator create(RangeIterator left, RangeIterator right)
    {
        return new RangeAntiJoinIterator(left, right);
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        left.skipTo(nextKey);
        right.skipTo(nextKey);
    }

    public void close() throws IOException
    {
        FileUtils.close(left, right);
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
            }
            else
            {
                right.skipTo(key);
            }
            nextKeyToSkip = right.nextOrNull();
            cmp = compare(key, nextKeyToSkip);
        }

        return key != null ? key : endOfData();
    }

    private int compare(PrimaryKey key1, PrimaryKey key2)
    {
        return (key1 == null || key2 == null) ? -1 : key1.compareTo(key2);
    }
}
