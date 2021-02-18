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

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.v1.BlockPackedReader;

/**
 * Abstraction over a long-indexed array of longs.
 */
public interface LongArray extends Closeable
{
    /**
     * Get value at {@code idx}.
     */
    long get(long idx);

    /**
     * Get array length.
     */
    long length();

    /**
     * Using the given token returns the first row ID corresponding to the token.
     * @param targetToken Token to lookup and it must not be smaller than previous value
     * @return The row ID of the given token or negative value if target token is greater than all tokens
     */
    long findTokenRowID(long targetToken);

    @Override
    default void close() throws IOException { }

    class DeferredLongArray implements LongArray
    {
        private Supplier<LongArray> supplier;
        private LongArray longArray;
        private boolean opened = false;

        public DeferredLongArray(Supplier<LongArray> supplier)
        {
            this.supplier = supplier;
        }

        @Override
        public long get(long idx)
        {
            open();
            return longArray.get(idx);
        }

        @Override
        public long length()
        {
            open();
            return longArray.length();
        }

        @Override
        public long findTokenRowID(long targetToken)
        {
            open();
            return longArray.findTokenRowID(targetToken);
        }

        @Override
        public void close() throws IOException
        {
            if (opened)
                longArray.close();
        }

        private void open()
        {
            if (!opened)
            {
                longArray = supplier.get();
                opened = true;
            }
        }
    }

    interface Factory
    {
        LongArray open();

        /**
         * TODO use a different interface for {@link BlockPackedReader}, as {@link LongArray#findTokenRowID(long)} is
         * not supported by other implementation.
         *
         * @param startingIndex minimum index to be used in {@link LongArray#findTokenRowID(long)}.
         *                      In {@link org.apache.cassandra.index.sai.disk.PostingListRangeIterator}, a segmentRowId
         *                      is provided and then in {@link OffsetFactory},
         *                      segment offset is applied to segmentRowId to create sstableRowId which will be used by
         *                      {@link BlockPackedReader#openTokenReader(long, SSTableQueryContext)}.
         * @param context shared between indexed columns for the same sstable in a given query
         * @return token BlockPackedReader
         */
        default LongArray openTokenReader(long startingIndex, SSTableQueryContext context)
        {
            return open();
        }

        default Factory withOffset(long idxOffset)
        {
            return new OffsetFactory(this, idxOffset);
        }
    }
}
