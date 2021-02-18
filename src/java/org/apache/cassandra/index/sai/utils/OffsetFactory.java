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

import org.apache.cassandra.index.sai.SSTableQueryContext;

public class OffsetFactory implements LongArray.Factory
{
    private final LongArray.Factory wrapped;
    private final long idxOffset;

    OffsetFactory(LongArray.Factory wrapped, long idxOffset)
    {
        this.wrapped = wrapped;
        this.idxOffset = idxOffset;
    }

    @Override
    public LongArray open()
    {
        return new OffsetLongArray(wrapped.open(), idxOffset);
    }

    @Override
    @SuppressWarnings("resource")
    public LongArray openTokenReader(long segmentRowId, SSTableQueryContext context)
    {
        // apply segment offset so that `LongArray.findTokenRowId` will start searching tokens from current segment.
        LongArray reader = wrapped.openTokenReader(segmentRowId + idxOffset, context);
        return new TokenLongArray(context, reader, idxOffset);
    }

    private static class OffsetLongArray implements LongArray
    {
        private final LongArray wrapped;
        protected final long idxOffset;

        OffsetLongArray(LongArray wrapped, long idxOffset)
        {
            this.wrapped = wrapped;
            this.idxOffset = idxOffset;
        }

        /**
         * Get value at {@code idx}.
         */
        @Override
        public long get(long idx)
        {
            return wrapped.get(toSSTableRowId(idx));
        }

        @Override
        public long findTokenRowID(long value)
        {
            // Subtract the segment offset from the global row ID to provide a segment row ID for the caller:
            return toSegmentRowId(wrapped.findTokenRowID(value));
        }

        /**
         * Get array length.
         */
        public long length()
        {
            return wrapped.length();
        }

        @Override
        public void close() throws IOException
        {
            wrapped.close();
        }

        protected long toSSTableRowId(long segmentRowId)
        {
            return segmentRowId + idxOffset;
        }

        protected long toSegmentRowId(long sstableRowId)
        {
            return sstableRowId - idxOffset;
        }
    }

    /**
     * Cache the prev token value and prev sstable row id pair, and share it between different indexed columns in the
     * same query.
     */
    static class TokenLongArray extends OffsetLongArray
    {
        private final SSTableQueryContext context;

        TokenLongArray(SSTableQueryContext context, LongArray wrapped, long idxOffset)
        {
            super(wrapped, idxOffset);
            this.context = context;
        }

        @Override
        public long get(long segmentRowId)
        {
            long sstableRowId = toSSTableRowId(segmentRowId);
            if (sstableRowId == context.prevSSTableRowId)
            {
                return context.prevTokenValue;
            }

            long tokenValue = super.get(segmentRowId);

            // during intersection, the next pair of token and rowId from current indexed column iterator is very likely
            // to be used to skip another indexed column iterator (aka. used to call findTokenRowId) or used to fetch
            // token (aka. get) if there is matching row id from posting reader.
            context.prevTokenValue = tokenValue;
            context.prevSSTableRowId = sstableRowId;

            return tokenValue;
        }

        @Override
        public long findTokenRowID(long tokenValue)
        {
            long segmentRowId = toSegmentRowId(context.prevSkipToSSTableRowId);

            // Don't use cached value from previous segment when there is duplicated tokens across segments.
            if (tokenValue == context.prevSkipToTokenValue && segmentRowId >= 0)
            {
                context.markTokenSkippingCacheHit();
            }
            else
            {
                segmentRowId = super.findTokenRowID(tokenValue);

                context.prevSkipToTokenValue = tokenValue;
                context.prevSkipToSSTableRowId = toSSTableRowId(segmentRowId);
            }
            context.markTokenSkippingLookup();
            return segmentRowId;
        }
    }
}
