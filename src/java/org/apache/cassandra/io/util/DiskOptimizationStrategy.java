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

package org.apache.cassandra.io.util;

public interface DiskOptimizationStrategy
{
    int MIN_BUFFER_SIZE = 1 << 12; // 4096, the typical size of a page in the OS cache
    int MIN_BUFFER_SIZE_MASK = MIN_BUFFER_SIZE - 1;

    // The maximum buffer size, we will never buffer more than this size. Further,
    // when the limiter is not null, i.e. when throttling is enabled, we read exactly
    // this size, since when throttling the intention is to eventually read everything,
    // see CASSANDRA-8630
    // NOTE: this size is chosen both for historical consistency, as a reasonable upper bound,
    //       and because our BufferPool currently has a maximum allocation size of this.
    int MAX_BUFFER_SIZE = 1 << 16; // 64k

    /**
     * @param recordSize record size
     * @return the buffer size for a given record size.
     */
    int bufferSize(long recordSize);

    /**
     * Round up to the next multiple of 4k but no more than {@link #MAX_BUFFER_SIZE}.
     */
    default int roundBufferSize(long size)
    {
        if (size <= 0)
            return MIN_BUFFER_SIZE;

        size = (size + MIN_BUFFER_SIZE_MASK) & ~MIN_BUFFER_SIZE_MASK;
        return (int)Math.min(size, MAX_BUFFER_SIZE);
    }

    /**
     * Round either up or down to the next power of two, which is required by the
     * {@link org.apache.cassandra.cache.ChunkCache.CachingRebufferer}, but capping between {@link #MIN_BUFFER_SIZE}
     * and {@link #MAX_BUFFER_SIZE}.
     *
     * @param size - the size to round to a power of two, normally this is a buffer size that was previously
     *             returned by a {@link #bufferSize(long)}.
     * @param roundUp - whether to round up or down
     *
     * @return a value rounded to a power of two but never bigger than {@link #MAX_BUFFER_SIZE} or smaller than {@link #MIN_BUFFER_SIZE}.
     */
    static int roundForCaching(int size, boolean roundUp)
    {
        if (size <= MIN_BUFFER_SIZE)
            return MIN_BUFFER_SIZE;

        int ret = roundUp
                  ? 1 << (32 - Integer.numberOfLeadingZeros(size - 1))
                  : Integer.highestOneBit(size);

        return Math.min(MAX_BUFFER_SIZE, ret);
    }
}
