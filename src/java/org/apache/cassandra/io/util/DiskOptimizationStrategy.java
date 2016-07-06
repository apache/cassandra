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
            return 4096;

        size = (size + 4095) & ~4095;
        return (int)Math.min(size, MAX_BUFFER_SIZE);
    }
}
