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

package org.apache.cassandra.utils.memory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class BufferPools
{
    private static final Logger logger = LoggerFactory.getLogger(BufferPools.class);

    /**
     * Used by chunk cache to store decompressed data and buffers may be held by chunk cache for arbitrary period.
     */
    private static final long FILE_MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMiB() * 1024L * 1024L;
    private static final BufferPool CHUNK_CACHE_POOL = new BufferPool("chunk-cache", FILE_MEMORY_USAGE_THRESHOLD, true);

    /**
     * Used by client-server or inter-node requests, buffers should be released immediately after use.
     */
    private static final long NETWORKING_MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getNetworkingCacheSizeInMiB() * 1024L * 1024L;
    private static final BufferPool NETWORKING_POOL = new BufferPool("networking", NETWORKING_MEMORY_USAGE_THRESHOLD, false);

    static
    {
        logger.info("Global buffer pool limit is {} for {} and {} for {}",
                    prettyPrintMemory(FILE_MEMORY_USAGE_THRESHOLD),
                    CHUNK_CACHE_POOL.name,
                    prettyPrintMemory(NETWORKING_MEMORY_USAGE_THRESHOLD),
                    NETWORKING_POOL.name);

    }
    /**
     * Long-lived buffers used for chunk cache and other disk access
     */
    public static BufferPool forChunkCache()
    {
        return CHUNK_CACHE_POOL;
    }

    /**
     * Short-lived buffers used for internode messaging or client-server connections.
     */
    public static BufferPool forNetworking()
    {
        return NETWORKING_POOL;
    }

    public static void shutdownLocalCleaner(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException
    {
        CHUNK_CACHE_POOL.shutdownLocalCleaner(timeout, unit);
        NETWORKING_POOL.shutdownLocalCleaner(timeout, unit);
    }

}
