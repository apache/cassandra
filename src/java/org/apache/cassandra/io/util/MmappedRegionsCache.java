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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.compress.CompressionMetadata;

/**
 * It is a utility class for caching {@link MmappedRegions} primarily used by a {@link FileHandle.Builder} when a handle
 * to the same file is created multiple times (as when an sstable is opened early).
 */
@NotThreadSafe
public class MmappedRegionsCache implements AutoCloseable
{
    private final Map<File, MmappedRegions> cache = new HashMap<>();
    private boolean closed = false;

    /**
     * Looks for mmapped regions in cache. If found, a shared copy is created and extended to the provided length.
     * If mmapped regions do not exist yet for the provided key, they are created and a shared copy is returned.
     *
     * @param channel channel for which the mmapped regions are requested
     * @param length  length of the file
     * @return a shared copy of the cached mmapped regions
     */
    public MmappedRegions getOrCreate(ChannelProxy channel, long length)
    {
        Preconditions.checkState(!closed);
        MmappedRegions regions = cache.computeIfAbsent(channel.file(), ignored -> MmappedRegions.map(channel, length));
        Preconditions.checkArgument(regions.isValid(channel));
        regions.extend(length);
        return regions.sharedCopy();
    }

    /**
     * Looks for mmapped regions in cache. If found, a shared copy is created and extended according to the provided metadata.
     * If mmapped regions do not exist yet for the provided key, they are created and a shared copy is returned.
     *
     * @param channel channel for which the mmapped regions are requested
     * @param metadata compression metadata of the file
     * @return a shared copy of the cached mmapped regions
     */
    public MmappedRegions getOrCreate(ChannelProxy channel, CompressionMetadata metadata)
    {
        Preconditions.checkState(!closed);
        MmappedRegions regions = cache.computeIfAbsent(channel.file(), ignored -> MmappedRegions.map(channel, metadata));
        Preconditions.checkArgument(regions.isValid(channel));
        regions.extend(metadata);
        return regions.sharedCopy();
    }

    @Override
    public void close()
    {
        closed = true;
        Iterator<MmappedRegions> it = cache.values().iterator();
        while (it.hasNext())
        {
            MmappedRegions region = it.next();
            region.closeQuietly();
            it.remove();
        }
    }
}
