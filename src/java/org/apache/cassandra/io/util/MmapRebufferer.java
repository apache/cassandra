/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

/**
 * Rebufferer for memory-mapped files. Thread-safe and shared among reader instances.
 * This is simply a thin wrapper around MmappedRegions as the buffers there can be used directly after duplication.
 */
class MmapRebufferer extends AbstractReaderFileProxy implements Rebufferer, RebuffererFactory
{
    protected final MmappedRegions regions;

    MmapRebufferer(ChannelProxy channel, long fileLength, MmappedRegions regions)
    {
        super(channel, fileLength);
        this.regions = regions;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        return regions.floor(position);
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return this;
    }

    @Override
    public void close()
    {
        regions.closeQuietly();
    }

    @Override
    public void closeReader()
    {
        // Instance is shared among readers. Nothing to release.
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s - data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             fileLength());
    }
}
