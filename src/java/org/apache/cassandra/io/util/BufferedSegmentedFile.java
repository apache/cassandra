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

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.compress.BufferType;

public class BufferedSegmentedFile extends SegmentedFile
{
    public BufferedSegmentedFile(ChannelProxy channel, int bufferSize, long length)
    {
        this(channel, createRebufferer(channel, length, bufferSize), length);
    }

    private BufferedSegmentedFile(ChannelProxy channel, RebuffererFactory rebufferer, long length)
    {
        super(new Cleanup(channel, rebufferer), channel, rebufferer, length);
    }

    private BufferedSegmentedFile(BufferedSegmentedFile copy)
    {
        super(copy);
    }

    private static RebuffererFactory createRebufferer(ChannelProxy channel, long length, int bufferSize)
    {
        return ChunkCache.maybeWrap(new SimpleChunkReader(channel, length, BufferType.OFF_HEAP, bufferSize));
    }

    public static class Builder extends SegmentedFile.Builder
    {
        public SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength)
        {
            long length = overrideLength > 0 ? overrideLength : channel.size();
            return new BufferedSegmentedFile(channel, bufferSize, length);
        }
    }

    public BufferedSegmentedFile sharedCopy()
    {
        return new BufferedSegmentedFile(this);
    }
}
