/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.cassandra.io.util;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressedThrottledReader;
import org.apache.cassandra.io.compress.CompressionMetadata;

public class CompressedPoolingSegmentedFile extends PoolingSegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;
    private final TreeMap<Long, MappedByteBuffer> chunkSegments;

    public CompressedPoolingSegmentedFile(ChannelProxy channel, CompressionMetadata metadata)
    {
        this(channel, metadata, CompressedSegmentedFile.createMappedSegments(channel, metadata));
    }

    private CompressedPoolingSegmentedFile(ChannelProxy channel, CompressionMetadata metadata, TreeMap<Long, MappedByteBuffer> chunkSegments)
    {
        super(new Cleanup(channel, metadata, chunkSegments), channel, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
        this.chunkSegments = chunkSegments;
    }

    private CompressedPoolingSegmentedFile(CompressedPoolingSegmentedFile copy)
    {
        super(copy);
        this.metadata = copy.metadata;
        this.chunkSegments = copy.chunkSegments;
    }

    public ChannelProxy channel()
    {
        return channel;
    }

    public TreeMap<Long, MappedByteBuffer> chunkSegments()
    {
        return chunkSegments;
    }

    protected static final class Cleanup extends PoolingSegmentedFile.Cleanup
    {
        final CompressionMetadata metadata;
        final TreeMap<Long, MappedByteBuffer> chunkSegments;
        protected Cleanup(ChannelProxy channel, CompressionMetadata metadata, TreeMap<Long, MappedByteBuffer> chunkSegments)
        {
            super(channel);
            this.metadata = metadata;
            this.chunkSegments = chunkSegments;
        }
        public void tidy()
        {
            super.tidy();
            metadata.close();
            if (chunkSegments != null)
            {
                for (MappedByteBuffer segment : chunkSegments.values())
                    FileUtils.clean(segment);
            }
        }
    }

    public static class Builder extends CompressedSegmentedFile.Builder
    {
        public Builder(CompressedSequentialWriter writer)
        {
            super(writer);
        }

        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        public SegmentedFile complete(ChannelProxy channel, long overrideLength)
        {
            return new CompressedPoolingSegmentedFile(channel, metadata(channel.filePath(), overrideLength));
        }
    }

    public void dropPageCache(long before)
    {
        if (before >= metadata.dataLength)
            super.dropPageCache(0);
        super.dropPageCache(metadata.chunkFor(before).offset);
    }

    public RandomAccessReader createReader()
    {
        return CompressedRandomAccessReader.open(this);
    }

    public RandomAccessReader createThrottledReader(RateLimiter limiter)
    {
        return CompressedThrottledReader.open(this, limiter);
    }

    protected RandomAccessReader createPooledReader()
    {
        return CompressedRandomAccessReader.open(this);
    }

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }

    public CompressedPoolingSegmentedFile sharedCopy()
    {
        return new CompressedPoolingSegmentedFile(this);
    }
}
