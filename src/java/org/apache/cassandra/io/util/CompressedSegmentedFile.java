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

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.TreeMap;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressedThrottledReader;
import org.apache.cassandra.io.compress.CompressionMetadata;

public class CompressedSegmentedFile extends SegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;
    private static final boolean useMmap = DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap;
    private static int MAX_SEGMENT_SIZE = Integer.MAX_VALUE;
    private final TreeMap<Long, MappedByteBuffer> chunkSegments;

    public CompressedSegmentedFile(ChannelProxy channel, CompressionMetadata metadata)
    {
        this(channel, metadata, createMappedSegments(channel, metadata));
    }

    public CompressedSegmentedFile(ChannelProxy channel, CompressionMetadata metadata, TreeMap<Long, MappedByteBuffer> chunkSegments)
    {
        super(new Cleanup(channel, metadata, chunkSegments), channel, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
        this.chunkSegments = chunkSegments;
    }

    private CompressedSegmentedFile(CompressedSegmentedFile copy)
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

    static TreeMap<Long, MappedByteBuffer> createMappedSegments(ChannelProxy channel, CompressionMetadata metadata)
    {
        if (!useMmap)
            return null;
        TreeMap<Long, MappedByteBuffer> chunkSegments = new TreeMap<>();
        long offset = 0;
        long lastSegmentOffset = 0;
        long segmentSize = 0;

        while (offset < metadata.dataLength)
        {
            CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);

            //Reached a new mmap boundary
            if (segmentSize + chunk.length + 4 > MAX_SEGMENT_SIZE)
            {
                chunkSegments.put(lastSegmentOffset, channel.map(FileChannel.MapMode.READ_ONLY, lastSegmentOffset, segmentSize));
                lastSegmentOffset += segmentSize;
                segmentSize = 0;
            }

            segmentSize += chunk.length + 4; //checksum
            offset += metadata.chunkLength();
        }

        if (segmentSize > 0)
            chunkSegments.put(lastSegmentOffset, channel.map(FileChannel.MapMode.READ_ONLY, lastSegmentOffset, segmentSize));
        return chunkSegments;
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
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

    public CompressedSegmentedFile sharedCopy()
    {
        return new CompressedSegmentedFile(this);
    }

    public static class Builder extends SegmentedFile.Builder
    {
        protected final CompressedSequentialWriter writer;
        public Builder(CompressedSequentialWriter writer)
        {
            this.writer = writer;
        }

        public void addPotentialBoundary(long boundary)
        {
            // only one segment in a standard-io file
        }

        protected CompressionMetadata metadata(String path, long overrideLength)
        {
            if (writer == null)
                return CompressionMetadata.create(path);

            return writer.open(overrideLength);
        }

        public SegmentedFile complete(ChannelProxy channel, long overrideLength)
        {
            return new CompressedSegmentedFile(channel, metadata(channel.filePath(), overrideLength));
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

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }
}
