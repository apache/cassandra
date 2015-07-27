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

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.Ref;

public class CompressedSegmentedFile extends SegmentedFile implements ICompressedFile
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedSegmentedFile.class);
    private static final boolean useMmap = DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap;

    public final CompressionMetadata metadata;
    private final MmappedRegions regions;

    public CompressedSegmentedFile(ChannelProxy channel, int bufferSize, CompressionMetadata metadata)
    {
        this(channel,
             bufferSize,
             metadata,
             useMmap
             ? MmappedRegions.map(channel, metadata)
             : null);
    }

    public CompressedSegmentedFile(ChannelProxy channel, int bufferSize, CompressionMetadata metadata, MmappedRegions regions)
    {
        super(new Cleanup(channel, metadata, regions), channel, bufferSize, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
        this.regions = regions;
    }

    private CompressedSegmentedFile(CompressedSegmentedFile copy)
    {
        super(copy);
        this.metadata = copy.metadata;
        this.regions = copy.regions;
    }

    public ChannelProxy channel()
    {
        return channel;
    }

    public MmappedRegions regions()
    {
        return regions;
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        final CompressionMetadata metadata;
        private final MmappedRegions regions;

        protected Cleanup(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
        {
            super(channel);
            this.metadata = metadata;
            this.regions = regions;
        }
        public void tidy()
        {
            Throwable err = regions == null ? null : regions.close(null);
            if (err != null)
            {
                JVMStabilityInspector.inspectThrowable(err);

                // This is not supposed to happen
                logger.error("Error while closing mmapped regions", err);
            }

            metadata.close();

            super.tidy();
        }
    }

    public CompressedSegmentedFile sharedCopy()
    {
        return new CompressedSegmentedFile(this);
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        metadata.addTo(identities);
    }

    public static class Builder extends SegmentedFile.Builder
    {
        final CompressedSequentialWriter writer;
        public Builder(CompressedSequentialWriter writer)
        {
            this.writer = writer;
        }

        protected CompressionMetadata metadata(String path, long overrideLength)
        {
            if (writer == null)
                return CompressionMetadata.create(path);

            return writer.open(overrideLength);
        }

        public SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength)
        {
            return new CompressedSegmentedFile(channel, bufferSize, metadata(channel.filePath(), overrideLength));
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
        return new CompressedRandomAccessReader.Builder(this).build();
    }

    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new CompressedRandomAccessReader.Builder(this).limiter(limiter).build();
    }

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }
}
