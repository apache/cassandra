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

import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressedThrottledReader;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTableWriter;

public class CompressedSegmentedFile extends SegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;

    public CompressedSegmentedFile(String path, CompressionMetadata metadata)
    {
        super(new Cleanup(path, metadata), path, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
    }

    private CompressedSegmentedFile(CompressedSegmentedFile copy)
    {
        super(copy);
        this.metadata = copy.metadata;
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        final CompressionMetadata metadata;
        protected Cleanup(String path, CompressionMetadata metadata)
        {
            super(path);
            this.metadata = metadata;
        }
        public void tidy() throws Exception
        {
            metadata.close();
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

        protected CompressionMetadata metadata(String path, long overrideLength, boolean isFinal)
        {
            if (writer == null)
                return CompressionMetadata.create(path);

            return writer.open(overrideLength, isFinal);
        }

        public SegmentedFile complete(String path, long overrideLength, boolean isFinal)
        {
            assert !isFinal || overrideLength <= 0;
            return new CompressedSegmentedFile(path, metadata(path, overrideLength, isFinal));
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
        return CompressedRandomAccessReader.open(path, metadata);
    }

    public RandomAccessReader createThrottledReader(RateLimiter limiter)
    {
        return CompressedThrottledReader.open(path, metadata, limiter);
    }

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }
}
