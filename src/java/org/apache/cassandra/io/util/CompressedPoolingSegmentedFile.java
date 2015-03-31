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

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressedThrottledReader;
import org.apache.cassandra.io.compress.CompressionMetadata;

public class CompressedPoolingSegmentedFile extends PoolingSegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;

    public CompressedPoolingSegmentedFile(String path, CompressionMetadata metadata)
    {
        super(new Cleanup(path, metadata), path, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
    }

    private CompressedPoolingSegmentedFile(CompressedPoolingSegmentedFile copy)
    {
        super(copy);
        this.metadata = copy.metadata;
    }

    protected static final class Cleanup extends PoolingSegmentedFile.Cleanup
    {
        final CompressionMetadata metadata;
        protected Cleanup(String path, CompressionMetadata metadata)
        {
            super(path);
            this.metadata = metadata;
        }
        public void tidy() throws Exception
        {
            super.tidy();
            metadata.close();
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

        public SegmentedFile complete(String path, long overrideLength, boolean isFinal)
        {
            return new CompressedPoolingSegmentedFile(path, metadata(path, overrideLength, isFinal));
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
        return CompressedRandomAccessReader.open(path, metadata, null);
    }

    public RandomAccessReader createThrottledReader(RateLimiter limiter)
    {
        return CompressedThrottledReader.open(path, metadata, limiter);
    }

    protected RandomAccessReader createPooledReader()
    {
        return CompressedRandomAccessReader.open(path, metadata, this);
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
