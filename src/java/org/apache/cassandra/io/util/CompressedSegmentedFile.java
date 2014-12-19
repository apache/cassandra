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

import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableWriter;

public class CompressedSegmentedFile extends SegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;

    public CompressedSegmentedFile(String path, CompressionMetadata metadata)
    {
        super(path, metadata.dataLength, metadata.compressedFileLength);
        this.metadata = metadata;
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

        protected CompressionMetadata metadata(String path, SSTableWriter.FinishType finishType)
        {
            if (writer == null)
                return CompressionMetadata.create(path);

            return writer.open(finishType);
        }

        public SegmentedFile complete(String path, SSTableWriter.FinishType finishType)
        {
            return new CompressedSegmentedFile(path, metadata(path, finishType));
        }
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = CompressedRandomAccessReader.open(path, metadata, null);
        reader.seek(position);
        return reader;
    }

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }

    public void cleanup()
    {
        metadata.close();
    }
}
