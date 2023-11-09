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

package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

public class IndexFileUtils
{
    @VisibleForTesting
    public static final SequentialWriterOption DEFAULT_WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                             .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                             .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                                                                             .bufferType(BufferType.OFF_HEAP)
                                                                                             .finishOnClose(true)
                                                                                             .build();

    public static final IndexFileUtils instance = new IndexFileUtils(DEFAULT_WRITER_OPTION);
    private static final Supplier<Checksum> CHECKSUM_FACTORY = CRC32C::new;

    private final SequentialWriterOption writerOption;

    @VisibleForTesting
    protected IndexFileUtils(SequentialWriterOption writerOption)
    {
        this.writerOption = writerOption;
    }

    public IndexOutputWriter openOutput(File file)
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        return new IndexOutputWriter(new ChecksummingWriter(file, writerOption));
    }

    public IndexOutputWriter openOutput(File file, boolean append) throws IOException
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        IndexOutputWriter indexOutputWriter = new IndexOutputWriter(new ChecksummingWriter(file, writerOption));
        if (append)
            indexOutputWriter.skipBytes(file.length());

        return indexOutputWriter;
    }

    public IndexInput openInput(FileHandle handle)
    {
        return IndexInputReader.create(handle);
    }

    public IndexInput openBlockingInput(File file)
    {
        FileHandle fileHandle = new FileHandle.Builder(file).complete();
        RandomAccessReader randomReader = fileHandle.createReader();

        return IndexInputReader.create(randomReader, fileHandle::close);
    }

    public static ChecksumIndexInput getBufferedChecksumIndexInput(IndexInput indexInput)
    {
        return new BufferedChecksumIndexInput(indexInput, CHECKSUM_FACTORY.get());
    }

    /**
     * The SequentialWriter that calculates checksum of the data written to the file. This writer extends
     * {@link SequentialWriter} to add the checksumming functionality and typically is used together
     * with {@link IndexOutputWriter}. This, in turn, is used in conjunction with {@link BufferedChecksumIndexInput}
     * to verify the checksum of the data read from the file, so they must share the same checksum algorithm.
     */
    static class ChecksummingWriter extends SequentialWriter
    {
        private final Checksum checksum = CHECKSUM_FACTORY.get();

        ChecksummingWriter(File file, SequentialWriterOption writerOption)
        {
            super(file, writerOption);
        }

        public long getChecksum() throws IOException
        {
            flush();
            return checksum.getValue();
        }

        @Override
        protected void flushData()
        {
            ByteBuffer toAppend = buffer.duplicate().flip();
            super.flushData();
            checksum.update(toAppend);
        }
    }
}
