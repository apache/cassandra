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
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.IndexInput;

public class IndexFileUtils
{
    @VisibleForTesting
    protected static final SequentialWriterOption DEFAULT_WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                                .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                                .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                                                                                .bufferType(BufferType.OFF_HEAP)
                                                                                                .finishOnClose(true)
                                                                                                .build();

    public static final IndexFileUtils instance = new IndexFileUtils(DEFAULT_WRITER_OPTION);

    private final SequentialWriterOption writerOption;

    @VisibleForTesting
    protected IndexFileUtils(SequentialWriterOption writerOption)
    {
        this.writerOption = writerOption;
    }

    @SuppressWarnings({"resource", "RedundantSuppression"})
    public IndexOutputWriter openOutput(File file)
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        return new IndexOutputWriter(new IncrementalChecksumSequentialWriter(file, writerOption));
    }

    public IndexInput openInput(FileHandle handle)
    {
        return IndexInputReader.create(handle);
    }

    @SuppressWarnings({"resource", "RedundantSuppression"})
    public IndexInput openBlockingInput(File file)
    {
        FileHandle fileHandle = new FileHandle.Builder(file).complete();
        RandomAccessReader randomReader = fileHandle.createReader();

        return IndexInputReader.create(randomReader, fileHandle::close);
    }

    public interface ChecksumWriter
    {
        long getChecksum();
    }

    static class IncrementalChecksumSequentialWriter extends SequentialWriter implements ChecksumWriter
    {
        private final CRC32 checksum = new CRC32();

        IncrementalChecksumSequentialWriter(File file, SequentialWriterOption writerOption)
        {
            super(file, writerOption);
        }

        @Override
        public void writeByte(int b) throws IOException
        {
            super.writeByte(b);
            checksum.update(b);
        }

        @Override
        public void write(byte[] b) throws IOException
        {
            super.write(b);
            checksum.update(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            super.write(b, off, len);
            checksum.update(b, off, len);
        }

        @Override
        public void writeChar(int v) throws IOException
        {
            super.writeChar(v);
            addTochecksum(v, 2);
        }

        @Override
        public void writeInt(int v) throws IOException
        {
            super.writeInt(v);
            addTochecksum(v, 4);
        }

        @Override
        public void writeLong(long v) throws IOException
        {
            super.writeLong(v);
            addTochecksum(v, 8);
        }

        public long getChecksum()
        {
            return checksum.getValue();
        }

        private void addTochecksum(long bytes, int count)
        {
            int origCount = count;
            if (ByteOrder.BIG_ENDIAN == buffer.order())
                while (count > 0) checksum.update((int) (bytes >>> (8 * --count)));
            else
                while (count > 0) checksum.update((int) (bytes >>> (8 * (origCount - count--))));
        }
    }
}
