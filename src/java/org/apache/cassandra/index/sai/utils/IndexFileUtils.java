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

package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;

public class IndexFileUtils
{
    protected static final Logger logger = LoggerFactory.getLogger(IndexFileUtils.class);

    @VisibleForTesting
    protected static final SequentialWriterOption defaultWriterOption = SequentialWriterOption.newBuilder()
                                                                                              .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                              .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                                              .bufferType(BufferType.OFF_HEAP)
                                                                                              .finishOnClose(true)
                                                                                              .build();

    public static final IndexFileUtils instance = new IndexFileUtils();

    private static final SequentialWriterOption writerOption = defaultWriterOption;

    @VisibleForTesting
    protected IndexFileUtils()
    {}

    public IndexOutputWriter openOutput(File file)
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        return new IndexOutputWriter(new IncrementalChecksumSequentialWriter(file));
    }

    public IndexOutputWriter openOutput(File file, boolean append) throws IOException
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        var checksumWriter = new IncrementalChecksumSequentialWriter(file);
        IndexOutputWriter indexOutputWriter = new IndexOutputWriter(checksumWriter);
        if (append)
        {
            // Got to recalculate checksum for the file opened for append, otherwise final checksum will be wrong.
            // Checksum verification is not happening here as it sis not guranteed that the file has the checksum/footer.
            checksumWriter.recalculateChecksum();
            indexOutputWriter.skipBytes(file.length());
        }

        return indexOutputWriter;
    }

    public IndexInput openInput(FileHandle handle)
    {
        return IndexInputReader.create(handle);
    }

    public IndexInput openBlockingInput(File file)
    {
        try (final FileHandle.Builder builder = new FileHandle.Builder(file))
        {
            final FileHandle fileHandle = builder.complete();
            final RandomAccessReader randomReader = fileHandle.createReader();

            return IndexInputReader.create(randomReader, fileHandle::close);
        }
    }

    public interface ChecksumWriter
    {
        long getChecksum();
    }

    class IncrementalChecksumSequentialWriter extends SequentialWriter implements ChecksumWriter
    {
        private final CRC32 checksum = new CRC32();

        IncrementalChecksumSequentialWriter(File file)
        {
            super(file, writerOption);
        }

        /**
         * Recalculates checksum for the file.
         *
         * Usefil when the file is opened for append and checksum will need to account for the existing data.
         * e.g. if the file opened for append is a new file, then checksum start at 0 and goes from there with the writes.
         * If the file opened for append is an existing file, without recalculating the checksum will start at 0
         * and only account for appended data. Checksum validation will compare it to the checksum of the whole file and fail.
         * Hence, for the existing files this method should be called to recalculate the checksum.
         *
         * @throws IOException if file read failed.
         */
        public void recalculateChecksum() throws IOException
        {
            checksum.reset();
            if (!file.exists())
                return;

            try(FileChannel ch = FileChannel.open(file.toPath(), StandardOpenOption.READ))
            {
                if (ch.size() == 0)
                    return;

                final ByteBuffer buf = ByteBuffer.allocateDirect(65536);
                int b = ch.read(buf);
                while (b > 0)
                {
                    buf.flip();
                    checksum.update(buf);
                    buf.clear();
                    b = ch.read(buf);
                }
            }
        }

        @Override
        public void write(ByteBuffer src) throws IOException
        {
            ByteBuffer shallowCopy = src.slice().order(src.order());
            super.write(src);
            checksum.update(shallowCopy);
        }

        @Override
        public void writeBoolean(boolean v) throws IOException
        {
            super.writeBoolean(v);
            checksum.update(v ? 1 : 0);
        }

        @Override
        public void writeByte(int b) throws IOException
        {
            super.writeByte(b);
            checksum.update(b);
        }

        // Do not override write(byte[] b) to avoid double-counting bytes in the checksum.
        // It just calls this method anyway.
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

        // To avoid double-counting bytes in the checksum.
        // Same as super's but calls super.writeByte
        @DontInline
        @Override
        protected void writeSlow(long bytes, int count) throws IOException
        {
            int origCount = count;
            if (ByteOrder.BIG_ENDIAN == buffer.order())
                while (count > 0) super.writeByte((int) (bytes >>> (8 * --count)));
            else
                while (count > 0) super.writeByte((int) (bytes >>> (8 * (origCount - count--))));
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
