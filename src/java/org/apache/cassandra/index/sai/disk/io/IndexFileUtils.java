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
    public static final SequentialWriterOption DEFAULT_WRITER_OPTION = SequentialWriterOption.newBuilder()
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

        return new IndexOutputWriter(new ChecksummingWriter(file, writerOption));
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

    static class ChecksummingWriter extends SequentialWriter
    {
        private final CRC32 checksum = new CRC32();

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
