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

import java.io.File;
import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

public class ChecksummedRandomAccessReader extends RandomAccessReader
{
    @SuppressWarnings("serial")
    public static class CorruptFileException extends RuntimeException
    {
        public final String filePath;

        public CorruptFileException(Exception cause, String filePath)
        {
            super(cause);
            this.filePath = filePath;
        }
    }

    private final DataIntegrityMetadata.ChecksumValidator validator;

    private ChecksummedRandomAccessReader(Builder builder)
    {
        super(builder);
        this.validator = builder.validator;
    }

    @SuppressWarnings("resource")
    @Override
    protected void reBufferStandard()
    {
        long desiredPosition = current();
        // align with buffer size, as checksums were computed in chunks of buffer size each.
        bufferOffset = (desiredPosition / buffer.capacity()) * buffer.capacity();

        buffer.clear();

        long position = bufferOffset;
        while (buffer.hasRemaining())
        {
            int n = channel.read(buffer, position);
            if (n < 0)
                break;
            position += n;
        }

        buffer.flip();

        try
        {
            validator.validate(ByteBufferUtil.getArray(buffer), 0, buffer.remaining());
        }
        catch (IOException e)
        {
            throw new CorruptFileException(e, channel.filePath());
        }

        buffer.position((int) (desiredPosition - bufferOffset));
    }

    @Override
    protected void reBufferMmap()
    {
        throw new AssertionError("Unsupported operation");
    }

    @Override
    public void seek(long newPosition)
    {
        validator.seek(newPosition);
        super.seek(newPosition);
    }

    @Override
    public void close()
    {
        Throwables.perform(channel.filePath(), Throwables.FileOpType.READ,
                           super::close,
                           validator::close,
                           channel::close);
    }

    public static final class Builder extends RandomAccessReader.Builder
    {
        private final DataIntegrityMetadata.ChecksumValidator validator;

        @SuppressWarnings("resource")
        public Builder(File file, File crcFile) throws IOException
        {
            super(new ChannelProxy(file));
            this.validator = new DataIntegrityMetadata.ChecksumValidator(new CRC32(),
                                                                         RandomAccessReader.open(crcFile),
                                                                         file.getPath());

            super.bufferSize(validator.chunkSize)
                 .bufferType(BufferType.ON_HEAP);
        }

        @Override
        public RandomAccessReader build()
        {
            return new ChecksummedRandomAccessReader(this);
        }
    }
}
