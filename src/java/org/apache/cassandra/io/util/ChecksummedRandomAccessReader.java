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
import java.util.zip.Adler32;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ChecksummedRandomAccessReader extends RandomAccessReader
{
    @SuppressWarnings("serial")
    public static class CorruptFileException extends RuntimeException
    {
        public final File file;

        public CorruptFileException(Exception cause, File file)
        {
            super(cause);
            this.file = file;
        }
    }

    private final DataIntegrityMetadata.ChecksumValidator validator;
    private final File file;

    protected ChecksummedRandomAccessReader(File file, ChannelProxy channel, DataIntegrityMetadata.ChecksumValidator validator)
    {
        super(channel, validator.chunkSize, -1, BufferType.ON_HEAP, null);
        this.validator = validator;
        this.file = file;
    }

    @SuppressWarnings("resource")
    public static ChecksummedRandomAccessReader open(File file, File crcFile) throws IOException
    {
        try (ChannelProxy channel = new ChannelProxy(file))
        {
            RandomAccessReader crcReader = RandomAccessReader.open(crcFile);
            boolean closeCrcReader = true;
            try
            {
                DataIntegrityMetadata.ChecksumValidator validator =
                        new DataIntegrityMetadata.ChecksumValidator(new Adler32(), crcReader, file.getPath());
                closeCrcReader = false;
                boolean closeValidator = true;
                try
                {
                    ChecksummedRandomAccessReader retval = new ChecksummedRandomAccessReader(file, channel, validator);
                    closeValidator = false;
                    return retval;
                }
                finally
                {
                    if (closeValidator)
                        validator.close();
                }
            }
            finally
            {
                if (closeCrcReader)
                    crcReader.close();
            }
        }
    }

    @Override
    protected void reBuffer()
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
            throw new CorruptFileException(e, file);
        }

        buffer.position((int) (desiredPosition - bufferOffset));
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
        super.close();
        validator.close();
    }
}
