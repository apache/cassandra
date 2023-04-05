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

import java.io.IOException;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;

class ChecksummedRebufferer extends BufferManagingRebufferer
{
    private final DataIntegrityMetadata.ChecksumValidator validator;

    @SuppressWarnings("resource") // chunk reader is closed by super::close()
    ChecksummedRebufferer(ChannelProxy channel, DataIntegrityMetadata.ChecksumValidator validator)
    {
        super(new SimpleChunkReader(channel, channel.size(), BufferType.ON_HEAP, validator.chunkSize));
        this.validator = validator;
    }

    @Override
    public BufferHolder rebuffer(long desiredPosition)
    {
        if (desiredPosition != offset + buffer.position())
            validator.seek(desiredPosition);

        // align with buffer size, as checksums were computed in chunks of buffer size each.
        offset = alignedPosition(desiredPosition);
        source.readChunk(offset, buffer);

        try
        {
            validator.validate(ByteBufferUtil.getArray(buffer), 0, buffer.remaining());
        }
        catch (IOException e)
        {
            throw new CorruptFileException(e, channel().filePath());
        }

        return this;
    }

    @Override
    public void close()
    {
        try
        {
            source.close();
        }
        finally
        {
            validator.close();
        }
    }

    @Override
    long alignedPosition(long desiredPosition)
    {
        return (desiredPosition / buffer.capacity()) * buffer.capacity();
    }
}
