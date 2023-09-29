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
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import java.util.zip.Adler32;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.FastThreadLocal;

public enum ChecksumType
{
    ADLER32
    {
        @Override
        public Checksum newInstance()
        {
            return new Adler32();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            checksum.reset();
            checksum.update(buf);
        }
    },
    CRC32
    {
        @Override
        public Checksum newInstance()
        {
            return new CRC32();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            checksum.reset();
            checksum.update(buf);
        }
    },
    CRC32_WITH_INITIAL_BYTES
    {
        private final byte[] initialBytes = new byte[] { (byte) 0xFA, (byte) 0x2D, (byte) 0x55, (byte) 0xCA };

        @Override
        public Checksum newInstance()
        {
            return new CRC32();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            checksum.reset();
            checksum.update(initialBytes);
            checksum.update(buf);
        }
    },
    CRC32C
    {
        @Override
        public Checksum newInstance()
        {
            return new CRC32C();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            checksum.reset();
            checksum.update(buf);
        }
    };

    public abstract Checksum newInstance();
    public abstract void update(Checksum checksum, ByteBuffer buf);

    private final FastThreadLocal<Checksum> instances = new FastThreadLocal<>()
    {
        @Override
        protected Checksum initialValue()
        {
            return newInstance();
        }
    };

    public long of(ByteBuffer buf)
    {
        Checksum checksum = instances.get();
        update(checksum, buf);
        return checksum.getValue();
    }

    public long of(ByteBuffer buf, int start, int end)
    {
        int savePosition = buf.position();
        int saveLimit = buf.limit();
        buf.position(start);
        buf.limit(end);

        Checksum checksum = instances.get();
        update(checksum, buf);

        buf.position(savePosition);
        buf.limit(saveLimit);
        return checksum.getValue();
    }

    public long of(ByteBuf buffer, int startReaderIndex, int endReaderIndex)
    {
        return of(buffer.internalNioBuffer(startReaderIndex, endReaderIndex - startReaderIndex));
    }

    public long of(byte[] data, int off, int len)
    {
        Checksum checksum = instances.get();
        checksum.update(data, off, len);
        return checksum.getValue();
    }
}
