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
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import java.util.zip.Adler32;

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
            ((Adler32)checksum).update(buf);
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
            ((CRC32)checksum).update(buf);
        }

    };

    public abstract Checksum newInstance();
    public abstract void update(Checksum checksum, ByteBuffer buf);

    private FastThreadLocal<Checksum> instances = new FastThreadLocal<Checksum>()
    {
        protected Checksum initialValue() throws Exception
        {
            return newInstance();
        }
    };

    public long of(ByteBuffer buf)
    {
        Checksum checksum = instances.get();
        checksum.reset();
        update(checksum, buf);
        return checksum.getValue();
    }

    public long of(byte[] data, int off, int len)
    {
        Checksum checksum = instances.get();
        checksum.reset();
        checksum.update(data, off, len);
        return checksum.getValue();
    }
}
