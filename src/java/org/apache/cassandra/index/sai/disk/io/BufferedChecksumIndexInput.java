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

import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.zip.Checksum;

/**
 * This implementation of {@link ChecksumIndexInput} is based on {@link org.apache.lucene.store.BufferedChecksumIndexInput}
 * but uses custom checksum algorithm instead of the hardcoded {@code CRC32} in {@code BufferedChecksumIndexInput}.
 *
 * @see org.apache.cassandra.index.sai.disk.io.IndexFileUtils.ChecksummingWriter
 */
class BufferedChecksumIndexInput extends ChecksumIndexInput
{
    final IndexInput delegate;
    final Checksum digest;

    public BufferedChecksumIndexInput(IndexInput delegate, Checksum digest)
    {
        super("BufferedChecksumIndexInput(" + delegate + ')');
        this.delegate = delegate;
        this.digest = digest;
    }

    public byte readByte() throws IOException
    {
        byte b = this.delegate.readByte();
        this.digest.update(b);
        return b;
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException
    {
        this.delegate.readBytes(b, offset, len);
        this.digest.update(b, offset, len);
    }

    public long getChecksum()
    {
        return this.digest.getValue();
    }

    public void close() throws IOException
    {
        this.delegate.close();
    }
    public long getFilePointer()
    {
        return this.delegate.getFilePointer();
    }

    public long length()
    {
        return this.delegate.length();
    }

    public IndexInput clone()
    {
        throw new UnsupportedOperationException();
    }

    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
