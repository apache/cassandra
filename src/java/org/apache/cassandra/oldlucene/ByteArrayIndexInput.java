/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.oldlucene;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * A {@link IndexInput} backed by a byte array.
 *
 * @lucene.experimental
 */
public final class ByteArrayIndexInput extends IndexInput implements RandomAccessInput
{
    private byte[] bytes;

    private final int offset;
    private final int length;

    private int pos;

    public ByteArrayIndexInput(String description, byte[] bytes) {
        this(description, bytes, 0, bytes.length);
    }

    public ByteArrayIndexInput(String description, byte[] bytes, int offs, int length) {
        super(description);
        this.offset = offs;
        this.bytes = bytes;
        this.length = length;
        this.pos = offs;
    }

    public long getFilePointer() {
        return pos - offset;
    }

    public void seek(long pos) throws EOFException {
        int newPos = Math.toIntExact(pos + offset);
        try {
            if (pos < 0 || pos > length) {
                throw new EOFException();
            }
        } finally {
            this.pos = newPos;
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public short readShort() {
        return (short) (((bytes[pos++] & 0xFF) <<  8) |  (bytes[pos++] & 0xFF));
    }

    @Override
    public int readInt() {
        return ((bytes[pos++] & 0xFF) << 24) | ((bytes[pos++] & 0xFF) << 16)
               | ((bytes[pos++] & 0xFF) <<  8) |  (bytes[pos++] & 0xFF);
    }

    @Override
    public long readLong() {
        final int i1 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) |
                       ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
        final int i2 = ((bytes[pos++] & 0xff) << 24) | ((bytes[pos++] & 0xff) << 16) |
                       ((bytes[pos++] & 0xff) << 8) | (bytes[pos++] & 0xff);
        return (((long)i1) << 32) | (i2 & 0xFFFFFFFFL);
    }

    @Override
    public int readVInt() {
        byte b = bytes[pos++];
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = bytes[pos++];
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = bytes[pos++];
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
        throw new RuntimeException("Invalid vInt detected (too many bits)");
    }

    @Override
    public long readVLong() {
        byte b = bytes[pos++];
        if (b >= 0) return b;
        long i = b & 0x7FL;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = bytes[pos++];
        i |= (b & 0x7FL) << 56;
        if (b >= 0) return i;
        throw new RuntimeException("Invalid vLong detected (negative values disallowed)");
    }

    // NOTE: AIOOBE not EOF if you read too much
    @Override
    public byte readByte() {
        return bytes[pos++];
    }

    // NOTE: AIOOBE not EOF if you read too much
    @Override
    public void readBytes(byte[] b, int offset, int len) {
        System.arraycopy(bytes, pos, b, offset, len);
        pos += len;
    }

    @Override
    public void close() {
        bytes = null;
    }

    @Override
    public IndexInput clone() {
        ByteArrayIndexInput slice = slice("(cloned)" + toString(), 0, length());
        try {
            slice.seek(getFilePointer());
        } catch (EOFException e) {
            throw new UncheckedIOException(e);
        }
        return slice;
    }

    public ByteArrayIndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                                                             "slice(offset=%s, length=%s) is out of bounds: %s",
                                                             offset, length, this));
        }

        return new ByteArrayIndexInput(sliceDescription, this.bytes, Math.toIntExact(this.offset + offset),
                                       Math.toIntExact(length));
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return bytes[Math.toIntExact(offset + pos)];
    }

    @Override
    public short readShort(long pos) throws IOException {
        int i = Math.toIntExact(offset + pos);
        return (short) (((bytes[i]     & 0xFF) << 8) |
                        (bytes[i + 1] & 0xFF));
    }

    @Override
    public int readInt(long pos) throws IOException {
        int i = Math.toIntExact(offset + pos);
        return ((bytes[i]     & 0xFF) << 24) |
               ((bytes[i + 1] & 0xFF) << 16) |
               ((bytes[i + 2] & 0xFF) <<  8) |
               (bytes[i + 3] & 0xFF);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return (((long) readInt(pos)) << 32) |
               (readInt(pos + 4) & 0xFFFFFFFFL);
    }
}
