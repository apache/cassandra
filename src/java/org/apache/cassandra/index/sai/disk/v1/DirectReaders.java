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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import org.apache.lucene.store.RandomAccessInput;

public class DirectReaders
{
    public interface Reader
    {
        long get(RandomAccessInput in, long offset, long index);
    }

    public static Reader getReaderForBitsPerValue(byte bitsPerValue)
    {
        switch (bitsPerValue)
        {
            case 0:
                return READER_0;
            case 1:
                return READER_1;
            case 2:
                return READER_2;
            case 4:
                return READER_4;
            case 8:
                return READER_8;
            case 12:
                return READER_12;
            case 16:
                return READER_16;
            case 20:
                return READER_20;
            case 24:
                return READER_24;
            case 28:
                return READER_28;
            case 32:
                return READER_32;
            case 40:
                return READER_40;
            case 48:
                return READER_48;
            case 56:
                return READER_56;
            case 64:
                return READER_64;
            default:
                throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
        }
    }

    private static final Reader READER_0 = (in, offset, index) -> 0;

    private static final Reader READER_1 = (in, offset, index) -> {
        try
        {
            int shift = (int) (index & 7);
            return (in.readByte(offset + (index >>> 3)) >>> shift) & 0x1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_2 = (in, offset, index) -> {
        try
        {
            int shift = (int) (index & 3) << 1;
            return (in.readByte(offset + (index >>> 2)) >>> shift) & 0x3;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_4 = (in, offset, index) -> {
        try
        {
            int shift = (int) (index & 1) << 2;
            return (in.readByte(offset + (index >>> 1)) >>> shift) & 0xF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_8 = (in, offset, index) -> {
        try
        {
            return in.readByte(offset + index) & 0xFF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_12 = (in, offset, index) -> {
        try
        {
            long o = (index * 12) >>> 3;
            int shift = (int) (index & 1) * 4;
            int v = in.readShort(offset + o) & 0xFFFF;
            v = (v >>> shift) | ((in.readByte(offset + o + 2) & 0xFF) << (16 - shift));
            return v & 0xFFF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_16 = (in, offset, index) -> {
        try
        {
            long position = offset + (index << 1);
            int lowByte = in.readByte(position) & 0xFF;
            int highByte = in.readByte(position + 1) & 0xFF;
            return (highByte << 8) | lowByte;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_20 = (in, offset, index) -> {
        try
        {
            long o = (index * 20) >>> 3;
            int shift = (int) (index & 1) * 4;
            int v1 = in.readShort(offset + o) & 0xFFFF;
            int v2 = in.readByte(offset + o + 2) & 0xFF;
            int v = (v1 >>> shift) | (v2 << (16 - shift));
            return v & 0xFFFFF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_24 = (in, offset, index) -> {
        try
        {
            long o = offset + index * 3;
            int v1 = in.readByte(o) & 0xFF;
            int v2 = in.readShort(o + 1) & 0xFFFF;
            return (v2 << 8) | v1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_28 = (in, offset, index) -> {
        try
        {
            long o = (index * 28) >>> 3;
            int shift = (int) (index & 1) * 4;
            int v1 = in.readByte(o) & 0xFF;
            int v2 = in.readShort(o + 1) & 0xFFFF;
            int v3 = in.readByte(o + 3) & 0xFF;
            int v = (v3 << 24) | (v2 << 8) | v1;
            return (v >>> shift) & 0xFFFFFFF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_32 = (in, offset, index) -> {
        try
        {
            long o = offset + (index << 2);
            int v1 = in.readByte(o) & 0xFF;
            int v2 = in.readByte(o + 1) & 0xFF;
            int v3 = in.readByte(o + 2) & 0xFF;
            int v4 = in.readByte(o + 3) & 0xFF;
            return ((long) v4 << 24) | (v3 << 16) | (v2 << 8) | v1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_40 = (in, offset, index) -> {
        try
        {
            long o = offset + index * 5;
            long v1 = in.readByte(o) & 0xFFL;
            long v2 = in.readInt(o + 1) & 0xFFFFFFFFL;
            return (v2 << 8) | v1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_48 = (in, offset, index) -> {
        try
        {
            long o = offset + index * 6;
            long v1 = in.readShort(o) & 0xFFFFL;
            long v2 = in.readInt(o + 2) & 0xFFFFFFFFL;
            return (v2 << 16) | v1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_56 = (in, offset, index) -> {
        try
        {
            long o = offset + index * 7;
            long v1 = in.readByte(o) & 0xFFL;
            long v2 = in.readInt(o + 1) & 0xFFFFFFFFL;
            long v3 = in.readShort(o + 5) & 0xFFFFL;
            return (v3 << 40) | (v2 << 8) | v1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_64 = (in, offset, index) -> {
        try
        {
            long o = offset + (index << 3);
            long v1 = in.readByte(o) & 0xFFL;
            long v2 = in.readByte(o + 1) & 0xFFL;
            long v3 = in.readByte(o + 2) & 0xFFL;
            long v4 = in.readByte(o + 3) & 0xFFL;
            long v5 = in.readByte(o + 4) & 0xFFL;
            long v6 = in.readByte(o + 5) & 0xFFL;
            long v7 = in.readByte(o + 6) & 0xFFL;
            long v8 = in.readByte(o + 7) & 0xFFL;
            return (v8 << 56) | (v7 << 48) | (v6 << 40) | (v5 << 32) |
                   (v4 << 24) | (v3 << 16) | (v2 << 8) | v1;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };
}
