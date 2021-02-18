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

class DirectReaders
{
    interface Reader
    {
        long get(RandomAccessInput in, long offset, long index);
    }

    static Reader getReaderForBitsPerValue(byte bitsPerValue)
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
            int shift = 7 - (int) (index & 7);
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
            int shift = (3 - (int) (index & 3)) << 1;
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
            int shift = (int) ((index + 1) & 1) << 2;
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
            int shift = (int) ((index + 1) & 1) << 2;
            return (in.readShort(offset + o) >>> shift) & 0xFFF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_16 = (in, offset, index) -> {
        try
        {
            return in.readShort(offset + (index << 1)) & 0xFFFF;
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
            int v = in.readInt(offset + o) >>> 8;
            int shift = (int) ((index + 1) & 1) << 2;
            return (v >>> shift) & 0xFFFFF;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_24 = (in, offset, index) -> {
        try
        {
            return in.readInt(offset + index * 3) >>> 8;
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
            int shift = (int) ((index + 1) & 1) << 2;
            return (in.readInt(offset + o) >>> shift) & 0xFFFFFFFL;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_32 = (in, offset, index) -> {
        try
        {
            return in.readInt(offset + (index << 2)) & 0xFFFFFFFFL;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_40 = (in, offset, index) -> {
        try
        {
            return in.readLong(offset + index * 5) >>> 24;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_48 = (in, offset, index) -> {
        try
        {
            return in.readLong(offset + index * 6) >>> 16;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_56 = (in, offset, index) -> {
        try
        {
            return in.readLong(offset + index * 7) >>> 8;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };

    private static final Reader READER_64 = (in, offset, index) -> {
        try
        {
            return in.readLong(offset + (index << 3));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    };
}
