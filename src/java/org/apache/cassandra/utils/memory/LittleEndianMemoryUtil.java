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

package org.apache.cassandra.utils.memory;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.Architecture;

public class LittleEndianMemoryUtil extends MemoryUtil
{
    public static int getUnsignedShort(long address)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b1) == 0L)
            return (Architecture.BIG_ENDIAN ? Short.reverseBytes(unsafe.getShort(address)) : unsafe.getShort(address)) & 0xffff;
        else
            return getShortByByte(address) & 0xffff;
    }

    public static int getInt(long address)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b11) == 0L)
            return Architecture.BIG_ENDIAN ? Integer.reverseBytes(unsafe.getInt(address)) : unsafe.getInt(address);
        else
            return getIntByByte(address);
    }

    public static long getLong(long address)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b111) == 0L)
            return Architecture.BIG_ENDIAN ? Long.reverseBytes(unsafe.getLong(address)) : unsafe.getLong(address);
        else
            return getLongByByte(address);
    }

    public static void setShort(long address, short s)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b1) == 0L)
            unsafe.putShort(address, Architecture.BIG_ENDIAN ? Short.reverseBytes(s) : s);
        else
            putShortByByte(address, s);
    }

    public static void setInt(long address, int l)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b11) == 0L)
            unsafe.putInt(address, Architecture.BIG_ENDIAN ? Integer.reverseBytes(l) : l);
        else
            putIntByByte(address, l);
    }

    public static void setLong(long address, long l)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b111) == 0L)
            unsafe.putLong(address, Architecture.BIG_ENDIAN ? Long.reverseBytes(l) : l);
        else
            putLongByByte(address, l);
    }

    @VisibleForTesting
    static long getLongByByte(long address)
    {
        return  (((long) unsafe.getByte(address + 7)       ) << 56) |
                (((long) unsafe.getByte(address + 6) & 0xff) << 48) |
                (((long) unsafe.getByte(address + 5) & 0xff) << 40) |
                (((long) unsafe.getByte(address + 4) & 0xff) << 32) |
                (((long) unsafe.getByte(address + 3) & 0xff) << 24) |
                (((long) unsafe.getByte(address + 2) & 0xff) << 16) |
                (((long) unsafe.getByte(address + 1) & 0xff) <<  8) |
                (((long) unsafe.getByte(address    ) & 0xff)      );
    }

    @VisibleForTesting
    static int getIntByByte(long address)
    {
        return  (((int) unsafe.getByte(address + 3)       ) << 24) |
                (((int) unsafe.getByte(address + 2) & 0xff) << 16) |
                (((int) unsafe.getByte(address + 1) & 0xff) <<  8) |
                (((int) unsafe.getByte(address    ) & 0xff)      );
    }

    @VisibleForTesting
    static int getShortByByte(long address)
    {
        return  (((int) unsafe.getByte(address + 1)       ) <<  8) |
                (((int) unsafe.getByte(address    ) & 0xff)      );
    }

    @VisibleForTesting
    static void putLongByByte(long address, long value)
    {
        unsafe.putByte(address + 7, (byte) (value >> 56));
        unsafe.putByte(address + 6, (byte) (value >> 48));
        unsafe.putByte(address + 5, (byte) (value >> 40));
        unsafe.putByte(address + 4, (byte) (value >> 32));
        unsafe.putByte(address + 3, (byte) (value >> 24));
        unsafe.putByte(address + 2, (byte) (value >> 16));
        unsafe.putByte(address + 1, (byte) (value >>  8));
        unsafe.putByte(address    , (byte) (value      ));
    }

    @VisibleForTesting
    static void putIntByByte(long address, int value)
    {
        unsafe.putByte(address + 3, (byte) (value >> 24));
        unsafe.putByte(address + 2, (byte) (value >> 16));
        unsafe.putByte(address + 1, (byte) (value >>  8));
        unsafe.putByte(address    , (byte) (value      ));
    }

    @VisibleForTesting
    static void putShortByByte(long address, short value)
    {
        unsafe.putByte(address + 1, (byte) (value >> 8));
        unsafe.putByte(address    , (byte) (value     ));
    }

    public static ByteBuffer getByteBuffer(long address, int length)
    {
        return getByteBuffer(address, length, ByteOrder.LITTLE_ENDIAN);
    }

    public static ByteBuffer getHollowDirectByteBuffer()
    {
        return getHollowDirectByteBuffer(ByteOrder.LITTLE_ENDIAN);
    }
}
