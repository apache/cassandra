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

/**
 * Use this API only for data which are stored in-memory
 * and not serialized directly (without converting to Java primitives) to disk and network
 */
public class NativeEndianMemoryUtil extends MemoryUtil
{
    public static int getUnsignedShort(long address)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b1) == 0L)
            return unsafe.getShort(address) & 0xffff;
        else
            return getShortByByte(address) & 0xffff;
    }

    public static int getInt(long address)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b11) == 0L)
            return unsafe.getInt(address);
        else
            return getIntByByte(address);
    }

    public static long getLong(long address)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b111) == 0L)
            return unsafe.getLong(address);
        else
            return getLongByByte(address);
    }

    public static void setShort(long address, short s)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b1) == 0L)
            unsafe.putShort(address, s);
        else
            putShortByByte(address, s);
    }

    public static void setInt(long address, int l)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b11) == 0L)
            unsafe.putInt(address, l);
        else
            putIntByByte(address, l);
    }

    public static void setLong(long address, long l)
    {
        if (Architecture.IS_UNALIGNED || (address & 0b111) == 0L)
            unsafe.putLong(address, l);
        else
            putLongByByte(address, l);
    }

    @VisibleForTesting
    static long getLongByByte(long address)
    {
        if (Architecture.BIG_ENDIAN)
        {
            return  (((long) unsafe.getByte(address    )       ) << 56) |
                    (((long) unsafe.getByte(address + 1) & 0xff) << 48) |
                    (((long) unsafe.getByte(address + 2) & 0xff) << 40) |
                    (((long) unsafe.getByte(address + 3) & 0xff) << 32) |
                    (((long) unsafe.getByte(address + 4) & 0xff) << 24) |
                    (((long) unsafe.getByte(address + 5) & 0xff) << 16) |
                    (((long) unsafe.getByte(address + 6) & 0xff) <<  8) |
                    (((long) unsafe.getByte(address + 7) & 0xff)      );
        }
        else
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
    }

    @VisibleForTesting
    static int getIntByByte(long address)
    {
        if (Architecture.BIG_ENDIAN)
        {
            return  (((int) unsafe.getByte(address    )       ) << 24) |
                    (((int) unsafe.getByte(address + 1) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 2) & 0xff) <<  8) |
                    (((int) unsafe.getByte(address + 3) & 0xff)      );
        }
        else
        {
            return  (((int) unsafe.getByte(address + 3)       ) << 24) |
                    (((int) unsafe.getByte(address + 2) & 0xff) << 16) |
                    (((int) unsafe.getByte(address + 1) & 0xff) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    @VisibleForTesting
    static int getShortByByte(long address)
    {
        if (Architecture.BIG_ENDIAN)
        {
            return  (((int) unsafe.getByte(address    )       ) << 8) |
                    (((int) unsafe.getByte(address + 1) & 0xff)     );
        }
        else
        {
            return  (((int) unsafe.getByte(address + 1)       ) <<  8) |
                    (((int) unsafe.getByte(address    ) & 0xff)      );
        }
    }

    @VisibleForTesting
    static void putLongByByte(long address, long value)
    {
        if (Architecture.BIG_ENDIAN)
        {
            unsafe.putByte(address    , (byte) (value >> 56));
            unsafe.putByte(address + 1, (byte) (value >> 48));
            unsafe.putByte(address + 2, (byte) (value >> 40));
            unsafe.putByte(address + 3, (byte) (value >> 32));
            unsafe.putByte(address + 4, (byte) (value >> 24));
            unsafe.putByte(address + 5, (byte) (value >> 16));
            unsafe.putByte(address + 6, (byte) (value >>  8));
            unsafe.putByte(address + 7, (byte) (value      ));
        }
        else
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
    }

    @VisibleForTesting
    static void putIntByByte(long address, int value)
    {
        if (Architecture.BIG_ENDIAN)
        {
            unsafe.putByte(address    , (byte) (value >> 24));
            unsafe.putByte(address + 1, (byte) (value >> 16));
            unsafe.putByte(address + 2, (byte) (value >>  8));
            unsafe.putByte(address + 3, (byte) (value      ));
        }
        else
        {
            unsafe.putByte(address + 3, (byte) (value >> 24));
            unsafe.putByte(address + 2, (byte) (value >> 16));
            unsafe.putByte(address + 1, (byte) (value >>  8));
            unsafe.putByte(address    , (byte) (value      ));
        }
    }

    @VisibleForTesting
    static void putShortByByte(long address, short value)
    {
        if (Architecture.BIG_ENDIAN)
        {
            unsafe.putByte(address    , (byte) (value >> 8));
            unsafe.putByte(address + 1, (byte) (value     ));
        }
        else
        {
            unsafe.putByte(address + 1, (byte) (value >> 8));
            unsafe.putByte(address    , (byte) (value     ));
        }
    }

    public static ByteBuffer getByteBuffer(long address, int length)
    {
        return getByteBuffer(address, length, ByteOrder.nativeOrder());
    }

    public static ByteBuffer getHollowDirectByteBuffer()
    {
        return getHollowDirectByteBuffer(ByteOrder.nativeOrder());
    }
}
