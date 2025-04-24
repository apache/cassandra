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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.cassandra.utils.memory.MemoryUtil;

public class AddressBasedNativeData implements NativeData
{
    // use a real address, just in case
    private static final ByteBuffer EMPTY_NATIVE_BUFFER = ByteBuffer.allocateDirect(1);
    private static final long EMPTY_VALUE_ADDRESS = MemoryUtil.getAddress(EMPTY_NATIVE_BUFFER);
    public static final AddressBasedNativeData EMPTY = new AddressBasedNativeData(EMPTY_VALUE_ADDRESS, 0);

    private final long address;
    private final int length;

    public AddressBasedNativeData(long address, int length)
    {
        this.address = address;
        this.length = length;
    }


    @Override
    public int nativeDataSize()
    {
        return length;
    }

    @Override
    public ByteBuffer asByteBuffer()
    {
        return MemoryUtil.getByteBuffer(address, length, ByteOrder.BIG_ENDIAN);
    }

    @Override
    public NativeData slice(int offset, int length)
    {
        if (offset < 0 || offset > this.length)
            throw new IllegalArgumentException("offset must but be >= 0 and < parent length; " +
                                               "offset: " + offset +
                                               ", slice length: " + length +
                                               ", data length: " + this.length);
        if (length < 0 || offset + length > this.length) {
            throw new IllegalArgumentException("length must but be >= 0 and offset + length > parent length; " +
                                               "offset: " + offset +
                                               ", slice length: " + length +
                                               ", data length: " + this.length);
        }

        if (length == 0) {
            return EMPTY;
        }
        return new AddressBasedNativeData(address + offset, length);
    }

    @Override
    public long getAddress()
    {
        return address;
    }
}
