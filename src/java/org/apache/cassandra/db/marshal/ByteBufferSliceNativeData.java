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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Temporary created object as a part of slicing (usually to parse collection parts)
 */
public class ByteBufferSliceNativeData implements NativeData
{
    public static final ByteBufferSliceNativeData EMPTY = new ByteBufferSliceNativeData(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    private final ByteBuffer byteBuffer;

    public ByteBufferSliceNativeData(ByteBuffer byteBuffer)
    {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int nativeDataSize()
    {
        return byteBuffer.remaining();
    }

    @Override
    public ByteBuffer asByteBuffer()
    {
        return byteBuffer;
    }

    @Override
    public NativeData slice(int offset, int length)
    {
        return new ByteBufferSliceNativeData(ByteBufferAccessor.instance.slice(byteBuffer, offset, length));
    }

    @Override
    public void writeTo(DataOutputPlus out) throws IOException
    {
        out.write(byteBuffer);
    }
}
