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

package org.apache.cassandra.transport.frame.checksum;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Wrapper around byte[] with length which is expected to be reused with different data.  It is expected that the bytes
 * are directly modified and the length gets updated to reflect; this is done to avoid producing unneeded garbage.
 *
 * This class is not thread safe.
 */
public final class ReusableBuffer
{
    public final byte[] bytes;
    public int length;

    public ReusableBuffer(byte[] bytes)
    {
        this.bytes = bytes;
        this.length = bytes.length;
    }

    public ByteBuf toByteBuf() {
        return Unpooled.wrappedBuffer(bytes, 0, length);
    }

    public String toString()
    {
        return ByteBufferUtil.bytesToHex(ByteBuffer.wrap(bytes, 0, length));
    }
}
