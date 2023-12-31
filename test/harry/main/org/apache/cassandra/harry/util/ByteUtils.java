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

package org.apache.cassandra.harry.util;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ByteUtils
{
    public static ByteBuffer bytes(String s)
    {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuffer bytes(String s, Charset charset)
    {
        return ByteBuffer.wrap(s.getBytes(charset));
    }

    public static ByteBuffer bytes(byte b)
    {
        return ByteBuffer.allocate(1).put(0, b);
    }

    public static ByteBuffer bytes(short s)
    {
        return ByteBuffer.allocate(2).putShort(0, s);
    }

    public static ByteBuffer bytes(int i)
    {
        return ByteBuffer.allocate(4).putInt(0, i);
    }

    public static ByteBuffer bytes(long n)
    {
        return ByteBuffer.allocate(8).putLong(0, n);
    }

    public static ByteBuffer bytes(float f)
    {
        return ByteBuffer.allocate(4).putFloat(0, f);
    }

    public static ByteBuffer bytes(double d)
    {
        return ByteBuffer.allocate(8).putDouble(0, d);
    }

    public static ByteBuffer bytes(InetAddress address)
    {
        return ByteBuffer.wrap(address.getAddress());
    }

    public static ByteBuffer bytes(UUID uuid)
    {
        return ByteBuffer.wrap(decompose(uuid));
    }

    public static byte[] decompose(UUID uuid)
    {
        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();
        byte[] b = new byte[16];
        for (int i = 0; i < 8; i++)
        {
            b[i] = (byte)(most >>> ((7-i) * 8));
            b[8+i] = (byte)(least >>> ((7-i) * 8));
        }
        return b;
    }

    public static ByteBuffer[] objectsToBytes(Object... objects)
    {
        ByteBuffer[] bytes = new ByteBuffer[objects.length];
        for (int i = 0; i < objects.length; i++)
            bytes[i] = objectToBytes(objects[i]);
        return bytes;
    }

    public static ByteBuffer objectToBytes(Object obj)
    {
        if (obj instanceof Integer)
            return bytes((int) obj);
        else if (obj instanceof Byte)
            return bytes((byte) obj);
        else if (obj instanceof Boolean)
            return bytes( (byte) (((boolean) obj) ? 1 : 0));
        else if (obj instanceof Short)
            return bytes((short) obj);
        else if (obj instanceof Long)
            return bytes((long) obj);
        else if (obj instanceof Float)
            return bytes((float) obj);
        else if (obj instanceof Double)
            return bytes((double) obj);
        else if (obj instanceof UUID)
            return bytes((UUID) obj);
        else if (obj instanceof InetAddress)
            return bytes((InetAddress) obj);
        else if (obj instanceof String)
            return bytes((String) obj);
        else if (obj instanceof List)
        {
            throw new UnsupportedOperationException("Please use ByteUtils from integration package");
        }
        else if (obj instanceof Set)
        {
            throw new UnsupportedOperationException("Please use ByteUtils from integration package");
        }
        else if (obj instanceof ByteBuffer)
            return (ByteBuffer) obj;
        else
            throw new IllegalArgumentException(String.format("Cannot convert value %s of type %s",
                                                             obj,
                                                             obj.getClass()));
    }

    public static ByteBuffer compose(ByteBuffer... buffers) {
        if (buffers.length == 1) return buffers[0];

        int totalLength = 0;
        for (ByteBuffer bb : buffers) totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers) {
            ByteBuffer bb = buffer.duplicate();
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    public static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

}
