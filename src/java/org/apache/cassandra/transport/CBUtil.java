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
package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ChannelBuffer utility methods.
 * Note that contrarily to ByteBufferUtil, these method do "read" the
 * ChannelBuffer advancing it's (read) position. They also write by
 * advancing the write position. Functions are also provided to create
 * ChannelBuffer while avoiding copies.
 */
public abstract class CBUtil
{
    private CBUtil() {}

    public static String readString(ChannelBuffer cb)
    {
        try
        {
            int length = cb.readUnsignedShort();
            return readString(cb, length);
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read an UTF8 serialized string preceded by it's 2 bytes length");
        }
    }

    public static String readLongString(ChannelBuffer cb)
    {
        try
        {
            int length = cb.readInt();
            return readString(cb, length);
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read an UTF8 serialized string preceded by it's 4 bytes length");
        }
    }

    private static String readString(ChannelBuffer cb, int length)
    {
        try
        {
            String str = cb.toString(cb.readerIndex(), length, CharsetUtil.UTF_8);
            cb.readerIndex(cb.readerIndex() + length);
            return str;
        }
        catch (IllegalStateException e)
        {
            // That's the way netty encapsulate a CCE
            if (e.getCause() instanceof CharacterCodingException)
                throw new ProtocolException("Cannot decode string as UTF8");
            else
                throw e;
        }
    }

    private static ChannelBuffer bytes(String str)
    {
        return ChannelBuffers.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8));
    }

    public static ChannelBuffer shortToCB(int s)
    {
        ChannelBuffer cb = ChannelBuffers.buffer(2);
        cb.writeShort(s);
        return cb;
    }

    public static ChannelBuffer intToCB(int i)
    {
        ChannelBuffer cb = ChannelBuffers.buffer(4);
        cb.writeInt(i);
        return cb;
    }

    public static ChannelBuffer stringToCB(String str)
    {
        ChannelBuffer bytes = bytes(str);
        return ChannelBuffers.wrappedBuffer(shortToCB(bytes.readableBytes()), bytes);
    }

    public static ChannelBuffer bytesToCB(byte[] bytes)
    {
        return ChannelBuffers.wrappedBuffer(shortToCB(bytes.length), ChannelBuffers.wrappedBuffer(bytes));
    }

    public static byte[] readBytes(ChannelBuffer cb)
    {
        try
        {
            int length = cb.readUnsignedShort();
            byte[] bytes = new byte[length];
            cb.readBytes(bytes);
            return bytes;
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read a byte array preceded by it's 2 bytes length");
        }
    }

    public static ChannelBuffer consistencyLevelToCB(ConsistencyLevel consistency)
    {
        return shortToCB(consistency.code);
    }

    public static ConsistencyLevel readConsistencyLevel(ChannelBuffer cb)
    {
        return ConsistencyLevel.fromCode(cb.readUnsignedShort());
    }

    public static <T extends Enum<T>> T readEnumValue(Class<T> enumType, ChannelBuffer cb)
    {
        String value = CBUtil.readString(cb);
        try
        {
            return Enum.valueOf(enumType, value.toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            throw new ProtocolException(String.format("Invalid value '%s' for %s", value, enumType.getSimpleName()));
        }
    }

    public static <T extends Enum<T>> ChannelBuffer enumValueToCB(T enumValue)
    {
        return stringToCB(enumValue.toString());
    }

    public static ChannelBuffer uuidToCB(UUID uuid)
    {
        return ChannelBuffers.wrappedBuffer(UUIDGen.decompose(uuid));
    }

    public static UUID readUuid(ChannelBuffer cb)
    {
        byte[] bytes = new byte[16];
        cb.readBytes(bytes);
        return UUIDGen.getUUID(ByteBuffer.wrap(bytes));
    }

    public static ChannelBuffer longStringToCB(String str)
    {
        ChannelBuffer bytes = bytes(str);
        return ChannelBuffers.wrappedBuffer(intToCB(bytes.readableBytes()), bytes);
    }

    public static List<String> readStringList(ChannelBuffer cb)
    {
        int length = cb.readUnsignedShort();
        List<String> l = new ArrayList<String>(length);
        for (int i = 0; i < length; i++)
            l.add(readString(cb));
        return l;
    }

    public static void writeStringList(ChannelBuffer cb, List<String> l)
    {
        cb.writeShort(l.size());
        for (String str : l)
            cb.writeBytes(stringToCB(str));
    }

    public static Map<String, String> readStringMap(ChannelBuffer cb)
    {
        int length = cb.readUnsignedShort();
        Map<String, String> m = new HashMap<String, String>(length);
        for (int i = 0; i < length; i++)
        {
            String k = readString(cb).toUpperCase();
            String v = readString(cb);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringMap(ChannelBuffer cb, Map<String, String> m)
    {
        cb.writeShort(m.size());
        for (Map.Entry<String, String> entry : m.entrySet())
        {
            cb.writeBytes(stringToCB(entry.getKey()));
            cb.writeBytes(stringToCB(entry.getValue()));
        }
    }

    public static Map<String, List<String>> readStringToStringListMap(ChannelBuffer cb)
    {
        int length = cb.readUnsignedShort();
        Map<String, List<String>> m = new HashMap<String, List<String>>(length);
        for (int i = 0; i < length; i++)
        {
            String k = readString(cb).toUpperCase();
            List<String> v = readStringList(cb);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringToStringListMap(ChannelBuffer cb, Map<String, List<String>> m)
    {
        cb.writeShort(m.size());
        for (Map.Entry<String, List<String>> entry : m.entrySet())
        {
            cb.writeBytes(stringToCB(entry.getKey()));
            writeStringList(cb, entry.getValue());
        }
    }

    public static ByteBuffer readValue(ChannelBuffer cb)
    {
        int length = cb.readInt();
        return length < 0 ? null : cb.readSlice(length).toByteBuffer();
    }

    public static InetSocketAddress readInet(ChannelBuffer cb)
    {
        int addrSize = cb.readByte();
        byte[] address = new byte[addrSize];
        cb.readBytes(address);
        int port = cb.readInt();
        try
        {
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        }
        catch (UnknownHostException e)
        {
            throw new ProtocolException(String.format("Invalid IP address (%d.%d.%d.%d) while deserializing inet address", address[0], address[1], address[2], address[3]));
        }
    }

    public static ChannelBuffer inetToCB(InetSocketAddress inet)
    {
        byte[] address = inet.getAddress().getAddress();
        ChannelBuffer cb = ChannelBuffers.buffer(1 + address.length + 4);
        cb.writeByte(address.length);
        cb.writeBytes(address);
        cb.writeInt(inet.getPort());
        return cb;
    }

    public static class BufferBuilder
    {
        private final int size;
        private final ChannelBuffer[] buffers;
        private int i;

        public BufferBuilder(int simpleBuffers, int stringBuffers, int valueBuffers)
        {
            this.size = simpleBuffers + 2 * stringBuffers + 2 * valueBuffers;
            this.buffers = new ChannelBuffer[size];
        }

        public BufferBuilder add(ChannelBuffer cb)
        {
            buffers[i++] = cb;
            return this;
        }

        public BufferBuilder addString(String str)
        {
            ChannelBuffer bytes = bytes(str);
            add(shortToCB(bytes.readableBytes()));
            return add(bytes);
        }

        public BufferBuilder addValue(ByteBuffer bb)
        {
            add(intToCB(bb == null ? -1 : bb.remaining()));
            return add(bb == null ? ChannelBuffers.EMPTY_BUFFER : ChannelBuffers.wrappedBuffer(bb));
        }

        public ChannelBuffer build()
        {
            return ChannelBuffers.wrappedBuffer(buffers);
        }
    }
}
