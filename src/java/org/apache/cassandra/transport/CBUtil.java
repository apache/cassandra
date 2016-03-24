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
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ByteBuf utility methods.
 * Note that contrarily to ByteBufferUtil, these method do "read" the
 * ByteBuf advancing its (read) position. They also write by
 * advancing the write position. Functions are also provided to create
 * ByteBuf while avoiding copies.
 */
public abstract class CBUtil
{
    public static final boolean USE_HEAP_ALLOCATOR = Boolean.getBoolean(Config.PROPERTY_PREFIX + "netty_use_heap_allocator");
    public static final ByteBufAllocator allocator = USE_HEAP_ALLOCATOR ? new UnpooledByteBufAllocator(false) : new PooledByteBufAllocator(true);

    private final static FastThreadLocal<CharsetDecoder> TL_UTF8_DECODER = new FastThreadLocal<CharsetDecoder>()
    {
        @Override
        protected CharsetDecoder initialValue()
        {
            return Charset.forName("UTF-8").newDecoder();
        }
    };

    private final static FastThreadLocal<CharBuffer> TL_CHAR_BUFFER = new FastThreadLocal<>();

    private CBUtil() {}


    // Taken from Netty's ChannelBuffers.decodeString(). We need to use our own decoder to properly handle invalid
    // UTF-8 sequences.  See CASSANDRA-8101 for more details.  This can be removed once https://github.com/netty/netty/pull/2999
    // is resolved in a release used by Cassandra.
    private static String decodeString(ByteBuffer src) throws CharacterCodingException
    {
        // the decoder needs to be reset every time we use it, hence the copy per thread
        CharsetDecoder theDecoder = TL_UTF8_DECODER.get();
        theDecoder.reset();
        CharBuffer dst = TL_CHAR_BUFFER.get();
        int capacity = (int) ((double) src.remaining() * theDecoder.maxCharsPerByte());
        if (dst == null) {
            capacity = Math.max(capacity, 4096);
            dst = CharBuffer.allocate(capacity);
            TL_CHAR_BUFFER.set(dst);
        }
        else {
            dst.clear();
            if (dst.capacity() < capacity){
                dst = CharBuffer.allocate(capacity);
                TL_CHAR_BUFFER.set(dst);
            }
        }
        CoderResult cr = theDecoder.decode(src, dst, true);
        if (!cr.isUnderflow())
            cr.throwException();

        return dst.flip().toString();
    }

    private static String readString(ByteBuf cb, int length)
    {
        if (length == 0)
            return "";

        ByteBuffer buffer = cb.nioBuffer(cb.readerIndex(), length);
        try
        {
            String str = decodeString(buffer);
            cb.readerIndex(cb.readerIndex() + length);
            return str;
        }
        catch (IllegalStateException | CharacterCodingException e)
        {
            throw new ProtocolException("Cannot decode string as UTF8: '" + ByteBufferUtil.bytesToHex(buffer) + "'; " + e);
        }
    }

    public static String readString(ByteBuf cb)
    {
        try
        {
            int length = cb.readUnsignedShort();
            return readString(cb, length);
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read an UTF8 serialized string preceded by its 2 bytes length");
        }
    }

    public static void writeString(String str, ByteBuf cb)
    {
        int writerIndex = cb.writerIndex();
        cb.writeShort(0);
        int lengthBytes = ByteBufUtil.writeUtf8(cb, str);
        cb.setShort(writerIndex, lengthBytes);
    }

    public static int sizeOfString(String str)
    {
        return 2 + TypeSizes.encodedUTF8Length(str);
    }

    public static String readLongString(ByteBuf cb)
    {
        try
        {
            int length = cb.readInt();
            return readString(cb, length);
        }
        catch (IndexOutOfBoundsException e)
        {
            throw new ProtocolException("Not enough bytes to read an UTF8 serialized string preceded by its 4 bytes length");
        }
    }

    public static void writeLongString(String str, ByteBuf cb)
    {
        byte[] bytes = str.getBytes(CharsetUtil.UTF_8);
        cb.writeInt(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfLongString(String str)
    {
        return 4 + str.getBytes(CharsetUtil.UTF_8).length;
    }

    public static byte[] readBytes(ByteBuf cb)
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
            throw new ProtocolException("Not enough bytes to read a byte array preceded by its 2 bytes length");
        }
    }

    public static void writeBytes(byte[] bytes, ByteBuf cb)
    {
        cb.writeShort(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfBytes(byte[] bytes)
    {
        return 2 + bytes.length;
    }

    public static Map<String, ByteBuffer> readBytesMap(ByteBuf cb)
    {
        int length = cb.readUnsignedShort();
        Map<String, ByteBuffer> m = new HashMap<>(length);
        for (int i = 0; i < length; i++)
        {
            String k = readString(cb);
            ByteBuffer v = readValue(cb);
            m.put(k, v);
        }
        return m;
    }

    public static void writeBytesMap(Map<String, ByteBuffer> m, ByteBuf cb)
    {
        cb.writeShort(m.size());
        for (Map.Entry<String, ByteBuffer> entry : m.entrySet())
        {
            writeString(entry.getKey(), cb);
            writeValue(entry.getValue(), cb);
        }
    }

    public static int sizeOfBytesMap(Map<String, ByteBuffer> m)
    {
        int size = 2;
        for (Map.Entry<String, ByteBuffer> entry : m.entrySet())
        {
            size += sizeOfString(entry.getKey());
            size += sizeOfValue(entry.getValue());
        }
        return size;
    }

    public static ConsistencyLevel readConsistencyLevel(ByteBuf cb)
    {
        return ConsistencyLevel.fromCode(cb.readUnsignedShort());
    }

    public static void writeConsistencyLevel(ConsistencyLevel consistency, ByteBuf cb)
    {
        cb.writeShort(consistency.code);
    }

    public static int sizeOfConsistencyLevel(ConsistencyLevel consistency)
    {
        return 2;
    }

    public static <T extends Enum<T>> T readEnumValue(Class<T> enumType, ByteBuf cb)
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

    public static <T extends Enum<T>> void writeEnumValue(T enumValue, ByteBuf cb)
    {
        writeString(enumValue.toString(), cb);
    }

    public static <T extends Enum<T>> int sizeOfEnumValue(T enumValue)
    {
        return sizeOfString(enumValue.toString());
    }

    public static UUID readUUID(ByteBuf cb)
    {
        byte[] bytes = new byte[16];
        cb.readBytes(bytes);
        return UUIDGen.getUUID(ByteBuffer.wrap(bytes));
    }

    public static void writeUUID(UUID uuid, ByteBuf cb)
    {
        cb.writeBytes(UUIDGen.decompose(uuid));
    }

    public static int sizeOfUUID(UUID uuid)
    {
        return 16;
    }

    public static List<String> readStringList(ByteBuf cb)
    {
        int length = cb.readUnsignedShort();
        List<String> l = new ArrayList<String>(length);
        for (int i = 0; i < length; i++)
            l.add(readString(cb));
        return l;
    }

    public static void writeStringList(List<String> l, ByteBuf cb)
    {
        cb.writeShort(l.size());
        for (String str : l)
            writeString(str, cb);
    }

    public static int sizeOfStringList(List<String> l)
    {
        int size = 2;
        for (String str : l)
            size += sizeOfString(str);
        return size;
    }

    public static Map<String, String> readStringMap(ByteBuf cb)
    {
        int length = cb.readUnsignedShort();
        Map<String, String> m = new HashMap<String, String>(length);
        for (int i = 0; i < length; i++)
        {
            String k = readString(cb);
            String v = readString(cb);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringMap(Map<String, String> m, ByteBuf cb)
    {
        cb.writeShort(m.size());
        for (Map.Entry<String, String> entry : m.entrySet())
        {
            writeString(entry.getKey(), cb);
            writeString(entry.getValue(), cb);
        }
    }

    public static int sizeOfStringMap(Map<String, String> m)
    {
        int size = 2;
        for (Map.Entry<String, String> entry : m.entrySet())
        {
            size += sizeOfString(entry.getKey());
            size += sizeOfString(entry.getValue());
        }
        return size;
    }

    public static Map<String, List<String>> readStringToStringListMap(ByteBuf cb)
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

    public static void writeStringToStringListMap(Map<String, List<String>> m, ByteBuf cb)
    {
        cb.writeShort(m.size());
        for (Map.Entry<String, List<String>> entry : m.entrySet())
        {
            writeString(entry.getKey(), cb);
            writeStringList(entry.getValue(), cb);
        }
    }

    public static int sizeOfStringToStringListMap(Map<String, List<String>> m)
    {
        int size = 2;
        for (Map.Entry<String, List<String>> entry : m.entrySet())
        {
            size += sizeOfString(entry.getKey());
            size += sizeOfStringList(entry.getValue());
        }
        return size;
    }

    public static ByteBuffer readValue(ByteBuf cb)
    {
        int length = cb.readInt();
        if (length < 0)
            return null;
        ByteBuf slice = cb.readSlice(length);

        return ByteBuffer.wrap(readRawBytes(slice));
    }

    public static ByteBuffer readBoundValue(ByteBuf cb, int protocolVersion)
    {
        int length = cb.readInt();
        if (length < 0)
        {
            if (protocolVersion < 4) // backward compatibility for pre-version 4
                return null;
            if (length == -1)
                return null;
            else if (length == -2)
                return ByteBufferUtil.UNSET_BYTE_BUFFER;
            else
                throw new ProtocolException("Invalid ByteBuf length " + length);
        }
        ByteBuf slice = cb.readSlice(length);

        return ByteBuffer.wrap(readRawBytes(slice));
    }

    public static void writeValue(byte[] bytes, ByteBuf cb)
    {
        if (bytes == null)
        {
            cb.writeInt(-1);
            return;
        }

        cb.writeInt(bytes.length);
        cb.writeBytes(bytes);
    }

    public static void writeValue(ByteBuffer bytes, ByteBuf cb)
    {
        if (bytes == null)
        {
            cb.writeInt(-1);
            return;
        }

        int remaining = bytes.remaining();
        cb.writeInt(remaining);

        if (remaining > 0)
            cb.writeBytes(bytes.duplicate());
    }

    public static int sizeOfValue(byte[] bytes)
    {
        return 4 + (bytes == null ? 0 : bytes.length);
    }

    public static int sizeOfValue(ByteBuffer bytes)
    {
        return 4 + (bytes == null ? 0 : bytes.remaining());
    }

    // The size of serializing a value given the size (in bytes) of said value. The provided size can be negative
    // to indicate that the value is null.
    public static int sizeOfValue(int valueSize)
    {
        return 4 + (valueSize < 0 ? 0 : valueSize);
    }

    public static List<ByteBuffer> readValueList(ByteBuf cb, int protocolVersion)
    {
        int size = cb.readUnsignedShort();
        if (size == 0)
            return Collections.<ByteBuffer>emptyList();

        List<ByteBuffer> l = new ArrayList<ByteBuffer>(size);
        for (int i = 0; i < size; i++)
            l.add(readBoundValue(cb, protocolVersion));
        return l;
    }

    public static void writeValueList(List<ByteBuffer> values, ByteBuf cb)
    {
        cb.writeShort(values.size());
        for (ByteBuffer value : values)
            CBUtil.writeValue(value, cb);
    }

    public static int sizeOfValueList(List<ByteBuffer> values)
    {
        int size = 2;
        for (ByteBuffer value : values)
            size += CBUtil.sizeOfValue(value);
        return size;
    }

    public static Pair<List<String>, List<ByteBuffer>> readNameAndValueList(ByteBuf cb, int protocolVersion)
    {
        int size = cb.readUnsignedShort();
        if (size == 0)
            return Pair.create(Collections.<String>emptyList(), Collections.<ByteBuffer>emptyList());

        List<String> s = new ArrayList<>(size);
        List<ByteBuffer> l = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            s.add(readString(cb));
            l.add(readBoundValue(cb, protocolVersion));
        }
        return Pair.create(s, l);
    }

    public static InetSocketAddress readInet(ByteBuf cb)
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

    public static void writeInet(InetSocketAddress inet, ByteBuf cb)
    {
        byte[] address = inet.getAddress().getAddress();

        cb.writeByte(address.length);
        cb.writeBytes(address);
        cb.writeInt(inet.getPort());
    }

    public static int sizeOfInet(InetSocketAddress inet)
    {
        byte[] address = inet.getAddress().getAddress();
        return 1 + address.length + 4;
    }

    /*
     * Reads *all* readable bytes from {@code cb} and return them.
     */
    public static byte[] readRawBytes(ByteBuf cb)
    {
        byte[] bytes = new byte[cb.readableBytes()];
        cb.readBytes(bytes);
        return bytes;
    }

}
