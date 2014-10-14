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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.ByteBufferUtil;

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

    private final static ThreadLocal<CharsetDecoder> decoder = new ThreadLocal<CharsetDecoder>()
    {
        @Override
        protected CharsetDecoder initialValue()
        {
            return Charset.forName("UTF-8").newDecoder();
        }
    };

    private static String readString(ChannelBuffer cb, int length)
    {
        if (length == 0)
            return "";

        ByteBuffer buffer = cb.toByteBuffer(cb.readerIndex(), length);
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

    // Taken from Netty's ChannelBuffers.decodeString(). We need to use our own decoder to properly handle invalid
    // UTF-8 sequences.  See CASSANDRA-8101 for more details.  This can be removed once https://github.com/netty/netty/pull/2999
    // is resolved in a release used by Cassandra.
    private static String decodeString(ByteBuffer src) throws CharacterCodingException
    {
        // the decoder needs to be reset every time we use it, hence the copy per thread
        CharsetDecoder theDecoder = decoder.get();
        theDecoder.reset();

        final CharBuffer dst = CharBuffer.allocate(
                (int) ((double) src.remaining() * theDecoder.maxCharsPerByte()));

        CoderResult cr = theDecoder.decode(src, dst, true);
        if (!cr.isUnderflow())
            cr.throwException();

        cr = theDecoder.flush(dst);
        if (!cr.isUnderflow())
            cr.throwException();

        return dst.flip().toString();
    }

    public static void writeString(String str, ChannelBuffer cb)
    {
        byte[] bytes = str.getBytes(CharsetUtil.UTF_8);
        cb.writeShort(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfString(String str)
    {
        return 2 + TypeSizes.encodedUTF8Length(str);
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

    public static void writeLongString(String str, ChannelBuffer cb)
    {
        byte[] bytes = str.getBytes(CharsetUtil.UTF_8);
        cb.writeInt(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfLongString(String str)
    {
        return 4 + str.getBytes(CharsetUtil.UTF_8).length;
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

    public static void writeBytes(byte[] bytes, ChannelBuffer cb)
    {
        cb.writeShort(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfBytes(byte[] bytes)
    {
        return 2 + bytes.length;
    }

    public static ConsistencyLevel readConsistencyLevel(ChannelBuffer cb)
    {
        return ConsistencyLevel.fromCode(cb.readUnsignedShort());
    }

    public static void writeConsistencyLevel(ConsistencyLevel consistency, ChannelBuffer cb)
    {
        cb.writeShort(consistency.code);
    }

    public static int sizeOfConsistencyLevel(ConsistencyLevel consistency)
    {
        return 2;
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

    public static <T extends Enum<T>> void writeEnumValue(T enumValue, ChannelBuffer cb)
    {
        writeString(enumValue.toString(), cb);
    }

    public static <T extends Enum<T>> int sizeOfEnumValue(T enumValue)
    {
        return sizeOfString(enumValue.toString());
    }

    public static UUID readUUID(ChannelBuffer cb)
    {
        byte[] bytes = new byte[16];
        cb.readBytes(bytes);
        return UUIDGen.getUUID(ByteBuffer.wrap(bytes));
    }

    public static void writeUUID(UUID uuid, ChannelBuffer cb)
    {
        cb.writeBytes(UUIDGen.decompose(uuid));
    }

    public static int sizeOfUUID(UUID uuid)
    {
        return 16;
    }

    public static List<String> readStringList(ChannelBuffer cb)
    {
        int length = cb.readUnsignedShort();
        List<String> l = new ArrayList<String>(length);
        for (int i = 0; i < length; i++)
            l.add(readString(cb));
        return l;
    }

    public static void writeStringList(List<String> l, ChannelBuffer cb)
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

    public static Map<String, String> readStringMap(ChannelBuffer cb)
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

    public static void writeStringMap(Map<String, String> m, ChannelBuffer cb)
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

    public static void writeStringToStringListMap(Map<String, List<String>> m, ChannelBuffer cb)
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

    public static ByteBuffer readValue(ChannelBuffer cb)
    {
        int length = cb.readInt();
        return length < 0 ? null : cb.readSlice(length).toByteBuffer();
    }

    public static void writeValue(byte[] bytes, ChannelBuffer cb)
    {
        if (bytes == null)
        {
            cb.writeInt(-1);
            return;
        }

        cb.writeInt(bytes.length);
        cb.writeBytes(bytes);
    }

    public static void writeValue(ByteBuffer bytes, ChannelBuffer cb)
    {
        if (bytes == null)
        {
            cb.writeInt(-1);
            return;
        }

        cb.writeInt(bytes.remaining());
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

    public static List<ByteBuffer> readValueList(ChannelBuffer cb)
    {
        int size = cb.readUnsignedShort();
        if (size == 0)
            return Collections.<ByteBuffer>emptyList();

        List<ByteBuffer> l = new ArrayList<ByteBuffer>(size);
        for (int i = 0; i < size; i++)
            l.add(readValue(cb));
        return l;
    }

    public static void writeValueList(List<ByteBuffer> values, ChannelBuffer cb)
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

    public static void writeInet(InetSocketAddress inet, ChannelBuffer cb)
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
     * If {@code cb} is backed by an array, this will return the underlying array directly, without copy.
     */
    public static byte[] readRawBytes(ChannelBuffer cb)
    {
        if (cb.hasArray() && cb.readableBytes() == cb.array().length)
        {
            // Move the readerIndex just so we consistenly consume the input
            cb.readerIndex(cb.writerIndex());
            return cb.array();
        }

        // Otherwise, just read the bytes in a new array
        byte[] bytes = new byte[cb.readableBytes()];
        cb.readBytes(bytes);
        return bytes;
    }
}
