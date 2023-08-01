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

package org.apache.cassandra.net;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;
import sun.misc.Unsafe;

import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

abstract class MessageGenerator
{
    final long seed;
    final Random random;

    private MessageGenerator(long seed)
    {
        this.seed = seed;
        this.random = new Random();
    }

    Message.Builder<Object> builder(long id)
    {
        random.setSeed(id ^ seed);
        long now = approxTime.now();

        int expiresInMillis;
        int expiryMask = random.nextInt();
        if (0 == (expiryMask & 0xffff)) expiresInMillis = 2;
        else if (0 == (expiryMask & 0xfff)) expiresInMillis = 10;
        else if (0 == (expiryMask & 0xff)) expiresInMillis = 100;
        else if (0 == (expiryMask & 0xf)) expiresInMillis = 1000;
        else expiresInMillis = 60 * 1000;

        long expiresInNanos = TimeUnit.MILLISECONDS.toNanos((expiresInMillis / 2) + random.nextInt(expiresInMillis / 2));

        return Message.builder(Verb._TEST_2, null)
                      .withId(id)
                      .withCreatedAt(now)
                      .withExpiresAt(now + expiresInNanos); // don't expire for now
    }

    public int uniformInt(int limit)
    {
        return random.nextInt(limit);
    }

    // generate a Message<?> with the provided id and with both id and info encoded in its payload
    abstract Message<?> generate(long id, byte info);
    abstract MessageGenerator copy();

    static final class UniformPayloadGenerator extends MessageGenerator
    {
        final int minSize;
        final int maxSize;
        final byte[] fillWithBytes;
        UniformPayloadGenerator(long seed, int minSize, int maxSize)
        {
            super(seed);
            this.minSize = Math.max(9, minSize);
            this.maxSize = Math.max(9, maxSize);
            this.fillWithBytes = new byte[32];
            random.setSeed(seed);
            random.nextBytes(fillWithBytes);
        }

        Message<?> generate(long id, byte info)
        {
            Message.Builder<Object> builder = builder(id);
            byte[] payload = new byte[minSize + random.nextInt(maxSize - minSize)];
            ByteBuffer wrapped = ByteBuffer.wrap(payload);
            setId(payload, id);
            payload[8] = info;
            wrapped.position(9);
            while (wrapped.hasRemaining())
                wrapped.put(fillWithBytes, 0, Math.min(fillWithBytes.length, wrapped.remaining()));
            builder.withPayload(payload);
            return builder.build();
        }

        MessageGenerator copy()
        {
            return new UniformPayloadGenerator(seed, minSize, maxSize);
        }
    }

    static long getId(byte[] payload)
    {
        return unsafe.getLong(payload, BYTE_ARRAY_BASE_OFFSET);
    }
    static byte getInfo(byte[] payload)
    {
        return payload[8];
    }
    private static void setId(byte[] payload, long id)
    {
        unsafe.putLong(payload, BYTE_ARRAY_BASE_OFFSET, id);
    }

    static class Header
    {
        public final int length;
        public final long id;
        public final byte info;

        Header(int length, long id, byte info)
        {
            this.length = length;
            this.id = id;
            this.info = info;
        }

        public byte[] read(DataInputPlus in, int length, int messagingVersion) throws IOException
        {
            byte[] result = new byte[Math.max(9, length)];
            setId(result, id);
            result[8] = info;
            in.readFully(result, 9, Math.max(0, length - 9));
            return result;
        }
    }

    static Header readHeader(DataInputPlus in, int messagingVersion) throws IOException
    {
        assert messagingVersion >= VERSION_40;
        int length = in.readUnsignedVInt32();
        long id = in.readLong();
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN)
            id = Long.reverseBytes(id);
        byte info = in.readByte();
        return new Header(length, id, info);
    }

    static void writeLength(byte[] payload, DataOutputPlus out, int messagingVersion) throws IOException
    {
        assert messagingVersion >= VERSION_40;
        out.writeUnsignedVInt32(payload.length);
    }

    static long serializedSize(byte[] payload)
    {
        return payload.length + VIntCoding.computeUnsignedVIntSize(payload.length);
    }

    private static final Unsafe unsafe;
    static
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }
    private static final long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

}

