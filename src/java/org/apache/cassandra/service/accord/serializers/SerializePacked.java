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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.utils.BitUtils;
import accord.utils.Invariants;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A set of simple utilities to quickly serialize/deserialize arrays/lists of values that each require <= 64 bits to represent.
 * These are packed into an "array" of fixed bit width, so that the total size consumed is ceil((bits*elements)/8).
 * This can (in future) be read directly without deserialization, by indexing into the byte stream directly.
 */
public class SerializePacked
{
    public static void serializePackedInts(int[] vs, int from, int to, int max, DataOutputPlus out) throws IOException
    {
        serializePacked((in, i) -> in[i], vs, from, to, max, out);
    }

    public static void deserializePackedInts(int[] vs, int from, int to, int max, DataInputPlus in) throws IOException
    {
        deserializePacked((out, i, v) -> out[i] = (int)v, vs, from, to, max, in);
    }

    public static long serializedPackedIntsSize(int[] vs, int from, int to, int max)
    {
        return serializedPackedSize(to - from, max);
    }

    public interface SerializeAdapter<In>
    {
        long get(In in, int i);
    }

    @Inline
    public static <In> void serializePacked(SerializeAdapter<In> adapter, In in, int from, int to, long max, DataOutputPlus out) throws IOException
    {
        int bitsPerEntry = BitUtils.numberOfBitsToRepresent(max);
        if (bitsPerEntry == 0)
            return;

        long buffer = 0L;
        int bufferCount = 0;
        for (int i = from; i < to; i++)
        {
            long v = adapter.get(in, i);
            Invariants.require(v <= max);
            buffer |= v << bufferCount;
            bufferCount = bufferCount + bitsPerEntry;
            if (bufferCount >= 64)
            {
                out.writeLong(buffer);
                bufferCount -= 64;
                buffer = v >>> (bitsPerEntry - bufferCount);
            }
        }
        if (bufferCount > 0)
            out.writeLeastSignificantBytes(buffer, (bufferCount + 7) / 8);
    }

    public interface DeserializeAdapter<Out>
    {
        void accept(Out out, int i, long v);
    }

    @Inline
    public static <Out> void deserializePacked(DeserializeAdapter<Out> consumer, Out out, int from, int to, long max, DataInputPlus in) throws IOException
    {
        int bitsPerEntry = BitUtils.numberOfBitsToRepresent(max);
        if (bitsPerEntry == 0)
        {
            for (int i = from; i < to ; ++i)
                consumer.accept(out, i, 0);
            return;
        }
        long mask = -1L >>> (64 - bitsPerEntry);
        int remainingBytes = (bitsPerEntry * (to - from) + 7) / 8;
        long buffer = 0L;
        int bufferCount = 0;
        for (int i = from; i < to; i++)
        {
            long v = buffer & mask;
            if (bufferCount >= bitsPerEntry)
            {
                bufferCount -= bitsPerEntry;
                buffer >>>= bitsPerEntry;
            }
            else
            {
                int newBufferCount;
                if (remainingBytes >= 8)
                {
                    buffer = in.readLong();
                    newBufferCount = 64;
                    remainingBytes -= 8;
                }
                else
                {
                    Invariants.require(remainingBytes > 0);
                    newBufferCount = remainingBytes * 8;
                    buffer = in.readLeastSignificantBytes(remainingBytes);
                    remainingBytes = 0;
                }
                int readExtra = bitsPerEntry - bufferCount;
                long extraBits = buffer & (mask >>> bufferCount);
                v |= extraBits << bufferCount;
                bufferCount = newBufferCount - readExtra;
                buffer >>>= readExtra;
            }
            Invariants.require(v <= max);
            consumer.accept(out, i, v);
        }
    }

    public static long serializedPackedSize(int count, long max)
    {
        return serializedPackedBitsSize(count, BitUtils.numberOfBitsToRepresent(max));
    }

    public static long serializedPackedBitsSize(int count, int bitsPerEntry)
    {
        return ((long)bitsPerEntry * count + 7)/8;
    }
}
