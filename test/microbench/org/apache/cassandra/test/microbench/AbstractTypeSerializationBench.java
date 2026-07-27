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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Measures value serialization when one call site sees the fixed- and variable-width types commonly used by a table.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = { "-Xmx1G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@Threads(1)
@State(Scope.Thread)
public class AbstractTypeSerializationBench
{
    private final AbstractType<?>[] types = { Int32Type.instance, LongType.instance, BooleanType.instance, UUIDType.instance,
                                              UTF8Type.instance, Int32Type.instance, LongType.instance, UTF8Type.instance };
    private final ByteBuffer[] values = { ByteBufferUtil.bytes(42), ByteBufferUtil.bytes(42L), ByteBuffer.wrap(new byte[] { 1 }),
                                          ByteBuffer.wrap(new byte[16]), ByteBufferUtil.bytes("cassandra"), ByteBufferUtil.bytes(42),
                                          ByteBufferUtil.bytes(42L), ByteBufferUtil.bytes("cassandra") };
    private final ByteBuffer[] serializedValues = new ByteBuffer[types.length];
    private final DataOutputBuffer out = new DataOutputBuffer();
    private int index;

    @Setup
    public void setup() throws IOException
    {
        for (int i = 0; i < types.length; i++)
        {
            out.clear();
            types[i].writeValue(values[i], out);
            serializedValues[i] = out.asNewBuffer();
        }
    }

    @Benchmark
    public int writeValue() throws IOException
    {
        int i = index++ & (types.length - 1);
        out.clear();
        types[i].writeValue(values[i], out);
        return out.getLength();
    }

    @Benchmark
    public ByteBuffer readValue() throws IOException
    {
        int i = index++ & (types.length - 1);
        return types[i].readBuffer(new DataInputBuffer(serializedValues[i], true));
    }
}
