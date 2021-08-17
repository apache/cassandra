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

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = "-Xmx512M")
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.SampleTime)
public class PreaggregatedByteBufsBench
{
    @Param({ "1000", "2500", "5000", "10000", "20000", "40000"})
    private int len;

    private static final int subBufferCount = 64;

    private EmbeddedChannel channel;

    @Setup
    public void setUp()
    {
        channel = new EmbeddedChannel();
    }

    @Benchmark
    public boolean oneBigBuf()
    {
        boolean success = true;
        try
        {
            ByteBuf buf = channel.alloc().directBuffer(len);
            buf.writerIndex(len);
            channel.writeAndFlush(buf);
        }
        catch (Exception e)
        {
            success = false;
        }
        finally
        {
            channel.releaseOutbound();
        }

        return success;
    }

    @Benchmark
    public boolean chunkedBuf()
    {
        boolean success = true;
        try
        {
            int chunkLen = len / subBufferCount;

            for (int i = 0; i < subBufferCount; i++)
            {
                ByteBuf buf = channel.alloc().directBuffer(chunkLen);
                buf.writerIndex(chunkLen);
                channel.write(buf);
            }
            channel.flush();
        }
        catch (Exception e)
        {
            success = false;
        }
        finally
        {
            channel.releaseOutbound();
        }

        return success;
    }
}
