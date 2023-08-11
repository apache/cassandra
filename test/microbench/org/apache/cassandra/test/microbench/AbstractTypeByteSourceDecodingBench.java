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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1,jvmArgsAppend = { "-Xmx4G", "-Xms4G", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(1)
@State(Scope.Benchmark)
public class AbstractTypeByteSourceDecodingBench
{

    private static final ByteComparable.Version LATEST = ByteComparable.Version.OSS50;

    private static final Map<AbstractType, BiFunction<Random, Integer, ByteSource.Peekable>> PEEKABLE_GENERATOR_BY_TYPE = new HashMap<>();
    static
    {
        PEEKABLE_GENERATOR_BY_TYPE.put(UTF8Type.instance, (prng, length) ->
        {
            byte[] randomBytes = new byte[length];
            prng.nextBytes(randomBytes);
            return ByteSource.peekable(ByteSource.of(new String(randomBytes, StandardCharsets.UTF_8), LATEST));
        });
        PEEKABLE_GENERATOR_BY_TYPE.put(BytesType.instance, (prng, length) ->
        {
            byte[] randomBytes = new byte[length];
            prng.nextBytes(randomBytes);
            return ByteSource.peekable(ByteSource.of(randomBytes, LATEST));
        });
        PEEKABLE_GENERATOR_BY_TYPE.put(IntegerType.instance, (prng, length) ->
        {
            BigInteger randomVarint = BigInteger.valueOf(prng.nextLong());
            for (int i = 1; i < length / 8; ++i)
                randomVarint = randomVarint.multiply(BigInteger.valueOf(prng.nextLong()));
            return ByteSource.peekable(IntegerType.instance.asComparableBytes(IntegerType.instance.decompose(randomVarint), LATEST));
        });
        PEEKABLE_GENERATOR_BY_TYPE.put(DecimalType.instance, (prng, length) ->
        {
            BigInteger randomMantissa = BigInteger.valueOf(prng.nextLong());
            for (int i = 1; i < length / 8; ++i)
                randomMantissa = randomMantissa.multiply(BigInteger.valueOf(prng.nextLong()));
            int randomScale = prng.nextInt(Integer.MAX_VALUE >> 1) + Integer.MAX_VALUE >> 1;
            BigDecimal randomDecimal = new BigDecimal(randomMantissa, randomScale);
            return ByteSource.peekable(DecimalType.instance.asComparableBytes(DecimalType.instance.decompose(randomDecimal), LATEST));
        });
    }

    private Random prng = new Random();

    @Param({"32", "128", "512"})
    private int length;

    @Param({"UTF8Type", "BytesType", "IntegerType", "DecimalType"})
    private String abstractTypeName;

    private AbstractType abstractType;
    private BiFunction<Random, Integer, ByteSource.Peekable> peekableGenerator;

    @Setup(Level.Trial)
    public void setup()
    {
        abstractType = TypeParser.parse(abstractTypeName);
        peekableGenerator = PEEKABLE_GENERATOR_BY_TYPE.get(abstractType);
    }

    @Inline
    private ByteSource.Peekable randomPeekableBytes()
    {
        return peekableGenerator.apply(prng, length);
    }

    @Benchmark
    public int baseline()
    {
        // Getting the source is not enough as its content is produced on next() calls.
        ByteSource.Peekable source = randomPeekableBytes();
        int count = 0;
        while (source.next() != ByteSource.END_OF_STREAM)
            ++count;
        return count;
    }

    @Benchmark
    public ByteBuffer fromComparableBytes()
    {
        ByteSource.Peekable peekableBytes = randomPeekableBytes();
        return abstractType.fromComparableBytes(peekableBytes, ByteComparable.Version.OSS50);
    }
}
