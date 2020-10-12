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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

public class ValueGenerator
{
    public static final ValueGenerator INTEGER = new ValueGenerator(Int32Type.instance, Random::nextInt);
    public static final ValueGenerator BLOB = new ValueGenerator(BytesType.instance, ValueGenerator::randomBytes);
    public static final ValueGenerator TEXT = new ValueGenerator(UTF8Type.instance, ValueGenerator::randomString);
    public static final List<ValueGenerator> GENERATORS = ImmutableList.of(INTEGER, BLOB);

    private final AbstractType type;
    private final Function<Random, Object> generator;

    public static ByteBuffer randomBytes(Random random)
    {
        byte[] bytes = new byte[random.nextInt(100)];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static final char[] CHARS = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    public static String randomString(Random random)
    {
        char[] chars = new char[random.nextInt(100)];
        for (int i=0; i<chars.length; i++)
            chars[i] = CHARS[random.nextInt(CHARS.length)];
        return new String(chars);
    }

    private ValueGenerator(AbstractType<?> type, Function<Random, Object> generator)
    {
        this.type = type;
        this.generator = generator;
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    public Object nextValue(Random random)
    {
        return generator.apply(random);
    }

    public static ValueGenerator randomGenerator(Random random)
    {
        return GENERATORS.get(random.nextInt(GENERATORS.size()));
    }

    public static List<AbstractType<?>> toTypes(ValueGenerator[] generators)
    {
        List<AbstractType<?>> types = new ArrayList<>(generators.length);
        for (ValueGenerator generator : generators)
            types.add(generator.getType());
        return types;
    }

    public static ValueGenerator[] randomGenerators(Random random, int maxSize)
    {
        int size = random.nextInt(maxSize);
        ValueGenerator[] generators = new ValueGenerator[size];
        for (int i=0; i<size; i++)
        {
            generators[i] = randomGenerator(random);
        }
        return generators;
    }
}
