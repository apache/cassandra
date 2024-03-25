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

package org.apache.cassandra.harry.gen;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.cassandra.locator.InetAddressAndPort;

public class Generators
{
    public static class InetAddrAndPortGenerator implements Generator<InetAddressAndPort>
    {
        private final int port;
        public InetAddrAndPortGenerator()
        {
            this(9042);
        }

        public InetAddrAndPortGenerator(int port)
        {
            this.port = port;
        }

        @Override
        public InetAddressAndPort generate(EntropySource rng)
        {
            int orig = rng.nextInt();
            byte[] bytes = new byte[]{ (byte) (orig & 0xff),
                                       (byte) (orig << 8 & 0xff),
                                       (byte) (orig << 16 & 0xff),
                                       (byte) (orig << 24 & 0xff) };
            try
            {
                return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), bytes, port);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> Generator<T> pick(List<T> ts)
    {
        if (ts.isEmpty())
            throw new IllegalStateException("Can't pick from an empty list");
        return (rng) -> ts.get(rng.nextInt(0, ts.size()));
    }

    public static <T> Generator<T> pick(T... ts)
    {
        return pick(Arrays.asList(ts));
    }

    public static <T> Generator<List<T>> subsetGenerator(List<T> list)
    {
        return subsetGenerator(list, 0, list.size() - 1);
    }

    public static <T> Generator<List<T>> subsetGenerator(List<T> list, int minSize, int maxSize)
    {
        return (rng) -> {
            int count = rng.nextInt(minSize, maxSize);
            Set<T> set = new HashSet<>();
            for (int i = 0; i < count; i++)
                set.add(list.get(rng.nextInt(minSize, maxSize)));

            return (List<T>) new ArrayList<>(set);
        };
    }

    public static <T extends Enum<T>> Generator<T> enumValues(Class<T> e)
    {
        return pick(Arrays.asList(e.getEnumConstants()));
    }

    public static <T> Generator<List<T>> list(Generator<T> of, int maxSize)
    {
        return list(of, 0, maxSize);
    }

    public static <T> Generator<List<T>> list(Generator<T> of, int minSize, int maxSize)
    {
        return (rng) -> {
            int count = rng.nextInt(minSize, maxSize);
            return of.generate(rng, count);
        };
    }

    public static <T> Generator<T> constant(T constant)
    {
        return (random) -> constant;
    }

    public static <T> Generator<T> constant(Supplier<T> constant)
    {
        return (random) -> constant.get();
    }
}