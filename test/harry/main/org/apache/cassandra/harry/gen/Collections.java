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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.gen.rng.RngUtils;

// TODO: collections are currently not deflatable and/or checkable with a model
public class Collections
{
    public static <K, V> ColumnSpec.DataType<Map<K, V>> mapColumn(ColumnSpec.DataType<K> k,
                                                                  ColumnSpec.DataType<V> v,
                                                                  int maxSize)
    {
        return new ColumnSpec.DataType<Map<K, V>>(String.format("map<%s,%s>", k.toString(), v.toString()))
        {
            private final Bijections.Bijection<Map<K, V>> gen = mapGen(k.generator(), v.generator(), maxSize);

            public Bijections.Bijection<Map<K, V>> generator()
            {
                return gen;
            }

            public int maxSize()
            {
                return Long.BYTES;
            }
        };
    }

    public static <V> ColumnSpec.DataType<List<V>> listColumn(ColumnSpec.DataType<V> v,
                                                              int maxSize)
    {
        return new ColumnSpec.DataType<List<V>>(String.format("set<%s>", v.toString()))
        {
            private final Bijections.Bijection<List<V>> gen = listGen(v.generator(), maxSize);

            public Bijections.Bijection<List<V>> generator()
            {
                return gen;
            }

            public int maxSize()
            {
                return Long.BYTES;
            }
        };
    }


    public static <V> ColumnSpec.DataType<Set<V>> setColumn(ColumnSpec.DataType<V> v,
                                                            int maxSize)
    {
        return new ColumnSpec.DataType<Set<V>>(String.format("set<%s>", v.toString()))
        {
            private final Bijections.Bijection<Set<V>> gen = setGen(v.generator(), maxSize);

            public Bijections.Bijection<Set<V>> generator()
            {
                return gen;
            }

            public int maxSize()
            {
                return Long.BYTES;
            }
        };
    }


    public static <K, V> Bijections.Bijection<Map<K, V>> mapGen(Bijections.Bijection<K> keyGen,
                                                                Bijections.Bijection<V> valueGen,
                                                                int maxSize)
    {
        return new MapGenerator<>(keyGen, valueGen, maxSize);
    }

    public static <V> Bijections.Bijection<List<V>> listGen(Bijections.Bijection<V> valueGen,
                                                            int maxSize)
    {
        return new ListGenerator<>(valueGen, maxSize);
    }

    public static <V> Bijections.Bijection<Set<V>> setGen(Bijections.Bijection<V> valueGen,
                                                          int maxSize)
    {
        return new SetGenerator<>(valueGen, maxSize);
    }

    public static class MapGenerator<K, V> implements Bijections.Bijection<Map<K, V>>
    {
        public final Bijections.Bijection<K> keyGen;
        public final Bijections.Bijection<V> valueGen;
        public int maxSize;

        public MapGenerator(Bijections.Bijection<K> keyGen,
                            Bijections.Bijection<V> valueGen,
                            int maxSize)
        {
            this.keyGen = keyGen;
            this.valueGen = valueGen;
            this.maxSize = maxSize;
        }

        public Map<K, V> inflate(long descriptor)
        {
            long rnd = RngUtils.next(descriptor);
            int count = RngUtils.asInt(rnd, 0, maxSize);
            Map<K, V> m = new HashMap<>();
            for (int i = 0; i < count; i++)
            {
                rnd = RngUtils.next(rnd);
                K key = keyGen.inflate(rnd);
                rnd = RngUtils.next(rnd);
                V value = valueGen.inflate(rnd);
                m.put(key, value);
            }

            return m;
        }

        // At least for non-frozen ones
        public long deflate(Map<K, V> value)
        {
            throw new UnsupportedOperationException();
        }

        public int byteSize()
        {
            return Long.BYTES;
        }

        public int compare(long l, long r)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class ListGenerator<V> implements Bijections.Bijection<List<V>>
    {
        public final Bijections.Bijection<V> valueGen;
        public int maxSize;

        public ListGenerator(Bijections.Bijection<V> valueGen,
                             int maxSize)
        {
            this.valueGen = valueGen;
            this.maxSize = maxSize;
        }

        public List<V> inflate(long descriptor)
        {
            long rnd = RngUtils.next(descriptor);
            int count = RngUtils.asInt(rnd, 0, maxSize);
            List<V> m = new ArrayList<>();
            for (int i = 0; i < count; i++)
            {
                rnd = RngUtils.next(rnd);
                V value = valueGen.inflate(rnd);
                m.add(value);
            }

            return m;
        }

        // At least for non-frozen ones
        public long deflate(List<V> value)
        {
            throw new UnsupportedOperationException();
        }

        public int byteSize()
        {
            return Long.BYTES;
        }

        public int compare(long l, long r)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class SetGenerator<V> implements Bijections.Bijection<Set<V>>
    {
        public final Bijections.Bijection<V> valueGen;
        public int maxSize;

        public SetGenerator(Bijections.Bijection<V> valueGen,
                            int maxSize)
        {
            this.valueGen = valueGen;
            this.maxSize = maxSize;
        }

        public Set<V> inflate(long descriptor)
        {
            long rnd = RngUtils.next(descriptor);
            int count = RngUtils.asInt(rnd, 0, maxSize);
            Set<V> m = new HashSet<>();
            for (int i = 0; i < count; i++)
            {
                rnd = RngUtils.next(rnd);
                V value = valueGen.inflate(rnd);
                m.add(value);
            }

            return m;
        }

        // At least for non-frozen ones
        public long deflate(Set<V> value)
        {
            throw new UnsupportedOperationException();
        }

        public int byteSize()
        {
            return Long.BYTES;
        }

        public int compare(long l, long r)
        {
            throw new UnsupportedOperationException();
        }
    }
}
