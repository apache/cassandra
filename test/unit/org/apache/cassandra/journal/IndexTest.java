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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.junit.Test;

import org.agrona.collections.IntHashSet;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.TimeUUID;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class IndexTest
{
    private static final int[] EMPTY = new int[0];

    @Test
    public void testInMemoryIndexBasics()
    {
        InMemoryIndex<TimeUUID> index = InMemoryIndex.create(TimeUUIDKeySupport.INSTANCE);

        TimeUUID key0 = nextTimeUUID();
        TimeUUID key1 = nextTimeUUID();
        TimeUUID key2 = nextTimeUUID();
        TimeUUID key3 = nextTimeUUID();
        TimeUUID key4 = nextTimeUUID();

        assertArrayEquals(EMPTY, index.lookUp(key0));
        assertArrayEquals(EMPTY, index.lookUp(key1));
        assertArrayEquals(EMPTY, index.lookUp(key2));
        assertArrayEquals(EMPTY, index.lookUp(key3));
        assertArrayEquals(EMPTY, index.lookUp(key4));

        int val11 = 1100;
        int val21 = 2100;
        int val22 = 2200;
        int val31 = 3100;
        int val32 = 3200;
        int val33 = 3300;

        index.update(key1, val11);
        index.update(key2, val21);
        index.update(key2, val22);
        index.update(key3, val31);
        index.update(key3, val32);
        index.update(key3, val33);

        assertArrayEquals(EMPTY, index.lookUp(key0));
        assertArrayEquals(new int[] { val11 }, index.lookUp(key1));
        assertArrayEquals(new int[] { val21, val22 }, index.lookUp(key2));
        assertArrayEquals(new int[] { val31, val32, val33 }, index.lookUp(key3));
        assertArrayEquals(EMPTY, index.lookUp(key4));

        assertEquals(key1, index.firstId());
        assertEquals(key3, index.lastId());

        assertFalse(index.mayContainId(key0));
         assertTrue(index.mayContainId(key1));
         assertTrue(index.mayContainId(key2));
         assertTrue(index.mayContainId(key3));
        assertFalse(index.mayContainId(key4));
    }

    @Test
    public void testInMemoryIndexPersists() throws IOException
    {
        InMemoryIndex<TimeUUID> inMemory = InMemoryIndex.create(TimeUUIDKeySupport.INSTANCE);

        TimeUUID key0 = nextTimeUUID();
        TimeUUID key1 = nextTimeUUID();
        TimeUUID key2 = nextTimeUUID();
        TimeUUID key3 = nextTimeUUID();
        TimeUUID key4 = nextTimeUUID();

        int val11 = 1100;
        int val21 = 2100;
        int val22 = 2200;
        int val31 = 3100;
        int val32 = 3200;
        int val33 = 3300;

        inMemory.update(key1, val11);
        inMemory.update(key2, val21);
        inMemory.update(key2, val22);
        inMemory.update(key3, val31);
        inMemory.update(key3, val32);
        inMemory.update(key3, val33);

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();
        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);
        inMemory.persist(descriptor);

        try (OnDiskIndex<TimeUUID> onDisk = OnDiskIndex.open(descriptor, TimeUUIDKeySupport.INSTANCE))
        {
            assertArrayEquals(EMPTY, onDisk.lookUp(key0));
            assertArrayEquals(new int[] { val11 }, onDisk.lookUp(key1));
            assertArrayEquals(new int[] { val21, val22 }, onDisk.lookUp(key2));
            assertArrayEquals(new int[] { val31, val32, val33 }, onDisk.lookUp(key3));
            assertArrayEquals(EMPTY, onDisk.lookUp(key4));

            assertEquals(key1, onDisk.firstId());
            assertEquals(key3, onDisk.lastId());

            assertFalse(onDisk.mayContainId(key0));
             assertTrue(onDisk.mayContainId(key1));
             assertTrue(onDisk.mayContainId(key2));
             assertTrue(onDisk.mayContainId(key3));
            assertFalse(onDisk.mayContainId(key4));
        }
    }

    @Test
    public void prop() throws IOException
    {
        Constraint sizeConstraint = Constraint.between(1, 10);
        Constraint valueSizeConstraint = Constraint.between(0, 10);
        Constraint positionConstraint = Constraint.between(0, Integer.MAX_VALUE);
        Gen<TimeUUID> keyGen = Generators.timeUUID();
        Gen<int[]> valueGen = rs -> {
            int[] array = new int[(int) rs.next(valueSizeConstraint)];
            IntHashSet uniq = new IntHashSet();
            for (int i = 0; i < array.length; i++)
            {
                int value = (int) rs.next(positionConstraint);
                while (!uniq.add(value))
                    value = (int) rs.next(positionConstraint);
                array[i] = value;
            }
            return array;
        };
        Gen<Map<TimeUUID, int[]>> gen = rs -> {
            int size = (int) rs.next(sizeConstraint);
            Map<TimeUUID, int[]> map = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++)
            {
                TimeUUID key = keyGen.generate(rs);
                while (map.containsKey(key))
                    key = keyGen.generate(rs);
                int[] value = valueGen.generate(rs);
                map.put(key, value);
            }
            return map;
        };
        gen = gen.describedAs(map -> {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<TimeUUID, int[]> entry : map.entrySet())
                sb.append('\n').append(entry.getKey()).append('\t').append(Arrays.toString(entry.getValue()));
            return sb.toString();
        });
        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();
        qt().forAll(gen).checkAssert(map -> test(directory, map));
    }

    private static void test(File directory, Map<TimeUUID, int[]> map)
    {
        InMemoryIndex<TimeUUID> inMemory = InMemoryIndex.create(TimeUUIDKeySupport.INSTANCE);
        for (Map.Entry<TimeUUID, int[]> e : map.entrySet())
        {
            TimeUUID key = e.getKey();
            assertThat(inMemory.lookUp(key)).isEmpty();

            int[] value = e.getValue();
            if (value.length == 0)
                continue;
            for (int i : value)
                inMemory.update(key, i);
            Arrays.sort(value);
        }
        assertIndex(map, inMemory);

        Descriptor descriptor = Descriptor.create(directory, System.nanoTime(), 1);
        inMemory.persist(descriptor);

        try (OnDiskIndex<TimeUUID> onDisk = OnDiskIndex.open(descriptor, TimeUUIDKeySupport.INSTANCE))
        {
            assertIndex(map, onDisk);
        }
    }

    private static void assertIndex(Map<TimeUUID, int[]> expected, Index<TimeUUID> actual)
    {
        List<TimeUUID> keys = expected.entrySet()
                                      .stream()
                                      .filter(e -> e.getValue().length != 0)
                                      .map(Map.Entry::getKey)
                                      .sorted()
                                      .collect(Collectors.toList());

        if (keys.isEmpty())
        {
            assertThat(actual.firstId()).describedAs("Index %s had wrong firstId", actual).isNull();
            assertThat(actual.lastId()).describedAs("Index %s had wrong lastId", actual).isNull();
        }
        else
        {
            assertThat(actual.firstId()).describedAs("Index %s had wrong firstId", actual).isEqualTo(keys.get(0));
            assertThat(actual.lastId()).describedAs("Index %s had wrong lastId", actual).isEqualTo(keys.get(keys.size() - 1));
        }

        for (Map.Entry<TimeUUID, int[]> e : expected.entrySet())
        {
            TimeUUID key = e.getKey();
            int[] value = e.getValue();
            int[] read = actual.lookUp(key);

            if (value.length == 0)
            {
                assertThat(read).describedAs("Index %s returned wrong values for %s", actual, key).isEmpty();
            }
            else
            {
                assertThat(read).describedAs("Index %s returned wrong values for %s", actual, key).isEqualTo(value);
                assertThat(actual.mayContainId(key)).describedAs("Index %s expected %s to exist", key, actual).isTrue();
            }
        }
    }
}