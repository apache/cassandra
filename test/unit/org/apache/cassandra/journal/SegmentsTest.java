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

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.cassandra.utils.concurrent.Ref;
import org.junit.Test;


import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class SegmentsTest
{
    @Test
    public void testSelect()
    {
        withRandom(0l, rng -> {
            // Create mock segments with different timestamps
            java.io.File file = File.createTempFile("segments", "test");
            List<Segment<String, String>> segmentList = new ArrayList<>();
            Set<Long> taken = new HashSet<>();
            for (int i = 0; i < 100; i++)
            {
                while (true)
                {
                    long id = rng.nextLong(0, 10_000);
                    if (taken.add(id))
                    {
                        segmentList.add(new TestSegment<>(file, id));
                        break;
                    }
                }
            }
            segmentList.sort(Comparator.comparing(s -> s.descriptor.timestamp));

            Segments<String, String> segments = Segments.of(segmentList);
            for (int i = 0; i < 10_000; i++)
            {
                // Generate two distinct segment idxs
                int i1 = rng.nextInt(segmentList.size());
                int i2;
                do
                {
                    i2 = rng.nextInt(segmentList.size());
                }
                while (i2 == i1);
                int min = Math.min(i1, i2);
                int max = Math.max(i1, i2);
                List<Segment<String, String>> selected = new ArrayList<>();
                segments.select(segmentList.get(min).id(),
                                segmentList.get(max).id(),
                                selected);
                List<Segment<String, String>> expected = segmentList.subList(min, max + 1);
                if (!Objects.equals(expected, selected))
                {
                    throw new AssertionError(String.format("\nExpected: %s\n" +
                                                           "Selected: %s",
                                                           expected,
                                                           selected));
                }
            }
        });
    }

    private static class TestSegment<K, V> extends Segment<K, V>
    {
        TestSegment(File dir, long timestamp)
        {
            super(Descriptor.create(new org.apache.cassandra.io.util.File(dir), timestamp, 1), null, null);
        }

        @Override
        void close(Journal<K, V> journal)
        {

        }

        @Override
        public boolean isActive()
        {
            return false;
        }

        @Override public boolean isStatic()
        {
            return false;
        }

        @Override
        boolean isEmpty()
        {
            return false;
        }

        @Override Index<K> index() { throw new UnsupportedOperationException(); }
        @Override boolean isFlushed(long position) { throw new UnsupportedOperationException(); }
        @Override public void persistMetadata() { throw new UnsupportedOperationException(); }
        @Override boolean read(int offset, int size, EntrySerializer.EntryHolder<K> into)  { throw new UnsupportedOperationException(); }
        @Override public void readAll(RecordConsumer<K> consumer) { throw new UnsupportedOperationException(); }
        @Override public ActiveSegment<K, V> asActive() { throw new UnsupportedOperationException(); }
        @Override public StaticSegment<K, V> asStatic() { throw new UnsupportedOperationException(); }
        @Override public Ref<Segment<K, V>> selfRef() { throw new UnsupportedOperationException(); }
        @Override public Ref<Segment<K, V>> tryRef(){ throw new UnsupportedOperationException(); }
        @Override public Ref<Segment<K, V>> ref(){ throw new UnsupportedOperationException(); }

        @Override
        public String toString()
        {
            return "TestSegment{" +
                   "id=" + descriptor.timestamp +
                   '}';
        }

        @Override
        public boolean equals(Object obj)
        {
            TestSegment<K, V> other = (TestSegment<K, V>) obj;
            return descriptor.equals(other.descriptor);
        }
    }
}