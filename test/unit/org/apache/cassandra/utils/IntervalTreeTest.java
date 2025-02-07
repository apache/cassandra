package org.apache.cassandra.utils;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import org.junit.Test;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class IntervalTreeTest
{
    @Test
    public void testSearch() throws Exception
    {
        List<Interval<Integer, Void>> intervals = new ArrayList<Interval<Integer, Void>>();

        intervals.add(Interval.<Integer, Void>create(-300, -200));
        intervals.add(Interval.<Integer, Void>create(-3, -2));
        intervals.add(Interval.<Integer, Void>create(1, 2));
        intervals.add(Interval.<Integer, Void>create(3, 6));
        intervals.add(Interval.<Integer, Void>create(2, 4));
        intervals.add(Interval.<Integer, Void>create(5, 7));
        intervals.add(Interval.<Integer, Void>create(1, 3));
        intervals.add(Interval.<Integer, Void>create(4, 6));
        intervals.add(Interval.<Integer, Void>create(8, 9));
        intervals.add(Interval.<Integer, Void>create(15, 20));
        intervals.add(Interval.<Integer, Void>create(40, 50));
        intervals.add(Interval.<Integer, Void>create(49, 60));


        IntervalTree<Integer, Void, Interval<Integer, Void>> it = IntervalTree.build(intervals);

        assertEquals(3, it.search(Interval.<Integer, Void>create(4, 4)).size());
        assertEquals(4, it.search(Interval.<Integer, Void>create(4, 5)).size());
        assertEquals(7, it.search(Interval.<Integer, Void>create(-1, 10)).size());
        assertEquals(0, it.search(Interval.<Integer, Void>create(-1, -1)).size());
        assertEquals(5, it.search(Interval.<Integer, Void>create(1, 4)).size());
        assertEquals(2, it.search(Interval.<Integer, Void>create(0, 1)).size());
        assertEquals(0, it.search(Interval.<Integer, Void>create(10, 12)).size());

        List<Interval<Integer, Void>> intervals2 = new ArrayList<Interval<Integer, Void>>();

        //stravinsky 1880-1971
        intervals2.add(Interval.<Integer, Void>create(1880, 1971));
        //Schoenberg
        intervals2.add(Interval.<Integer, Void>create(1874, 1951));
        //Grieg
        intervals2.add(Interval.<Integer, Void>create(1843, 1907));
        //Schubert
        intervals2.add(Interval.<Integer, Void>create(1779, 1828));
        //Mozart
        intervals2.add(Interval.<Integer, Void>create(1756, 1828));
        //Schuetz
        intervals2.add(Interval.<Integer, Void>create(1585, 1672));

        IntervalTree<Integer, Void, Interval<Integer, Void>> it2 = IntervalTree.build(intervals2);

        assertEquals(0, it2.search(Interval.<Integer, Void>create(1829, 1842)).size());

        List<Void> intersection1 = it2.search(Interval.<Integer, Void>create(1907, 1907));
        assertEquals(3, intersection1.size());

        intersection1 = it2.search(Interval.<Integer, Void>create(1780, 1790));
        assertEquals(2, intersection1.size());

    }

    @Test
    public void testIteration()
    {
        List<Interval<Integer, Void>> intervals = new ArrayList<Interval<Integer, Void>>();

        intervals.add(Interval.<Integer, Void>create(-300, -200));
        intervals.add(Interval.<Integer, Void>create(-3, -2));
        intervals.add(Interval.<Integer, Void>create(1, 2));
        intervals.add(Interval.<Integer, Void>create(3, 6));
        intervals.add(Interval.<Integer, Void>create(2, 4));
        intervals.add(Interval.<Integer, Void>create(5, 7));
        intervals.add(Interval.<Integer, Void>create(1, 3));
        intervals.add(Interval.<Integer, Void>create(4, 6));
        intervals.add(Interval.<Integer, Void>create(8, 9));
        intervals.add(Interval.<Integer, Void>create(15, 20));
        intervals.add(Interval.<Integer, Void>create(40, 50));
        intervals.add(Interval.<Integer, Void>create(49, 60));

        IntervalTree<Integer, Void, Interval<Integer, Void>> it = IntervalTree.build(intervals);

        Collections.sort(intervals, Interval.<Integer, Void>minOrdering());

        List<Interval<Integer, Void>> l = new ArrayList<Interval<Integer, Void>>();
        for (Interval<Integer, Void> i : it)
            l.add(i);

        assertEquals(intervals, l);
    }

    @Test
    public void testSerialization() throws Exception
    {
        List<Interval<Integer, String>> intervals = new ArrayList<Interval<Integer, String>>();

        intervals.add(Interval.<Integer, String>create(-300, -200, "a"));
        intervals.add(Interval.<Integer, String>create(-3, -2, "b"));
        intervals.add(Interval.<Integer, String>create(1, 2, "c"));
        intervals.add(Interval.<Integer, String>create(1, 3, "d"));
        intervals.add(Interval.<Integer, String>create(2, 4, "e"));
        intervals.add(Interval.<Integer, String>create(3, 6, "f"));
        intervals.add(Interval.<Integer, String>create(4, 6, "g"));
        intervals.add(Interval.<Integer, String>create(5, 7, "h"));
        intervals.add(Interval.<Integer, String>create(8, 9, "i"));
        intervals.add(Interval.<Integer, String>create(15, 20, "j"));
        intervals.add(Interval.<Integer, String>create(40, 50, "k"));
        intervals.add(Interval.<Integer, String>create(49, 60, "l"));

        IntervalTree<Integer, String, Interval<Integer, String>> it = IntervalTree.build(intervals);

        IVersionedSerializer<IntervalTree<Integer, String, Interval<Integer, String>>> serializer = IntervalTree.serializer(
                new ISerializer<Integer>()
                {
                    public void serialize(Integer i, DataOutputPlus out) throws IOException
                    {
                        out.writeInt(i);
                    }

                    public Integer deserialize(DataInputPlus in) throws IOException
                    {
                        return in.readInt();
                    }

                    public long serializedSize(Integer i)
                    {
                        return 4;
                    }
                },
                new ISerializer<String>()
                {
                    public void serialize(String v, DataOutputPlus out) throws IOException
                    {
                        out.writeUTF(v);
                    }

                    public String deserialize(DataInputPlus in) throws IOException
                    {
                        return in.readUTF();
                    }

                    public long serializedSize(String v)
                    {
                        return v.length();
                    }
                },
                (Constructor<Interval<Integer, String>>) (Object) Interval.class.getConstructor(Object.class, Object.class, Object.class)
        );

        DataOutputBuffer out = new DataOutputBuffer();

        serializer.serialize(it, out, 0);

        DataInputPlus in = new DataInputBuffer(out.toByteArray());

        IntervalTree<Integer, String, Interval<Integer, String>> it2 = serializer.deserialize(in, 0);
        List<Interval<Integer, String>> intervals2 = new ArrayList<Interval<Integer, String>>();
        for (Interval<Integer, String> i : it2)
            intervals2.add(i);

        assertEquals(intervals, intervals2);
    }

    @Test
    public void testCopyAndReplace()
    {
        class DummyObj
        {
            final String data;
            public DummyObj(String d)
            {
                data = d;
            }
            @Override
            public final boolean equals(Object o)
            {
                if(!(o instanceof DummyObj))
                    return false;

                DummyObj that = (DummyObj)o;
                return Objects.equal(data, that.data);
            }
        }
        List<Interval<Integer, DummyObj>> intervals = new ArrayList<>();

        DummyObj data1 = new DummyObj("c");
        DummyObj data2 = new DummyObj("i");
        DummyObj data3 = new DummyObj("cc");
        DummyObj newData1 = new DummyObj("c");
        DummyObj newData2 = new DummyObj("i");

        Interval<Integer, DummyObj> target1 = Interval.create(1, 2, data1);
        // same interval range with target1 but different data
        Interval<Integer, DummyObj> target11 = Interval.create(1, 2, data3);
        Interval<Integer, DummyObj> target2 = Interval.create(8, 9, data2);

        // the two new intervals should replace the old ones in tree
        Interval<Integer, DummyObj> newTarget1 = Interval.create(1, 2, newData1);
        Interval<Integer, DummyObj> newTarget2 = Interval.create(8, 9, newData2);

        intervals.add(Interval.create(-300, -200, new DummyObj("a")));
        intervals.add(Interval.create(-3, -2, new DummyObj("b")));
        intervals.add(target1);
        intervals.add(target11);
        intervals.add(Interval.create(1, 3, new DummyObj("d")));
        intervals.add(Interval.create(2, 4, new DummyObj("e")));
        intervals.add(Interval.create(3, 6, new DummyObj("f")));
        intervals.add(Interval.create(4, 6, new DummyObj("g")));
        intervals.add(Interval.create(5, 7, new DummyObj("h")));
        intervals.add(target2);
        intervals.add(Interval.create(15, 20, new DummyObj("j")));
        intervals.add(Interval.create(40, 50, new DummyObj("k")));
        intervals.add(Interval.create(49, 60, new DummyObj("l")));

        IntervalTree<Integer, DummyObj, Interval<Integer, DummyObj>> it = IntervalTree.build(intervals);
        assertEquals(3, it.search(Interval.create(4, 4)).size());
        assertEquals(4, it.search(Interval.create(4, 5)).size());
        assertEquals(8, it.search(Interval.create(-1, 10)).size());
        assertEquals(0, it.search(Interval.create(-1, -1)).size());
        assertEquals(6, it.search(Interval.create(1, 4)).size());
        assertEquals(3, it.search(Interval.create(0, 1)).size());
        assertEquals(0, it.search(Interval.create(10, 12)).size());

        // should return 4 intervals: (1, 2, c), (1, 2, cc), (1, 3, d), (2, 4, e)
        List<DummyObj> intersection1 = it.search(Interval.create(1, 2));
        assertEquals(4, intersection1.size());
        for (DummyObj ob : intersection1)
        {
            if (ob.data.equals("c"))
            {
                assertNotSame(newData1, ob);
                assertSame(data1, ob);
            }
            else if (ob.data.equals("cc"))
                assertSame(data3, ob);
            else if (!ob.equals(new DummyObj("d")) && !ob.equals(new DummyObj("e")))
                // fail if not matchinng
                fail();
        }

        List<DummyObj> intersection2 = it.search(Interval.create(8, 9));
        assertSame(intersection2.get(0), data2);

        Map<Interval<Integer, DummyObj>, Interval<Integer, DummyObj>> toUpdate = new HashMap();
        toUpdate.put(target1, newTarget1);
        toUpdate.put(target2, newTarget2);
        it = new IntervalTree<>(it.intervalCount(), it.copyAndReplace(toUpdate));
        assertEquals(3, it.search(Interval.create(4, 4)).size());
        assertEquals(4, it.search(Interval.create(4, 5)).size());
        assertEquals(8, it.search(Interval.create(-1, 10)).size());
        assertEquals(0, it.search(Interval.create(-1, -1)).size());
        assertEquals(6, it.search(Interval.create(1, 4)).size());
        assertEquals(3, it.search(Interval.create(0, 1)).size());
        assertEquals(0, it.search(Interval.create(10, 12)).size());

        // should return 4 intervals: (1, 2, c'), (1, 2, cc), (1, 3, d), (2, 4, e)
        //                                   ^ replaced
        intersection1 = it.search(Interval.create(1, 2));
        assertEquals(4, intersection1.size());
        for (DummyObj ob : intersection1)
        {
            if (ob.data.equals("c"))
            {
                assertNotSame(data1, ob);
                assertSame(newData1, ob);
            }
            else if (ob.data.equals("cc"))
                assertSame(data3, ob);
            else if (!ob.equals(new DummyObj("d")) && !ob.equals(new DummyObj("e")))
                // fail if not matchinng
                fail();
        }
        intersection2 = it.search(Interval.create(8, 9));
        assertNotSame(intersection1.get(0), data2);
        assertSame(intersection2.get(0), newData2);
    }
}
