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
import java.util.List;

import org.junit.Test;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.junit.Assert.assertEquals;

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
}
