/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.serializers.*;

public class CollectionTypeTest
{
    @Test
    public void testListComparison()
    {
        ListType<String> lt = ListType.getInstance(UTF8Type.instance, true);

        ByteBuffer[] lists = new ByteBuffer[] {
            ByteBufferUtil.EMPTY_BYTE_BUFFER,
            lt.decompose(ImmutableList.<String>of()),
            lt.decompose(ImmutableList.of("aa")),
            lt.decompose(ImmutableList.of("bb")),
            lt.decompose(ImmutableList.of("bb", "cc")),
            lt.decompose(ImmutableList.of("bb", "dd"))
        };

        for (int i = 0; i < lists.length; i++)
            assertEquals(lt.compare(lists[i], lists[i]), 0);

        for (int i = 0; i < lists.length-1; i++)
        {
            for (int j = i+1; j < lists.length; j++)
            {
                assertEquals(String.format("compare(lists[%d], lists[%d])", i, j), -1, lt.compare(lists[i], lists[j]));
                assertEquals(String.format("compare(lists[%d], lists[%d])", j, i),  1, lt.compare(lists[j], lists[i]));
            }
        }
    }

    @Test
    public void testSetComparison()
    {
        SetType<String> st = SetType.getInstance(UTF8Type.instance, true);

        ByteBuffer[] sets = new ByteBuffer[] {
            ByteBufferUtil.EMPTY_BYTE_BUFFER,
            st.decompose(ImmutableSet.<String>of()),
            st.decompose(ImmutableSet.of("aa")),
            st.decompose(ImmutableSet.of("bb")),
            st.decompose(ImmutableSet.of("bb", "cc")),
            st.decompose(ImmutableSet.of("bb", "dd"))
        };

        for (int i = 0; i < sets.length; i++)
            assertEquals(st.compare(sets[i], sets[i]), 0);

        for (int i = 0; i < sets.length-1; i++)
        {
            for (int j = i+1; j < sets.length; j++)
            {
                assertEquals(String.format("compare(sets[%d], sets[%d])", i, j), -1, st.compare(sets[i], sets[j]));
                assertEquals(String.format("compare(sets[%d], sets[%d])", j, i),  1, st.compare(sets[j], sets[i]));
            }
        }
    }

    @Test
    public void testMapComparison()
    {
        MapType<String, String> mt = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true);

        ByteBuffer[] maps = new ByteBuffer[] {
            ByteBufferUtil.EMPTY_BYTE_BUFFER,
            mt.decompose(ImmutableMap.<String, String>of()),
            mt.decompose(ImmutableMap.of("aa", "val1")),
            mt.decompose(ImmutableMap.of("aa", "val2")),
            mt.decompose(ImmutableMap.of("bb", "val1")),
            mt.decompose(ImmutableMap.of("bb", "val1", "cc", "val3")),
            mt.decompose(ImmutableMap.of("bb", "val1", "dd", "val3")),
            mt.decompose(ImmutableMap.of("bb", "val1", "dd", "val4"))
        };

        for (int i = 0; i < maps.length; i++)
            assertEquals(mt.compare(maps[i], maps[i]), 0);

        for (int i = 0; i < maps.length-1; i++)
        {
            for (int j = i+1; j < maps.length; j++)
            {
                assertEquals(String.format("compare(maps[%d], maps[%d])", i, j), mt.compare(maps[i], maps[j]), -1);
                assertEquals(String.format("compare(maps[%d], maps[%d])", j, i), mt.compare(maps[j], maps[i]), 1);
            }
        }
    }

    @Test
    public void listSerDerTest()
    {
        ListSerializer<String> sls = ListType.getInstance(UTF8Type.instance, true).getSerializer();
        ListSerializer<Integer> ils = ListType.getInstance(Int32Type.instance, true).getSerializer();

        List<String> sl = Arrays.asList("Foo", "Bar");
        List<Integer> il = Arrays.asList(3, 1, 5);

        ByteBuffer sb = sls.serialize(sl);
        ByteBuffer ib = ils.serialize(il);

        assertEquals(sls.deserialize(sb), sl);
        assertEquals(ils.deserialize(ib), il);

        sls.validate(sb);
        ils.validate(ib);

        // string list with integer list type
        assertInvalid(ils, sb);
        // non list value
        assertInvalid(sls, UTF8Type.instance.getSerializer().serialize("foo"));
    }

    @Test
    public void setSerDerTest()
    {
        SetSerializer<String> sss = SetType.getInstance(UTF8Type.instance, true).getSerializer();
        SetSerializer<Integer> iss = SetType.getInstance(Int32Type.instance, true).getSerializer();

        Set<String> ss = new HashSet(){{ add("Foo"); add("Bar"); }};
        Set<Integer> is = new HashSet(){{ add(3); add(1); add(5); }};

        ByteBuffer sb = sss.serialize(ss);
        ByteBuffer ib = iss.serialize(is);

        assertEquals(sss.deserialize(sb), ss);
        assertEquals(iss.deserialize(ib), is);

        sss.validate(sb);
        iss.validate(ib);

        // string set with integer set type
        assertInvalid(iss, sb);
        // non set value
        assertInvalid(sss, UTF8Type.instance.getSerializer().serialize("foo"));
    }

    @Test
    public void setMapDerTest()
    {
        MapSerializer<String, String> sms = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true).getSerializer();
        MapSerializer<Integer, Integer> ims = MapType.getInstance(Int32Type.instance, Int32Type.instance, true).getSerializer();

        Map<String, String> sm = new HashMap(){{ put("Foo", "xxx"); put("Bar", "yyy"); }};
        Map<Integer, Integer> im = new HashMap(){{ put(3, 0); put(1, 8); put(5, 2); }};

        ByteBuffer sb = sms.serialize(sm);
        ByteBuffer ib = ims.serialize(im);

        assertEquals(sms.deserialize(sb), sm);
        assertEquals(ims.deserialize(ib), im);

        sms.validate(sb);
        ims.validate(ib);

        // string map with integer map type
        assertInvalid(ims, sb);
        // non map value
        assertInvalid(sms, UTF8Type.instance.getSerializer().serialize("foo"));

        MapSerializer<Integer, String> sims = MapType.getInstance(Int32Type.instance, UTF8Type.instance, true).getSerializer();
        MapSerializer<String, Integer> isms = MapType.getInstance(UTF8Type.instance, Int32Type.instance, true).getSerializer();

        // only key are invalid
        assertInvalid(isms, sb);
        // only values are invalid
        assertInvalid(sims, sb);
    }

    private void assertInvalid(TypeSerializer<?> type, ByteBuffer value)
    {
        try {
            type.validate(value);
            fail("Value " + ByteBufferUtil.bytesToHex(value) + " shouldn't be valid for type " + type);
        } catch (MarshalException e) {
            // ok, that's what we want
        }
    }
}
