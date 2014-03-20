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

import org.apache.cassandra.utils.ByteBufferUtil;

public class CollectionTypeTest
{
    @Test
    public void testListComparison()
    {
        ListType<String> lt = ListType.getInstance(UTF8Type.instance);

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
                assertEquals(lt.compare(lists[i], lists[j]), -1);
                assertEquals(lt.compare(lists[j], lists[i]), 1);
            }
        }
    }

    @Test
    public void testSetComparison()
    {
        SetType<String> st = SetType.getInstance(UTF8Type.instance);

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
                assertEquals(st.compare(sets[i], sets[j]), -1);
                assertEquals(st.compare(sets[j], sets[i]), 1);
            }
        }
    }

    @Test
    public void testMapComparison()
    {
        MapType<String, String> mt = MapType.getInstance(UTF8Type.instance, UTF8Type.instance);

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
                assertEquals(mt.compare(maps[i], maps[j]), -1);
                assertEquals(mt.compare(maps[j], maps[i]), 1);
            }
        }
    }
}
