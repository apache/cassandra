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

package org.apache.cassandra.diag.store;

import java.util.Map;
import java.util.NavigableMap;

import org.junit.Test;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventServiceTest.TestEvent1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class DiagnosticEventMemoryStoreTest
{
    @Test
    public void testEmpty()
    {
        DiagnosticEventMemoryStore store = new DiagnosticEventMemoryStore();
        assertEquals(0, store.size());
        assertEquals(0, store.scan(0L, 10).size());
    }

    @Test
    public void testSingle()
    {
        DiagnosticEventMemoryStore store = new DiagnosticEventMemoryStore();
        store.store(new TestEvent1());
        assertEquals(1, store.size());
        assertEquals(1, store.scan(0L, 10).size());
    }

    @Test
    public void testIdentity()
    {
        DiagnosticEventMemoryStore store = new DiagnosticEventMemoryStore();
        TestEvent1 e1 = new TestEvent1();
        TestEvent1 e2 = new TestEvent1();
        TestEvent1 e3 = new TestEvent1();

        store.store(e1);
        store.store(e2);
        store.store(e3);

        assertEquals(3, store.size());

        NavigableMap<Long, DiagnosticEvent> res = store.scan(0L, 10);
        assertEquals(3, res.size());

        Map.Entry<Long, DiagnosticEvent> entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(1), entry.getKey());
        assertSame(e1, entry.getValue());

        entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(2), entry.getKey());
        assertSame(e2, entry.getValue());

        entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(3), entry.getKey());
        assertSame(e3, entry.getValue());
    }

    @Test
    public void testLimit()
    {
        DiagnosticEventMemoryStore store = new DiagnosticEventMemoryStore();

        TestEvent1 e1 = new TestEvent1();
        TestEvent1 e2 = new TestEvent1();
        TestEvent1 e3 = new TestEvent1();

        store.store(e1);
        store.store(e2);
        store.store(e3);

        NavigableMap<Long, DiagnosticEvent> res = store.scan(0L, 2);
        assertEquals(2, res.size());

        Map.Entry<Long, DiagnosticEvent> entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(1), entry.getKey());
        assertSame(e1, entry.getValue());

        entry = res.pollLastEntry();
        assertEquals(Long.valueOf(2), entry.getKey());
        assertSame(e2, entry.getValue());
    }

    @Test
    public void testSeek()
    {
        DiagnosticEventMemoryStore store = new DiagnosticEventMemoryStore();

        TestEvent1 e2 = new TestEvent1();
        TestEvent1 e3 = new TestEvent1();

        store.store(new TestEvent1());
        store.store(e2);
        store.store(e3);
        store.store(new TestEvent1());
        store.store(new TestEvent1());
        store.store(new TestEvent1());

        NavigableMap<Long, DiagnosticEvent> res = store.scan(2L, 2);
        assertEquals(2, res.size());

        Map.Entry<Long, DiagnosticEvent> entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(2), entry.getKey());
        assertSame(e2, entry.getValue());

        entry = res.pollLastEntry();
        assertEquals(Long.valueOf(3), entry.getKey());
        assertSame(e3, entry.getValue());
    }

    @Test
    public void testMaxElements()
    {
        DiagnosticEventMemoryStore store = new DiagnosticEventMemoryStore();
        store.setMaxSize(3);

        store.store(new TestEvent1());
        store.store(new TestEvent1());
        store.store(new TestEvent1());
        TestEvent1 e2 = new TestEvent1(); // 4
        TestEvent1 e3 = new TestEvent1();
        store.store(e2);
        store.store(e3);
        store.store(new TestEvent1()); // 6

        assertEquals(3, store.size());

        NavigableMap<Long, DiagnosticEvent> res = store.scan(4L, 2);
        assertEquals(2, res.size());

        Map.Entry<Long, DiagnosticEvent> entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(4), entry.getKey());
        assertSame(e2, entry.getValue());

        entry = res.pollFirstEntry();
        assertEquals(Long.valueOf(5), entry.getKey());
        assertSame(e3, entry.getValue());

        store.store(new TestEvent1()); // 7
        store.store(new TestEvent1());
        store.store(new TestEvent1());

        res = store.scan(4L, 2);
        assertEquals(2, res.size());
        assertEquals(Long.valueOf(7), res.pollFirstEntry().getKey());
        assertEquals(Long.valueOf(8), res.pollLastEntry().getKey());
    }
}
