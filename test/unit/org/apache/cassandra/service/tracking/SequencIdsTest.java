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
package org.apache.cassandra.service.tracking;

import org.junit.Test;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SequencIdsTest
{
    @Test
    public void testEmptyAndAddExisting()
    {
        SequenceIds ids = new SequenceIds();
        assertEquals(0, ids.rangeCount());
        assertEquals(0, ids.idCount());

        long id10 = id(10);

        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        assertFalse(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());
    }

    @Test
    public void testAppend()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should extend
        long id11 = id(11);
        assertTrue(ids.add(id11));
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should append
        long id13 = id(13);
        assertTrue(ids.add(id13));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());
    }

    @Test
    public void testPrepend()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should extend
        long id9 = id(9);
        assertTrue(ids.add(id9));
        assertEquals(1, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should prepend
        long id7 = id(7);
        assertTrue(ids.add(id7));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());
    }

    @Test
    public void testClosesGaps()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should prepend
        long id6 = id(6);
        assertTrue(ids.add(id6));
        assertEquals(2, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should extend left range
        long id7 = id(7);
        assertTrue(ids.add(id7));
        assertEquals(2, ids.rangeCount());
        assertEquals(3, ids.idCount());

        // should extend right range
        long id9 = id(9);
        assertTrue(ids.add(id9));
        assertEquals(2, ids.rangeCount());
        assertEquals(4, ids.idCount());

        // should close the gap and collapse all into one range
        long id8 = id(8);
        assertTrue(ids.add(id8));
        assertEquals(1, ids.rangeCount());
        assertEquals(5, ids.idCount());
    }

    @Test
    public void testCreatesMoreGaps()
    {
        SequenceIds ids = new SequenceIds();

        long id10 = id(10);
        assertTrue(ids.add(id10));
        assertEquals(1, ids.rangeCount());
        assertEquals(1, ids.idCount());

        // should prepend
        long id6 = id(6);
        assertTrue(ids.add(id6));
        assertEquals(2, ids.rangeCount());
        assertEquals(2, ids.idCount());

        // should insert in the middle
        long id8 = id(8);
        assertTrue(ids.add(id8));
        assertEquals(3, ids.rangeCount());
        assertEquals(3, ids.idCount());
    }

    private long id(int offset)
    {
        return MutationId.sequenceId(offset, (int) currentTimeMillis() / 1000);
    }
}
