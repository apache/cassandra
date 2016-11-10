/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;

import org.junit.Test;

import static org.junit.Assert.*;

public class AccumulatorTest
{
    @Test
    public void testAddMoreThanCapacity()
    {
        Accumulator<Integer> accu = new Accumulator(4);

        accu.add(1);
        accu.add(2);
        accu.add(3);
        accu.add(4);

        try
        {
            accu.add(5);
            fail();
        }
        catch (IllegalStateException e)
        {
            // Expected
        }
    }

    @Test
    public void testIsEmptyAndSize()
    {
        Accumulator<Integer> accu = new Accumulator(4);

        assertTrue(accu.isEmpty());
        assertEquals(0, accu.size());

        accu.add(1);
        accu.add(2);

        assertTrue(!accu.isEmpty());
        assertEquals(2, accu.size());

        accu.add(3);
        accu.add(4);

        assertTrue(!accu.isEmpty());
        assertEquals(4, accu.size());
    }

    @Test
    public void testGetAndIterator()
    {
        Accumulator<String> accu = new Accumulator(4);

        accu.add("3");
        accu.add("2");
        accu.add("4");

        assertEquals("3", accu.get(0));
        assertEquals("2", accu.get(1));
        assertEquals("4", accu.get(2));

        try
        {
            assertEquals(null, accu.get(3));
            fail();
        }
        catch (IndexOutOfBoundsException e)
        {
            // Expected
        }

        accu.add("0");

        assertEquals("0", accu.get(3));

        Iterator<String> iter = accu.iterator();

        assertEquals("3", iter.next());
        assertEquals("2", iter.next());
        assertEquals("4", iter.next());
        assertEquals("0", iter.next());
        assertFalse(iter.hasNext());
    }
}
