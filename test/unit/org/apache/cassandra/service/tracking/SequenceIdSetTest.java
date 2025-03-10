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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SequenceIdSetTest
{
    private static void assertEquals(SequenceIdSet actual, long... expected)
    {
        Assert.assertEquals(expected.length, actual.size());
        Assert.assertArrayEquals(expected, actual.toArray());
    }

    @Test
    public void appendTest()
    {
        SequenceIdSet ids = new SequenceIdSet(8);
        Assert.assertEquals(8, ids.capacity());

        for (int i = 0; i < 8; i++)
        {
            ids.append(i);
            Assert.assertEquals(i + 1, ids.size());
            Assert.assertEquals(8, ids.capacity());
        }

        assertEquals(ids, 0, 1, 2, 3, 4, 5, 6, 7);

        // this should cause the array to be expanded
        ids.append(10);
        Assert.assertEquals(9, ids.size());
        Assert.assertEquals(16, ids.capacity());

        assertEquals(ids, 0, 1, 2, 3, 4, 5, 6, 7, 10);

        // confirm that trying to append a value less than the final
        // value fails
        try
        {
            ids.append(9);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            // expected
            assertEquals(ids, 0, 1, 2, 3, 4, 5, 6, 7, 10);
        }
    }

    @Test
    public void addTest()
    {
        SequenceIdSet ids = new SequenceIdSet(8);

        ids.append(4);
        ids.append(5);

        ids.append(7);
        ids.append(8);

        ids.append(11);
        ids.append(12);

        assertEquals(ids, 4, 5, 7, 8, 11, 12);

        // prepend
        assertFalse(ids.add(4));
        assertTrue(ids.add(3));
        assertEquals(ids, 3, 4, 5, 7, 8, 11, 12);

        // append
        assertFalse(ids.add(12));
        assertTrue(ids.add(13));
        assertEquals(ids, 3, 4, 5, 7, 8, 11, 12, 13);

        // insert
        assertFalse(ids.add(7));
        assertTrue(ids.add(6));
        assertTrue(ids.add(9));
        assertEquals(ids, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13);
    }
}
