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

package org.apache.cassandra.utils.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.utils.btree.BTree.Dir;
import org.junit.Test;

public class BTreeSearchIteratorTest
{

    private static List<Integer> seq(int count)
    {
        return seq(count, 0, 1);
    }

    private static List<Integer> seq(int count, int base, int multi)
    {
        List<Integer> r = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
            r.add(i * multi + base);
        return r;
    }

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    private static void assertIteratorExceptionBegin(final BTreeSearchIterator<Integer, Integer> iter)
    {
        try
        {
            iter.current();
            fail("Should throw NoSuchElementException");
        }
        catch (NoSuchElementException ex)
        {
        }
        try
        {
            iter.indexOfCurrent();
            fail("Should throw NoSuchElementException");
        }
        catch (NoSuchElementException ex)
        {
        }
    }

    private static void assertIteratorExceptionEnd(final BTreeSearchIterator<Integer, Integer> iter)
    {
        assertFalse(iter.hasNext());
        try
        {
            iter.next();
            fail("Should throw NoSuchElementException");
        }
        catch (NoSuchElementException ex)
        {
        }
    }

    private static void assertBTreeSearchIteratorEquals(final BTreeSearchIterator<Integer, Integer> iter1,
                                                        final BTreeSearchIterator<Integer, Integer> iter2)
    {
        assertIteratorExceptionBegin(iter1);
        assertIteratorExceptionBegin(iter2);
        while (iter1.hasNext())
        {
            assertTrue(iter2.hasNext());
            assertEquals(iter1.next(), iter2.next());
            assertEquals(iter1.current(), iter2.current());
            assertEquals(iter1.indexOfCurrent(), iter2.indexOfCurrent());
        }
        assertIteratorExceptionEnd(iter1);
        assertIteratorExceptionEnd(iter2);
    }

    private static void assertBTreeSearchIteratorEquals(final BTreeSearchIterator<Integer, Integer> iter1,
                                                        final BTreeSearchIterator<Integer, Integer> iter2,
                                                        int... targets)
    {
        assertIteratorExceptionBegin(iter1);
        assertIteratorExceptionBegin(iter2);
        for (int i : targets)
        {
            Integer val1 = iter1.next(i);
            Integer val2 = iter2.next(i);
            assertEquals(val1, val2);
            if (val1 != null)
            {
                assertEquals(iter1.current(), iter2.current());
                assertEquals(iter1.indexOfCurrent(), iter2.indexOfCurrent());
            }
        }

        while (iter1.hasNext())
        {
            assertTrue(iter2.hasNext());
            assertEquals(iter1.next(), iter2.next());
            assertEquals(iter1.current(), iter2.current());
            assertEquals(iter1.indexOfCurrent(), iter2.indexOfCurrent());
        }
        assertIteratorExceptionEnd(iter1);
        assertIteratorExceptionEnd(iter2);
    }

    @Test
    public void testTreeIteratorNormal()
    {
        Object[] btree = BTree.build(seq(30));
        BTreeSearchIterator fullIter = new FullBTreeSearchIterator<>(btree, CMP, Dir.ASC);
        BTreeSearchIterator leafIter = new LeafBTreeSearchIterator<>(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, -8);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 100);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3, 4);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 4, 3);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, -8, 3, 100);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 0, 29, 30, 0);

        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 100);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, -8);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 4, 3);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 100, 3, -8);
    }

    @Test
    public void testTreeIteratorOneElem()
    {
        Object[] btree = BTree.build(seq(1));
        BTreeSearchIterator fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        BTreeSearchIterator leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 0);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);

        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 0);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3);
    }

    @Test
    public void testTreeIteratorEmpty()
    {
        BTreeSearchIterator leafIter = new LeafBTreeSearchIterator(BTree.empty(), CMP, Dir.ASC);
        assertFalse(leafIter.hasNext());
        leafIter = new LeafBTreeSearchIterator(BTree.empty(), CMP, Dir.DESC);
        assertFalse(leafIter.hasNext());
    }

    @Test
    public void testTreeIteratorNotFound()
    {
        Object[] btree = BTree.build(seq(31, 0, 3));
        BTreeSearchIterator fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        BTreeSearchIterator leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3 * 5 + 1);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3 * 5 + 1, 3 * 7);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.ASC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.ASC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3 * 5 + 1, 3 * 7 + 1);

        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3 * 5 + 1);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3 * 5 + 1, 3 * 2);
        fullIter = new FullBTreeSearchIterator(btree, CMP, Dir.DESC);
        leafIter = new LeafBTreeSearchIterator(btree, CMP, Dir.DESC);
        assertBTreeSearchIteratorEquals(fullIter, leafIter, 3 * 5 + 1, 3 * 2 + 1);
    }
}
