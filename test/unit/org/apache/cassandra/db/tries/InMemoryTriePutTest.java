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

package org.apache.cassandra.db.tries;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.fail;

public class InMemoryTriePutTest extends InMemoryTrieTestBase
{
    @Override
    boolean usePut()
    {
        return true;
    }

    @Test
    public void testLongKey_StackOverflow() throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<String> trie = new InMemoryTrie<>(BufferType.ON_HEAP);
        Random rand = new Random(1);
        byte[] key = new byte[40960];
        rand.nextBytes(key);
        ByteBuffer buf = ByteBuffer.wrap(key);

        try
        {
            trie.putRecursive(ByteComparable.fixedLength(buf), "value", (x, y) -> y);
            Assert.fail("StackOverflowError expected with a recursive put for very long keys!");
        }
        catch (StackOverflowError soe)
        {
            // Expected.
        }
        // Using non-recursive put should work.
        putSimpleResolve(trie, ByteComparable.fixedLength(buf), "value", (x, y) -> y, false);
    }

    // This tests that trie space allocation works correctly close to the 2G limit. It is normally disabled because
    // the test machines don't provide enough heap memory (test requires ~8G heap to finish). Run it manually when
    // InMemoryTrie.allocateBlock is modified.
    @Ignore
    @Test
    public void testOver1GSize() throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<String> trie = new InMemoryTrie<>(BufferType.ON_HEAP);
        trie.advanceAllocatedPos(0x20000000);
        String t1 = "test1";
        String t2 = "testing2";
        String t3 = "onemoretest3";
        trie.putRecursive(ByteComparable.of(t1), t1, (x, y) -> y);
        Assert.assertEquals(t1, trie.get(ByteComparable.of(t1)));
        Assert.assertNull(trie.get(ByteComparable.of(t2)));
        Assert.assertFalse(trie.reachedAllocatedSizeThreshold());

        trie.advanceAllocatedPos(InMemoryTrie.ALLOCATED_SIZE_THRESHOLD + 0x1000);
        trie.putRecursive(ByteComparable.of(t2), t2, (x, y) -> y);
        Assert.assertEquals(t1, trie.get(ByteComparable.of(t1)));
        Assert.assertEquals(t2, trie.get(ByteComparable.of(t2)));
        Assert.assertNull(trie.get(ByteComparable.of(t3)));
        Assert.assertTrue(trie.reachedAllocatedSizeThreshold());

        trie.advanceAllocatedPos(0x7FFFFEE0);  // close to 2G
        Assert.assertEquals(t1, trie.get(ByteComparable.of(t1)));
        Assert.assertEquals(t2, trie.get(ByteComparable.of(t2)));
        Assert.assertNull(trie.get(ByteComparable.of(t3)));
        Assert.assertTrue(trie.reachedAllocatedSizeThreshold());

        try
        {
            trie.putRecursive(ByteComparable.of(t3), t3, (x, y) -> y);  // should put it over the edge
            fail("InMemoryTrie.SpaceExhaustedError was expected");
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            // expected
        }

        Assert.assertEquals(t1, trie.get(ByteComparable.of(t1)));
        Assert.assertEquals(t2, trie.get(ByteComparable.of(t2)));
        Assert.assertNull(trie.get(ByteComparable.of(t3)));
        Assert.assertTrue(trie.reachedAllocatedSizeThreshold());

        try
        {
            trie.advanceAllocatedPos(Integer.MAX_VALUE);
            fail("InMemoryTrie.SpaceExhaustedError was expected");
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            // expected
        }

        Assert.assertEquals(t1, trie.get(ByteComparable.of(t1)));
        Assert.assertEquals(t2, trie.get(ByteComparable.of(t2)));
        Assert.assertNull(trie.get(ByteComparable.of(t3)));
        Assert.assertTrue(trie.reachedAllocatedSizeThreshold());

        trie.discardBuffers();
    }
}
