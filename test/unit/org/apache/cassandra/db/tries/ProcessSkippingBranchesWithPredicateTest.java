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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for processSkippingBranches with acceptance predicate functionality added in commit d9b32443a8.
 */
public class ProcessSkippingBranchesWithPredicateTest
{
    private static class TestContent
    {
        final String value;
        final boolean shouldAccept;

        TestContent(String value, boolean shouldAccept)
        {
            this.value = value;
            this.shouldAccept = shouldAccept;
        }

        @Override
        public String toString()
        {
            return "TestContent{" + value + ", accept=" + shouldAccept + '}';
        }
    }

    private static ByteComparable bc(String s)
    {
        return ByteComparable.preencoded(VERSION, s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateAcceptsAll() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        trie.putSingleton(bc("a"), new TestContent("a", true), (x, y) -> y);
        trie.putSingleton(bc("ab"), new TestContent("ab", true), (x, y) -> y);
        trie.putSingleton(bc("abc"), new TestContent("abc", true), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", true), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        assertEquals(Arrays.asList("a", "b"), collected);
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateRejectsAll() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        trie.putSingleton(bc("a"), new TestContent("a", false), (x, y) -> y);
        trie.putSingleton(bc("ab"), new TestContent("ab", false), (x, y) -> y);
        trie.putSingleton(bc("abc"), new TestContent("abc", false), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", false), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        assertTrue("Should collect nothing when all rejected", collected.isEmpty());
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateSelectiveAcceptance() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        trie.putSingleton(bc("a"), new TestContent("a", true), (x, y) -> y);
        trie.putSingleton(bc("ab"), new TestContent("ab", false), (x, y) -> y);
        trie.putSingleton(bc("abc"), new TestContent("abc", true), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", false), (x, y) -> y);
        trie.putSingleton(bc("c"), new TestContent("c", true), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        // Should collect "a" (accepted, skips branch including "ab" and "abc"), then "c" (accepted)
        // "b" is rejected so we skip its branch
        assertEquals(Arrays.asList("a", "c"), collected);
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateReverseDirection() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        trie.putSingleton(bc("a"), new TestContent("a", true), (x, y) -> y);
        trie.putSingleton(bc("ab"), new TestContent("ab", false), (x, y) -> y);
        trie.putSingleton(bc("abc"), new TestContent("abc", true), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", true), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.REVERSE, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        // In reverse: "b" (accepted, skips branch), then "a" (accepted, skips branch including "ab" and "abc")
        assertEquals(Arrays.asList("b", "a"), collected);
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateEmptyTrie() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        assertTrue("Empty trie should produce no results", collected.isEmpty());
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateRootContent() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        // Add root content
        trie.putSingleton(bc(""), new TestContent("root", true), (x, y) -> y);
        trie.putSingleton(bc("a"), new TestContent("a", false), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", true), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        // Root is accepted and should skip all branches
        assertEquals(Arrays.asList("root"), collected);
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateRootContentRejected() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        // Add root content that will be rejected
        trie.putSingleton(bc(""), new TestContent("root", false), (x, y) -> y);
        trie.putSingleton(bc("a"), new TestContent("a", true), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", true), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        // Root is rejected, so we continue to children
        assertEquals(Arrays.asList("a", "b"), collected);
    }

    @Test
    public void testForEachEntrySkippingBranchesWithPredicate() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        trie.putSingleton(bc("a"), new TestContent("a", true), (x, y) -> y);
        trie.putSingleton(bc("ab"), new TestContent("ab", false), (x, y) -> y);
        trie.putSingleton(bc("abc"), new TestContent("abc", true), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", true), (x, y) -> y);

        List<String> collectedKeys = new ArrayList<>();
        List<String> collectedValues = new ArrayList<>();
        
        trie.forEachEntrySkippingBranches(Direction.FORWARD,
                                          content -> content.shouldAccept,
                                          (key, content) -> {
                                              collectedKeys.add(new String(key.asByteComparableArray(VERSION), java.nio.charset.StandardCharsets.UTF_8));
                                              collectedValues.add(content.value);
                                          });

        // "a" accepted (skips branch including "ab" and "abc"), "b" accepted (skips branch)
        assertEquals(Arrays.asList("a", "b"), collectedKeys);
        assertEquals(Arrays.asList("a", "b"), collectedValues);
    }

    @Test
    public void testProcessSkippingBranchesWithPredicateComplexPaths() throws Exception
    {
        InMemoryTrie<TestContent> trie = InMemoryTrie.shortLived(VERSION);
        
        // Create a more complex tree structure
        trie.putSingleton(bc("aa"), new TestContent("aa", true), (x, y) -> y);
        trie.putSingleton(bc("aaa"), new TestContent("aaa", false), (x, y) -> y);
        trie.putSingleton(bc("aaaa"), new TestContent("aaaa", true), (x, y) -> y);
        trie.putSingleton(bc("aab"), new TestContent("aab", true), (x, y) -> y);
        trie.putSingleton(bc("ab"), new TestContent("ab", false), (x, y) -> y);
        trie.putSingleton(bc("aba"), new TestContent("aba", true), (x, y) -> y);
        trie.putSingleton(bc("b"), new TestContent("b", true), (x, y) -> y);

        List<String> collected = new ArrayList<>();
        trie.processSkippingBranches(Direction.FORWARD, 
                                     content -> content.shouldAccept,
                                     new Trie.ValueConsumer<TestContent>()
                                     {
                                         @Override
                                         public void content(TestContent content)
                                         {
                                             collected.add(content.value);
                                         }
                                     });

        // "aa" accepted (skips entire branch including "aaa", "aaaa", "aab"), "aba" accepted, "b" accepted
        // When a node is accepted, ALL its descendants are skipped
        assertEquals(Arrays.asList("aa", "aba", "b"), collected);
    }
}
