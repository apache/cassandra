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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Collections2;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/// Tests for internal methods of InMemoryBaseTrie that handle trie structure manipulation.
public class InMemoryBaseTrieInternalsTest
{
    private InMemoryTrie<String> trie;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        trie = InMemoryTrie.shortLived(ByteComparable.Version.OSS50);
    }

    @Test
    public void testAllocateCell() throws TrieSpaceExhaustedException
    {
        int cell1 = trie.allocateCell();
        int cell2 = trie.allocateCell();
        
        // Cells should be different addresses
        assertNotEquals("Cells should be different", cell1, cell2);
        // Both cells should be valid (non-negative in the buffer space)
        assertTrue("First cell should be >= 0", cell1 >= 0);
        assertTrue("Second cell should be >= 0", cell2 >= 0);

        checkZeroed(cell1);
        checkZeroed(cell2);
    }

    private void checkZeroed(int cell)
    {
        for (int i = 0; i < InMemoryReadTrie.CELL_SIZE; i += 4)
            assertEquals(0, trie.getIntVolatile(cell + i));
    }

    @Test
    public void testCopyCell() throws TrieSpaceExhaustedException
    {
        int originalCell = trie.allocateCell();
        
        // Write some test data to the original cell
        for (int i = 0; i < InMemoryReadTrie.CELL_SIZE; i += 4)
            trie.putIntVolatile(originalCell + i, 0x12345678 + i);

        int copiedCell = trie.copyCell(originalCell);
        
        assertTrue("Copied cell should be positive", copiedCell > 0);
        assertNotEquals("Copied cell should be different from original", originalCell, copiedCell);

        checkEqual(originalCell, copiedCell);
    }

    private void checkEqual(int originalCell, int copiedCell)
    {
        // Verify that the bytes were actually copied
        for (int i = 0; i < InMemoryReadTrie.CELL_SIZE; i += 4)
        {
            int originalValue = trie.getIntVolatile(originalCell + i);
            int copiedValue = trie.getIntVolatile(copiedCell + i);
            assertEquals("Byte content should be copied at offset " + i, originalValue, copiedValue);
        }
    }

    @Test
    public void testRecycleCell() throws TrieSpaceExhaustedException
    {
        int cell = trie.allocateCell();
        // Should not throw
        trie.recycleCell(cell);
    }

    @Test
    public void testCopyIfOriginalWhenSame() throws TrieSpaceExhaustedException
    {
        int originalCell = trie.allocateCell();
        
        // Write test data
        for (int i = 0; i < InMemoryReadTrie.CELL_SIZE; i += 4)
            trie.putIntVolatile(originalCell + i, 0x12345678 + i);

        // When node == originalNode, should copy
        int copiedCell = trie.copyIfOriginal(originalCell, originalCell);
        
        assertNotEquals("Should create a copy when node == originalNode", originalCell, copiedCell);
        
        // Verify bytes were copied
        checkEqual(originalCell, copiedCell);
    }

    @Test
    public void testCopyIfOriginalWhenDifferent() throws TrieSpaceExhaustedException
    {
        int node = trie.allocateCell();
        int originalNode = trie.allocateCell();
        
        // When node != originalNode, should return node unchanged
        int result = trie.copyIfOriginal(node, originalNode);
        
        assertEquals("Should return node unchanged when node != originalNode", node, result);
    }

    @Test
    public void testInsertInOrderWord()
    {
        // Test the actual behavior of insertInOrderWord
        // These values are based on the actual implementation
        assertEquals("Insert at position 0", Integer.valueOf("1203", 6).intValue(),
                     InMemoryBaseTrie.insertInOrderWord(Integer.valueOf("120", 6), 3, 0));
        assertEquals("Insert at position 1", Integer.valueOf("1230", 6).intValue(),
                     InMemoryBaseTrie.insertInOrderWord(Integer.valueOf("120", 6), 3, 1));
        assertEquals("Insert at position 2", Integer.valueOf("1320", 6).intValue(),
                     InMemoryBaseTrie.insertInOrderWord(Integer.valueOf("120", 6), 3, 2));
        assertEquals("Insert at position 3", Integer.valueOf("3120", 6).intValue(),
                     InMemoryBaseTrie.insertInOrderWord(Integer.valueOf("120", 6), 3, 3));
    }

    @Test
    public void testInsertInOrderWordEmpty()
    {
        // Insert into empty order word
        assertEquals("Insert into empty", 0, InMemoryBaseTrie.insertInOrderWord(0, 0, 0));
        assertEquals("Insert second element", 1, InMemoryBaseTrie.insertInOrderWord(0, 1, 0));
    }

    @Test
    public void testInsertInOrderWordMultiple()
    {
        // Build up an order word step by step
        int order = 0;
        order = InMemoryBaseTrie.insertInOrderWord(order, 0, 0);
        assertEquals(Integer.valueOf("0", 6).intValue(), order);
        
        order = InMemoryBaseTrie.insertInOrderWord(order, 1, 1);
        assertEquals(Integer.valueOf("10", 6).intValue(), order);
        
        order = InMemoryBaseTrie.insertInOrderWord(order, 2, 2);
        assertEquals(Integer.valueOf("210", 6).intValue(), order);

        order = InMemoryBaseTrie.insertInOrderWord(order, 3, 1);
        assertEquals(Integer.valueOf("2130", 6).intValue(), order);

        order = InMemoryBaseTrie.insertInOrderWord(order, 4, 4);
        assertEquals(Integer.valueOf("42130", 6).intValue(), order);

        order = InMemoryBaseTrie.insertInOrderWord(order, 5, 0);
        assertEquals(Integer.valueOf("421305", 6).intValue(), order);
    }

    @Test
    public void testInsertInOrderWordExhaustive()
    {
        for (int length = 2; length < 6; ++length)
        {
            List<Integer> ints = IntStream.range(0, length).mapToObj(Integer::valueOf).collect(Collectors.toList());
            for (List<Integer> permutation : Collections2.permutations(ints))
            {
                String asString = permutation.stream().map(Object::toString).collect(Collectors.joining());
                if (asString.startsWith("0")) // invalid order word, 0 must be last
                    continue;
                int asInt = Integer.valueOf(asString, 6).intValue();

                for (int position = 0; position <= length; ++position)
                {
                    String expected = asString.substring(0, length - position) +
                                      Integer.toString(length) +
                                      asString.substring(length - position);
                    int actual = InMemoryBaseTrie.insertInOrderWord(asInt, length, position);
                    assertEquals(Integer.valueOf(expected, 6).intValue(), actual);
                }
            }
        }
    }

    @Test
    public void testIsExpandableChainWithLeaf() throws TrieSpaceExhaustedException
    {
        // Leaf nodes (content) are not expandable
        int contentId = trie.addContent("test", false);
        assertFalse("Leaf nodes should not be expandable", trie.isExpandableChain(contentId));

        assertFalse("Leaf -1 should not be expandable", trie.isExpandableChain(-1));
        assertFalse("Leaf -7 should not be expandable", trie.isExpandableChain(-7));
        assertFalse("Leaf 0x80000000 should not be expandable", trie.isExpandableChain(Integer.MIN_VALUE));
        assertFalse("Leaf 0x80000001 should not be expandable", trie.isExpandableChain(0x80000001));
    }

    @Test
    public void testIsExpandableChainWithNone()
    {
        // NONE is not expandable
        assertFalse("NONE should not be expandable", trie.isExpandableChain(InMemoryReadTrie.NONE));
    }

    @Test
    public void testIsExpandableChainWithNonePlusOne()
    {
        // NONE + 1 (which expands to NONE) is not expandable
        assertFalse("NONE + 1 should not be expandable", trie.isExpandableChain(InMemoryReadTrie.NONE + 1));
    }

    @Test
    public void testIsExpandableChainWithOtherTypes()
    {
        assertFalse("Sparse should not be expandable", trie.isExpandableChain(InMemoryReadTrie.SPARSE_OFFSET + 0x760));
        assertFalse("Split should not be expandable", trie.isExpandableChain(InMemoryReadTrie.SPLIT_OFFSET + 0x123120));
        assertFalse("Prefix should not be expandable", trie.isExpandableChain(InMemoryReadTrie.PREFIX_OFFSET + 0xFEC0));
        assertFalse("Unknown should not be expandable", trie.isExpandableChain(InMemoryReadTrie.CELL_SIZE - 3 + 0x58FE0));
    }

    @Test
    public void testIsExpandableChainWithValidChainNode() throws TrieSpaceExhaustedException
    {
        // make sure we don't get NONE as the chain cell
        trie.allocateCell();
        // Create a chain node with room to expand
        int child = trie.addContent("test", false);

        int i;
        for (i = 1; i <= InMemoryReadTrie.CHAIN_MAX_OFFSET - InMemoryReadTrie.CHAIN_MIN_OFFSET; ++i)
        {
            int chainNode = trie.expandOrCreateChainNode(0x40 + i, child);
            assertEquals("Chain node with " + i + " characters should be expandable",
                         true, trie.isExpandableChain(chainNode));
            child = chainNode;
        }
        int chainNode = trie.expandOrCreateChainNode(0x40 + i, child);
        assertEquals("Chain node with " + i + " characters should not be expandable",
                     false, trie.isExpandableChain(chainNode));
    }

    @Test
    public void testExpandOrCreateChainNode() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        int chainNode = trie.expandOrCreateChainNode(0x41, contentId); // 'A'
        
        assertTrue("Chain node should be valid", chainNode > InMemoryReadTrie.NONE);
        assertFalse("Chain node should not be a leaf", InMemoryReadTrie.isLeaf(chainNode));
        
        // Verify the transition byte is stored correctly
        int transitionByte = trie.getUnsignedByte(chainNode);
        assertEquals("Transition byte should match", 0x41, transitionByte);
    }

    @Test
    public void testExpandOrCreateChainNodeExpansion() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        int chainNode1 = trie.expandOrCreateChainNode(0x41, contentId); // 'A'
        
        // If the chain is expandable, expanding it should return a different pointer
        if (trie.isExpandableChain(chainNode1))
        {
            int chainNode2 = trie.expandOrCreateChainNode(0x42, chainNode1); // 'B'
            assertNotEquals("Expanded chain should have different pointer", chainNode1, chainNode2);
            
            // The new node should be one position before the old one
            assertEquals("Expanded chain should be one position before", 
                        chainNode1 - 1, chainNode2);
        }
    }

    @Test
    public void testCreatePrefixNodeWithSplitChild() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        
        // Create a split node (empty)
        int splitNode = trie.allocateCell() + InMemoryReadTrie.SPLIT_OFFSET;
        
        // Create prefix node with split child (should create embedded prefix)
        int prefixNode = trie.createPrefixNode(contentId, InMemoryReadTrie.NONE, splitNode, false);
        
        assertTrue("Prefix node should be valid", prefixNode > InMemoryReadTrie.NONE);
        assertEquals("Prefix node should have PREFIX_OFFSET", 
                    InMemoryReadTrie.PREFIX_OFFSET, InMemoryReadTrie.offset(prefixNode));
        
        // Verify it's an embedded prefix node
        assertTrue("Should be embedded prefix node", trie.isEmbeddedPrefixNode(prefixNode));
    }

    @Test
    public void testCreatePrefixNodeWithChainChild() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        int leafContent = trie.addContent("leaf", false);
        
        // Create a chain node
        int chainNode = trie.expandOrCreateChainNode(0x41, leafContent);
        
        // Create prefix node with chain child
        int prefixNode = trie.createPrefixNode(contentId, InMemoryReadTrie.NONE, chainNode, true);
        
        assertTrue("Prefix node should be valid", prefixNode > InMemoryReadTrie.NONE);
        assertEquals("Prefix node should have PREFIX_OFFSET", 
                    InMemoryReadTrie.PREFIX_OFFSET, InMemoryReadTrie.offset(prefixNode));
    }

    @Test
    public void testCreatePrefixNodeWithAlternateBranch() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        int alternateContent = trie.addContent("alternate", false);
        int leafContent = trie.addContent("leaf", false);
        
        int chainNode = trie.expandOrCreateChainNode(0x41, leafContent);
        
        // Create prefix node with both content and alternate branch
        int prefixNode = trie.createPrefixNode(contentId, alternateContent, chainNode, false);
        
        assertTrue("Prefix node should be valid", prefixNode > InMemoryReadTrie.NONE);
        assertEquals("Prefix node should have PREFIX_OFFSET", 
                    InMemoryReadTrie.PREFIX_OFFSET, InMemoryReadTrie.offset(prefixNode));
        
        // Verify both content and alternate branch are stored
        int storedContent = trie.getIntVolatile(prefixNode + InMemoryReadTrie.PREFIX_CONTENT_OFFSET);
        int storedAlternate = trie.getIntVolatile(prefixNode + InMemoryReadTrie.PREFIX_ALTERNATE_OFFSET);
        
        assertEquals("Content should be stored", contentId, storedContent);
        assertEquals("Alternate branch should be stored", alternateContent, storedAlternate);
    }

    @Test
    public void testCreatePrefixNodeFullNode() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        
        // Create a sparse node (which cannot be embedded)
        int sparseNode = trie.allocateCell() + InMemoryReadTrie.SPARSE_OFFSET;
        
        // Create prefix node with sparse child (should create full prefix node)
        int prefixNode = trie.createPrefixNode(contentId, InMemoryReadTrie.NONE, sparseNode, false);
        
        assertTrue("Prefix node should be valid", prefixNode > InMemoryReadTrie.NONE);
        assertEquals("Prefix node should have PREFIX_OFFSET", 
                    InMemoryReadTrie.PREFIX_OFFSET, InMemoryReadTrie.offset(prefixNode));
        
        // Verify it's a full prefix node (not embedded)
        assertFalse("Should be full prefix node", trie.isEmbeddedPrefixNode(prefixNode));
        
        // Verify the child pointer is stored
        int storedChild = trie.getIntVolatile(prefixNode + InMemoryReadTrie.PREFIX_POINTER_OFFSET);
        assertEquals("Child pointer should be stored", sparseNode, storedChild);
    }

    @Test
    public void testIsEmbeddedPrefixNode() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        
        // Create embedded prefix node (with split child)
        int splitNode = trie.allocateCell() + InMemoryReadTrie.SPLIT_OFFSET;
        int embeddedPrefix = trie.createPrefixNode(contentId, InMemoryReadTrie.NONE, splitNode, false);
        
        assertTrue("Should be embedded prefix node", trie.isEmbeddedPrefixNode(embeddedPrefix));
        
        // Create full prefix node (with sparse child)
        int sparseNode = trie.allocateCell() + InMemoryReadTrie.SPARSE_OFFSET;
        int fullPrefix = trie.createPrefixNode(contentId, InMemoryReadTrie.NONE, sparseNode, false);
        
        assertFalse("Should be full prefix node", trie.isEmbeddedPrefixNode(fullPrefix));
    }

    @Test
    public void testCreatePrefixNodeWithNullChild() throws TrieSpaceExhaustedException
    {
        int contentId = trie.addContent("test", false);
        int alternateContent = trie.addContent("alternate", false);
        
        // Can create prefix node with null child if alternate branch is present
        int prefixNode = trie.createPrefixNode(contentId, alternateContent, InMemoryReadTrie.NONE, false);
        
        assertTrue("Prefix node should be valid", prefixNode > InMemoryReadTrie.NONE);
        assertEquals("Prefix node should have PREFIX_OFFSET", 
                    InMemoryReadTrie.PREFIX_OFFSET, InMemoryReadTrie.offset(prefixNode));
    }

    @Test
    public void testGetNextTransitionPastLastByte() throws TrieSpaceExhaustedException
    {
        // Walks advance to the next sibling with getNextTransition(node, transition + 1), which asks for 0x100 after
        // a 0xFF child. This must report no further children for all node types rather than wrap around.
        int[] transitions = new int[]{ 0x14, 0x33, 0x5d, 0x5f, 0xb0, 0xde, 0xff };
        for (int count = 1; count <= transitions.length; ++count)
        {
            trie = InMemoryTrie.shortLived(ByteComparable.Version.OSS50);
            int first = transitions.length - count;
            for (int i = first; i < transitions.length; ++i)
                trie.putRecursive(ByteComparable.preencoded(ByteComparable.Version.OSS50, new byte[]{ (byte) transitions[i] }),
                                  "v" + i,
                                  (x, y) -> y);

            int node = trie.followPrefixTransition(trie.root);
            int expectedType = count == 1 ? InMemoryReadTrie.CHAIN_MAX_OFFSET
                                          : count <= InMemoryReadTrie.SPARSE_CHILD_COUNT ? InMemoryReadTrie.SPARSE_OFFSET
                                                                                         : InMemoryReadTrie.SPLIT_OFFSET;
            assertEquals("Node type for " + count + " children", expectedType, InMemoryReadTrie.offset(node));

            assertEquals(transitions[first], trie.getNextTransition(node, 0));
            assertEquals(0xFF, trie.getNextTransition(node, 0xFF));
            assertEquals("Transition past 0xFF with " + count + " children",
                         Integer.MAX_VALUE, trie.getNextTransition(node, 0x100));
            assertEquals(InMemoryReadTrie.NONE, trie.getNextChild(node, 0x100));
        }
    }
}
