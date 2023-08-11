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
package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Incremental builders of on-disk tries which packs trie stages into disk cache pages.
 *
 * The incremental core is as in {@link IncrementalTrieWriterSimple}, which this augments by:
 * <ul>
 *   <li> calculating branch sizes reflecting the amount of data that needs to be written to store the trie
 *     branch rooted at each node
 *   <li> delaying writing any part of a completed node until its branch size is above the page size
 *   <li> laying out (some of) its children branches (each smaller than a page) to be contained within a page
 *   <li> adjusting the branch size to reflect the fact that the children are now written (i.e. removing their size)
 * </ul>
 * <p>
 * The process is bottom-up, i.e. pages are packed at the bottom and the root page is usually smaller.
 * This may appear less efficient than a top-down process which puts more information in the top pages that
 * tend to stay in cache, but in both cases performing a search will usually require an additional disk read
 * for the leaf page. When we maximize the amount of relevant data that read brings by using the bottom-up
 * process, we have practically the same efficiency with smaller intermediate page footprint, i.e. fewer data
 * to keep in cache.
 * <p>
 * As an example, taking a sample page size fitting 4 nodes, a simple trie would be split like this:
 * <pre>
 * Node 0 |
 *   -a-> | Node 1
 *        |   -s-> Node 2
 *        |          -k-> Node 3 (payload 1)
 *        |          -s-> Node 4 (payload 2)
 *        -----------------------------------
 *   -b-> Node 5 |
 *          -a-> |Node 6
 *               |  -n-> Node 7
 *               |         -k-> Node 8 (payload 3)
 *               |                -s-> Node 9 (payload 4)
 * </pre>
 * where lines denote page boundaries.
 * <p>
 * The process itself will start by adding "ask" which adds three nodes after the root to the stack. Adding "ass"
 * completes Node 3, setting its branch a size of 1 and replaces it on the stack with Node 4.
 * The step of adding "bank" starts by completing Node 4 (size 1), Node 2 (size 3), Node 1 (size 4), then adds 4 more
 * nodes to the stack. Adding "banks" descends one more node.
 * <p>
 * The trie completion step completes nodes 9 (size 1), 8 (size 2), 7 (size 3), 6 (size 4), 5 (size 5). Since the size
 * of node 5 is above the page size, the algorithm lays out its children. Nodes 6, 7, 8, 9 are written in order. The
 * size of node 5 is now just the size of it individually, 1. The process continues with completing Node 0 (size 6).
 * This is bigger than the page size, so some of its children need to be written. The algorithm takes the largest,
 * Node 1, and lays it out with its children in the file. Node 0 now has an adjusted size of 2 which is below the
 * page size, and we can continue the process.
 * <p>
 * Since this was the root of the trie, the current page is padded and the remaining nodes 0, 5 are written.
 */
@NotThreadSafe
public class IncrementalTrieWriterPageAware<VALUE>
extends IncrementalTrieWriterBase<VALUE, DataOutputPlus, IncrementalTrieWriterPageAware.Node<VALUE>>
implements IncrementalTrieWriter<VALUE>
{
    final int maxBytesPerPage;

    private final static Comparator<Node<?>> BRANCH_SIZE_COMPARATOR = (l, r) ->
    {
        // Smaller branches first.
        int c = Integer.compare(l.branchSize + l.nodeSize, r.branchSize + r.nodeSize);
        if (c != 0)
            return c;

        // Then order by character, which serves several purposes:
        // - enforces inequality to make sure equal sizes aren't treated as duplicates,
        // - makes sure the item we use for comparison key comes greater than all equal-sized nodes,
        // - orders equal sized items so that most recently processed (and potentially having closer children) comes
        //   last and is thus the first one picked for layout.
        c = Integer.compare(l.transition, r.transition);

        assert c != 0 || l == r;
        return c;
    };

    IncrementalTrieWriterPageAware(TrieSerializer<VALUE, ? super DataOutputPlus> trieSerializer, DataOutputPlus dest)
    {
        super(trieSerializer, dest, new Node<>((byte) 0));
        this.maxBytesPerPage = dest.maxBytesInPage();
    }

    @Override
    public void reset()
    {
        reset(new Node<>((byte) 0));
    }

    @Override
    Node<VALUE> performCompletion() throws IOException
    {
        Node<VALUE> root = super.performCompletion();

        int actualSize = recalcTotalSize(root, dest.position());
        int bytesLeft = dest.bytesLeftInPage();
        if (actualSize > bytesLeft)
        {
            if (actualSize <= maxBytesPerPage)
            {
                dest.padToPageBoundary();
                bytesLeft = maxBytesPerPage;
                // position changed, recalculate again
                actualSize = recalcTotalSize(root, dest.position());
            }

            if (actualSize > bytesLeft)
            {
                // Still greater. Lay out children separately.
                layoutChildren(root);

                // Pad if needed and place.
                if (root.nodeSize > dest.bytesLeftInPage())
                {
                    dest.padToPageBoundary();
                    // Recalculate again as pointer size may have changed, triggering assertion in writeRecursive.
                    recalcTotalSize(root, dest.position());
                }
            }
        }


        root.finalizeWithPosition(write(root));
        return root;
    }

    @Override
    void complete(Node<VALUE> node) throws IOException
    {
        assert node.filePos == -1;

        int branchSize = 0;
        for (Node<VALUE> child : node.children)
            branchSize += child.branchSize + child.nodeSize;

        node.branchSize = branchSize;

        int nodeSize = serializer.sizeofNode(node, dest.position());
        if (nodeSize + branchSize < maxBytesPerPage)
        {
            // Good. This node and all children will (most probably) fit page.
            node.nodeSize = nodeSize;
            node.hasOutOfPageChildren = false;
            node.hasOutOfPageInBranch = false;

            for (Node<VALUE> child : node.children)
                if (child.filePos != -1)
                    node.hasOutOfPageChildren = true;
                else if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
                    node.hasOutOfPageInBranch = true;

            return;
        }

        // Cannot fit. Lay out children; The current node will be marked as one with out-of-page children.
        layoutChildren(node);
    }

    private void layoutChildren(Node<VALUE> node) throws IOException
    {
        assert node.filePos == -1;

        NavigableSet<Node<VALUE>> children = node.getChildrenWithUnsetPosition();

        int bytesLeft = dest.bytesLeftInPage();
        Node<VALUE> cmp = new Node<>(256); // goes after all equal-sized unplaced nodes (whose transition character is 0-255)
        cmp.nodeSize = 0;
        while (!children.isEmpty())
        {
            cmp.branchSize = bytesLeft;
            Node<VALUE> child = children.headSet(cmp, true).pollLast();    // grab biggest that could fit
            if (child == null)
            {
                dest.padToPageBoundary();
                bytesLeft = maxBytesPerPage;
                child = children.pollLast();       // just biggest
            }

            assert child != null;
            if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
            {
                // We didn't know what size this branch will actually need to be, node's children may be far.
                // We now know where we would place it, so let's reevaluate size.
                int actualSize = recalcTotalSize(child, dest.position());
                if (actualSize > bytesLeft)
                {
                    if (bytesLeft == maxBytesPerPage)
                    {
                        // Branch doesn't even fit in a page.

                        // Note: In this situation we aren't actually making the best choice as the layout should have
                        // taken place at the child (which could have made the current parent small enough to fit).
                        // This is not trivial to fix but should be very rare.

                        layoutChildren(child);
                        bytesLeft = dest.bytesLeftInPage();

                        assert (child.filePos == -1);
                    }

                    // Doesn't fit, but that's probably because we don't have a full page. Put it back with the new
                    // size and retry when we do have enough space.
                    children.add(child);
                    continue;
                }
            }

            child.finalizeWithPosition(write(child));
            bytesLeft = dest.bytesLeftInPage();
        }

        // The sizing below will use the branch size, so make sure it's set.
        node.branchSize = 0;
        node.hasOutOfPageChildren = true;
        node.hasOutOfPageInBranch = false;
        node.nodeSize = serializer.sizeofNode(node, dest.position());
    }

    @SuppressWarnings("DuplicatedCode") // intentionally duplicated in IncrementalDeepTrieWriterPageAware
    protected int recalcTotalSize(Node<VALUE> node, long nodePosition) throws IOException
    {
        if (node.hasOutOfPageInBranch)
        {
            int sz = 0;
            for (Node<VALUE> child : node.children)
                sz += recalcTotalSize(child, nodePosition + sz);
            node.branchSize = sz;
        }

        // The sizing below will use the branch size calculated above. Since that can change on out-of-page in branch,
        // we need to recalculate the size if either flag is set.
        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
            node.nodeSize = serializer.sizeofNode(node, nodePosition + node.branchSize);

        return node.branchSize + node.nodeSize;
    }

    @SuppressWarnings("DuplicatedCode") // intentionally duplicated in IncrementalDeepTrieWriterPageAware
    protected long write(Node<VALUE> node) throws IOException
    {
        long nodePosition = dest.position();
        for (Node<VALUE> child : node.children)
            if (child.filePos == -1)
                child.filePos = write(child);

        nodePosition += node.branchSize;
        assert dest.position() == nodePosition
                : "Expected node position to be " + nodePosition + " but got " + dest.position() + " after writing children.\n" + dumpNode(node, dest.position());

        serializer.write(dest, node, nodePosition);

        assert dest.position() == nodePosition + node.nodeSize
                || dest.paddedPosition() == dest.position() // For PartitionIndexTest.testPointerGrowth where position may jump on page boundaries.
                : "Expected node position to be " + (nodePosition + node.nodeSize) + " but got " + dest.position() + " after writing node, nodeSize " + node.nodeSize + ".\n" + dumpNode(node, nodePosition);
        return nodePosition;
    }

    protected String dumpNode(Node<VALUE> node, long nodePosition)
    {
        StringBuilder res = new StringBuilder(String.format("At %,d(%x) type %s child count %s nodeSize %,d branchSize %,d %s%s%n",
                                                            nodePosition, nodePosition,
                                                            TrieNode.typeFor(node, nodePosition), node.childCount(), node.nodeSize, node.branchSize,
                                                            node.hasOutOfPageChildren ? "C" : "",
                                                            node.hasOutOfPageInBranch ? "B" : ""));
        for (Node<VALUE> child : node.children)
            res.append(String.format("Child %2x at %,d(%x) type %s child count %s size %s nodeSize %,d branchSize %,d %s%s%n",
                                     child.transition & 0xFF,
                                     child.filePos,
                                     child.filePos,
                                     child.children != null ? TrieNode.typeFor(child, child.filePos) : "n/a",
                                     child.children != null ? child.childCount() : "n/a",
                                     child.children != null ? serializer.sizeofNode(child, child.filePos) : "n/a",
                                     child.nodeSize,
                                     child.branchSize,
                                     child.hasOutOfPageChildren ? "C" : "",
                                     child.hasOutOfPageInBranch ? "B" : ""));

        return res.toString();
    }

    @Override
    public PartialTail makePartialRoot() throws IOException
    {
        // The expectation is that the partial tail will be in memory, so we don't bother with page-fitting.
        // We could also send some completed children to disk, but that could make suboptimal layout choices, so we'd
        // rather not. Just write anything not written yet to a buffer, from bottom to top, and we're done.
        try (DataOutputBuffer buf = new DataOutputBuffer())
        {
            PTail tail = new PTail();
            // Readers ask rebufferers for page-aligned positions, so make sure tail starts at one.
            // "Padding" of the cutoff point may leave some unaddressable space in the constructed file view.
            // Nothing will point to it, though, so that's fine.
            tail.cutoff = dest.paddedPosition();
            tail.count = count;
            tail.root = writePartial(stack.getFirst(), buf, tail.cutoff);
            tail.tail = buf.asNewBuffer();
            return tail;
        }
    }

    @SuppressWarnings("DuplicatedCode") // intentionally duplicated in IncrementalDeepTrieWriterPageAware
    protected long writePartial(Node<VALUE> node, DataOutputPlus dest, long baseOffset) throws IOException
    {
        long startPosition = dest.position() + baseOffset;

        List<Node<VALUE>> childrenToClear = new ArrayList<>();
        for (Node<VALUE> child : node.children)
        {
            if (child.filePos == -1)
            {
                childrenToClear.add(child);
                child.filePos = writePartial(child, dest, baseOffset);
            }
        }

        long nodePosition = dest.position() + baseOffset;

        if (node.hasOutOfPageInBranch)
        {
            // Update the branch size with the size of what we have just written. This may be used by the node's
            // maxPositionDelta, and it's a better approximation for later fitting calculations.
            node.branchSize = (int) (nodePosition - startPosition);
        }

        serializer.write(dest, node, nodePosition);

        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
        {
            // Update the node size with what we have just seen. It's a better approximation for later fitting
            // calculations.
            long endPosition = dest.position() + baseOffset;
            node.nodeSize = (int) (endPosition - nodePosition);
        }

        for (Node<VALUE> child : childrenToClear)
            child.filePos = -1;
        return nodePosition;
    }

    static class Node<Value> extends IncrementalTrieWriterBase.BaseNode<Value, Node<Value>>
    {
        /**
         * Currently calculated size of the branch below this node, not including the node itself.
         * If hasOutOfPageInBranch is true, this may be underestimated as the size
         * depends on the position the branch is written.
         */
        int branchSize = -1;
        /**
         * Currently calculated node size. If hasOutOfPageChildren is true, this may be underestimated as the size
         * depends on the position the node is written.
         */
        int nodeSize = -1;

        /**
         * Whether there is an out-of-page, already written node in the branches below the immediate children of the
         * node.
         */
        boolean hasOutOfPageInBranch = false;
        /**
         * Whether a child of the node is out of page, already written.
         * Forced to true before being set to make sure maxPositionDelta performs its evaluation on non-completed
         * nodes for makePartialRoot.
         */
        boolean hasOutOfPageChildren = true;

        Node(int transition)
        {
            super(transition);
        }

        @Override
        Node<Value> newNode(byte transition)
        {
            return new Node<>(transition & 0xFF);
        }

        public long serializedPositionDelta(int i, long nodePosition)
        {
            assert (children.get(i).filePos != -1);
            return children.get(i).filePos - nodePosition;
        }

        /**
         * The max delta is the delta with either:
         * - the position where the first child not-yet-placed child will be laid out.
         * - the position of the furthest child that is already placed.
         *
         * This method assumes all children's branch and node sizes, as well as this node's branchSize, are already
         * calculated.
         */
        public long maxPositionDelta(long nodePosition)
        {
            // The max delta is the position the first child would be laid out.
            assert (childCount() > 0);

            if (!hasOutOfPageChildren)
                // We need to be able to address the first child. We don't need to cover its branch, though.
                return -(branchSize - children.get(0).branchSize);

            long minPlaced = 0;
            long minUnplaced = 1;
            for (Node<Value> child : children)
            {
                if (child.filePos != -1)
                    minPlaced = Math.min(minPlaced, child.filePos - nodePosition);
                else if (minUnplaced > 0)   // triggers once
                    minUnplaced = -(branchSize - child.branchSize);
            }

            return Math.min(minPlaced, minUnplaced);
        }

        NavigableSet<Node<Value>> getChildrenWithUnsetPosition()
        {
            NavigableSet<Node<Value>> result = new TreeSet<>(BRANCH_SIZE_COMPARATOR);
            for (Node<Value> child : children)
                if (child.filePos == -1)
                    result.add(child);

            return result;
        }

        @Override
        void finalizeWithPosition(long position)
        {
            this.branchSize = 0;                // takes no space in current page
            this.nodeSize = 0;
            this.hasOutOfPageInBranch = false;  // its size no longer needs to be recalculated
            this.hasOutOfPageChildren = false;
            super.finalizeWithPosition(position);
        }

        @Override
        public String toString()
        {
            return String.format("%02x branchSize=%04x nodeSize=%04x %s%s", transition, branchSize, nodeSize, hasOutOfPageInBranch ? "B" : "", hasOutOfPageChildren ? "C" : "");
        }
    }
}
