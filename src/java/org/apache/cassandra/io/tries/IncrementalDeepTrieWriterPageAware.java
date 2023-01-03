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
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * This class is a variant of {@link IncrementalTrieWriterPageAware} which is able to build even very deep
 * tries. While the parent class uses recursion for clarity, it may end up with stack overflow for tries with
 * very long keys. This implementation can switch processing from stack to heap at a certain depth (provided
 * as a constructor param).
 * <p>
 * This class intentionally repeats code present in the parent class, both in the in-stack and on-heap versions
 * of each of the three implemented recursive operations. Removing this repetition can cause higher stack usage
 * and thus stack overflow failures.
 */
@NotThreadSafe
public class IncrementalDeepTrieWriterPageAware<VALUE> extends IncrementalTrieWriterPageAware<VALUE>
{
    private final int maxRecursionDepth;

    public IncrementalDeepTrieWriterPageAware(TrieSerializer<VALUE, ? super DataOutputPlus> trieSerializer, DataOutputPlus dest, int maxRecursionDepth)
    {
        super(trieSerializer, dest);
        this.maxRecursionDepth = maxRecursionDepth;
    }

    public IncrementalDeepTrieWriterPageAware(TrieSerializer<VALUE, ? super DataOutputPlus> trieSerializer, DataOutputPlus dest)
    {
        this(trieSerializer, dest, 64);
    }

    /**
     * Simple framework for executing recursion using on-heap linked trace to avoid stack overruns.
     */
    static abstract class Recursion<NODE>
    {
        final Recursion<NODE> parent;
        final NODE node;
        final Iterator<NODE> childIterator;

        Recursion(NODE node, Iterator<NODE> childIterator, Recursion<NODE> parent)
        {
            this.parent = parent;
            this.node = node;
            this.childIterator = childIterator;
        }

        /**
         * Make a child Recursion object for the given node and initialize it as necessary to continue processing
         * with it.
         * <p>
         * May return null if the recursion does not need to continue inside the child branch.
         */
        abstract Recursion<NODE> makeChild(NODE child);

        /**
         * Complete the processing this Recursion object.
         * <p>
         * Note: this method is not called for the nodes for which makeChild() returns null.
         */
        abstract void complete() throws IOException;

        /**
         * Complete processing of the given child (possibly retrieve data to apply to any accumulation performed
         * in this Recursion object).
         * <p>
         * This is called when processing a child completes, including when recursion inside the child branch
         * is skipped by makeChild() returning null.
         */
        void completeChild(NODE child)
        {}

        /**
         * Recursive process, in depth-first order, the branch rooted at this recursion node.
         * <p>
         * Returns this.
         */
        Recursion<NODE> process() throws IOException
        {
            Recursion<NODE> curr = this;

            while (true)
            {
                if (curr.childIterator.hasNext())
                {
                    NODE child = curr.childIterator.next();
                    Recursion<NODE> childRec = curr.makeChild(child);
                    if (childRec != null)
                        curr = childRec;
                    else
                        curr.completeChild(child);
                }
                else
                {
                    curr.complete();
                    Recursion<NODE> currParent = curr.parent;
                    if (currParent == null)
                        return curr;
                    currParent.completeChild(curr.node);
                    curr = currParent;
                }
            }
        }
    }

    @Override
    protected int recalcTotalSize(Node<VALUE> node, long nodePosition) throws IOException
    {
        return recalcTotalSizeRecursiveOnStack(node, nodePosition, 0);
    }

    @SuppressWarnings("DuplicatedCode") // intentionally duplicates IncrementalTrieWriterPageAware
    private int recalcTotalSizeRecursiveOnStack(Node<VALUE> node, long nodePosition, int depth) throws IOException
    {
        if (node.hasOutOfPageInBranch)
        {
            int sz = 0;
            for (Node<VALUE> child : node.children)
            {
                if (depth < maxRecursionDepth)
                    sz += recalcTotalSizeRecursiveOnStack(child, nodePosition + sz, depth + 1);
                else
                    sz += recalcTotalSizeRecursiveOnHeap(child, nodePosition + sz);
            }
            node.branchSize = sz;
        }

        // The sizing below will use the branch size calculated above. Since that can change on out-of-page in branch,
        // we need to recalculate the size if either flag is set.
        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
            node.nodeSize = serializer.sizeofNode(node, nodePosition + node.branchSize);

        return node.branchSize + node.nodeSize;
    }

    private int recalcTotalSizeRecursiveOnHeap(Node<VALUE> node, long nodePosition) throws IOException
    {
        if (node.hasOutOfPageInBranch)
            new RecalcTotalSizeRecursion(node, null, nodePosition).process();

        if (node.hasOutOfPageChildren || node.hasOutOfPageInBranch)
            node.nodeSize = serializer.sizeofNode(node, nodePosition + node.branchSize);

        return node.branchSize + node.nodeSize;
    }

    class RecalcTotalSizeRecursion extends Recursion<Node<VALUE>>
    {
        final long nodePosition;
        int sz;

        RecalcTotalSizeRecursion(Node<VALUE> node, Recursion<Node<VALUE>> parent, long nodePosition)
        {
            super(node, node.children.iterator(), parent);
            sz = 0;
            this.nodePosition = nodePosition;
        }

        @Override
        Recursion<Node<VALUE>> makeChild(Node<VALUE> child)
        {
            if (child.hasOutOfPageInBranch)
                return new RecalcTotalSizeRecursion(child, this, nodePosition + sz);
            else
                return null;
        }

        @Override
        void complete()
        {
            node.branchSize = sz;
        }

        @SuppressWarnings("DuplicatedCode") // intentionally duplicates IncrementalTrieWriterPageAware and onStack code
        @Override
        void completeChild(Node<VALUE> child)
        {
            // This will be called for nodes that were recursively processed as well as the ones that weren't.

            // The sizing below will use the branch size calculated above. Since that can change on out-of-page in branch,
            // we need to recalculate the size if either flag is set.
            if (child.hasOutOfPageChildren || child.hasOutOfPageInBranch)
            {
                long childPosition = this.nodePosition + sz;
                child.nodeSize = serializer.sizeofNode(child, childPosition + child.branchSize);
            }

            sz += child.branchSize + child.nodeSize;
        }
    }

    @Override
    protected long write(Node<VALUE> node) throws IOException
    {
        return writeRecursiveOnStack(node, 0);
    }

    @SuppressWarnings("DuplicatedCode") // intentionally duplicates IncrementalTrieWriterPageAware
    private long writeRecursiveOnStack(Node<VALUE> node, int depth) throws IOException
    {
        long nodePosition = dest.position();
        for (Node<VALUE> child : node.children)
            if (child.filePos == -1)
            {
                if (depth < maxRecursionDepth)
                    child.filePos = writeRecursiveOnStack(child, depth + 1);
                else
                    child.filePos = writeRecursiveOnHeap(child);
            }

        nodePosition += node.branchSize;
        assert dest.position() == nodePosition
        : "Expected node position to be " + nodePosition + " but got " + dest.position() + " after writing children.\n" + dumpNode(node, dest.position());

        serializer.write(dest, node, nodePosition);

        assert dest.position() == nodePosition + node.nodeSize
               || dest.paddedPosition() == dest.position() // For PartitionIndexTest.testPointerGrowth where position may jump on page boundaries.
        : "Expected node position to be " + (nodePosition + node.nodeSize) + " but got " + dest.position() + " after writing node, nodeSize " + node.nodeSize + ".\n" + dumpNode(node, nodePosition);
        return nodePosition;
    }

    private long writeRecursiveOnHeap(Node<VALUE> node) throws IOException
    {
        return new WriteRecursion(node, null).process().node.filePos;
    }

    class WriteRecursion extends Recursion<Node<VALUE>>
    {
        long nodePosition;

        WriteRecursion(Node<VALUE> node, Recursion<Node<VALUE>> parent)
        {
            super(node, node.children.iterator(), parent);
            nodePosition = dest.position();
        }

        @Override
        Recursion<Node<VALUE>> makeChild(Node<VALUE> child)
        {
            if (child.filePos == -1)
                return new WriteRecursion(child, this);
            else
                return null;
        }

        @SuppressWarnings("DuplicatedCode") // intentionally duplicates IncrementalTrieWriterPageAware and onStack code
        @Override
        void complete() throws IOException
        {
            nodePosition = nodePosition + node.branchSize;
            assert dest.position() == nodePosition
                    : "Expected node position to be " + nodePosition + " but got " + dest.position() + " after writing children.\n" + dumpNode(node, dest.position());

            serializer.write(dest, node, nodePosition);

            assert dest.position() == nodePosition + node.nodeSize
                   || dest.paddedPosition() == dest.position() // For PartitionIndexTest.testPointerGrowth where position may jump on page boundaries.
                    : "Expected node position to be " + (nodePosition + node.nodeSize) + " but got " + dest.position() + " after writing node, nodeSize " + node.nodeSize + ".\n" + dumpNode(node, nodePosition);

            node.filePos = nodePosition;
        }
    }

    @Override
    protected long writePartial(Node<VALUE> node, DataOutputPlus dest, long baseOffset) throws IOException
    {
        return writePartialRecursiveOnStack(node, dest, baseOffset, 0);
    }

    @SuppressWarnings("DuplicatedCode") // intentionally duplicates IncrementalTrieWriterPageAware
    private long writePartialRecursiveOnStack(Node<VALUE> node, DataOutputPlus dest, long baseOffset, int depth) throws IOException
    {
        long startPosition = dest.position() + baseOffset;

        List<Node<VALUE>> childrenToClear = new ArrayList<>();
        for (Node<VALUE> child : node.children)
        {
            if (child.filePos == -1)
            {
                childrenToClear.add(child);
                if (depth < maxRecursionDepth)
                    child.filePos = writePartialRecursiveOnStack(child, dest, baseOffset, depth + 1);
                else
                    child.filePos = writePartialRecursiveOnHeap(child, dest, baseOffset);
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

    private long writePartialRecursiveOnHeap(Node<VALUE> node, DataOutputPlus dest, long baseOffset) throws IOException
    {
        new WritePartialRecursion(node, dest, baseOffset).process();
        long pos = node.filePos;
        node.filePos = -1;
        return pos;
    }

    class WritePartialRecursion extends Recursion<Node<VALUE>>
    {
        final DataOutputPlus dest;
        final long baseOffset;
        final long startPosition;
        final List<Node<VALUE>> childrenToClear;

        WritePartialRecursion(Node<VALUE> node, WritePartialRecursion parent)
        {
            super(node, node.children.iterator(), parent);
            this.dest = parent.dest;
            this.baseOffset = parent.baseOffset;
            this.startPosition = dest.position() + baseOffset;
            childrenToClear = new ArrayList<>();
        }

        WritePartialRecursion(Node<VALUE> node, DataOutputPlus dest, long baseOffset)
        {
            super(node, node.children.iterator(), null);
            this.dest = dest;
            this.baseOffset = baseOffset;
            this.startPosition = dest.position() + baseOffset;
            childrenToClear = new ArrayList<>();
        }

        @SuppressWarnings("DuplicatedCode") // intentionally duplicates IncrementalTrieWriterPageAware and onStack code
        @Override
        Recursion<Node<VALUE>> makeChild(Node<VALUE> child)
        {
            if (child.filePos == -1)
            {
                childrenToClear.add(child);
                return new WritePartialRecursion(child, this);
            }
            else
                return null;
        }

        @Override
        void complete() throws IOException
        {
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

            node.filePos = nodePosition;
        }
    }
}
