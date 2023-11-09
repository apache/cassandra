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
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static java.lang.String.format;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.utils.ByteBufferUtil.compare;
import static org.apache.cassandra.utils.MerkleTree.Difference.*;

/**
 * A MerkleTree implemented as a binary tree.
 *
 * A MerkleTree is a full binary tree that represents a perfect binary tree of
 * depth 'hashdepth'. In a perfect binary tree, each leaf contains a
 * sequentially hashed range, and each inner node contains the binary hash of
 * its two children. In the MerkleTree, many ranges will not be split to the
 * full depth of the perfect binary tree: the leaves of this tree are Leaf objects,
 * which contain the computed values of the nodes that would be below them if
 * the tree were perfect.
 *
 * The hash values of the inner nodes of the MerkleTree are calculated lazily based
 * on their children when the hash of a range is requested with hash(range).
 *
 * Inputs passed to TreeRange.validate should be calculated using a very secure hash,
 * because all hashing internal to the tree is accomplished using XOR.
 *
 * If two MerkleTrees have the same hashdepth, they represent a perfect tree
 * of the same depth, and can always be compared, regardless of size or splits.
 */
public class MerkleTree
{
    private static final Logger logger = LoggerFactory.getLogger(MerkleTree.class);

    private static final int HASH_SIZE = 32; // 2xMM3_128 = 32 bytes.
    private static final byte[] EMPTY_HASH = new byte[HASH_SIZE];

    /*
     * Thread-local byte array, large enough to host 32B of digest or MM3/Random partitoners' tokens
     */
    private static final ThreadLocal<byte[]> byteArray = ThreadLocal.withInitial(() -> new byte[HASH_SIZE]);

    private static byte[] getTempArray(int minimumSize)
    {
        return minimumSize <= HASH_SIZE ? byteArray.get() : new byte[minimumSize];
    }

    public static final byte RECOMMENDED_DEPTH = Byte.MAX_VALUE - 1;

    private final int hashdepth;

    /** The top level range that this MerkleTree covers. */
    final Range<Token> fullRange;
    private final IPartitioner partitioner;

    private long maxsize;
    private long size;
    private Node root;

    /**
     * @param partitioner The partitioner in use.
     * @param range the range this tree covers
     * @param hashdepth The maximum depth of the tree. 100/(2^depth) is the %
     *        of the key space covered by each subrange of a fully populated tree.
     * @param maxsize The maximum number of subranges in the tree.
     */
    public MerkleTree(IPartitioner partitioner, Range<Token> range, int hashdepth, long maxsize)
    {
        this(new OnHeapLeaf(), partitioner, range, hashdepth, maxsize, 1);
    }

    /**
     * @param partitioner The partitioner in use.
     * @param range the range this tree covers
     * @param hashdepth The maximum depth of the tree. 100/(2^depth) is the %
     *        of the key space covered by each subrange of a fully populated tree.
     * @param maxsize The maximum number of subranges in the tree.
     * @param size The size of the tree. Typically 1, unless deserilized from an existing tree
     */
    private MerkleTree(Node root, IPartitioner partitioner, Range<Token> range, int hashdepth, long maxsize, long size)
    {
        assert hashdepth < Byte.MAX_VALUE;

        this.root = root;
        this.fullRange = Preconditions.checkNotNull(range);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.hashdepth = hashdepth;
        this.maxsize = maxsize;
        this.size = size;
    }

    /**
     * Initializes this tree by splitting it until hashdepth is reached,
     * or until an additional level of splits would violate maxsize.
     *
     * NB: Replaces all nodes in the tree, and always builds on the heap
     */
    public void init()
    {
        // determine the depth to which we can safely split the tree
        int sizedepth = (int) (Math.log10(maxsize) / Math.log10(2));
        int depth = Math.min(sizedepth, hashdepth);

        root = initHelper(fullRange.left, fullRange.right, 0, depth);
        size = (long) Math.pow(2, depth);
    }

    private OnHeapNode initHelper(Token left, Token right, int depth, int max)
    {
        if (depth == max)
            // we've reached the leaves
            return new OnHeapLeaf();
        Token midpoint = partitioner.midpoint(left, right);

        if (midpoint.equals(left) || midpoint.equals(right))
            return new OnHeapLeaf();

        OnHeapNode leftChild = initHelper(left, midpoint, depth + 1, max);
        OnHeapNode rightChild = initHelper(midpoint, right, depth + 1, max);
        return new OnHeapInner(midpoint, leftChild, rightChild);
    }

    public void release()
    {
        if (root instanceof OffHeapNode)
            ((OffHeapNode) root).release();
        root = null;
    }

    public IPartitioner partitioner()
    {
        return partitioner;
    }

    /**
     * The number of distinct ranges contained in this tree. This is a reasonable
     * measure of the memory usage of the tree (assuming 'this.order' is significant).
     */
    public long size()
    {
        return size;
    }

    public long maxsize()
    {
        return maxsize;
    }

    public void maxsize(long maxsize)
    {
        this.maxsize = maxsize;
    }

    /**
     * @param ltree First tree.
     * @param rtree Second tree.
     * @return A list of the largest contiguous ranges where the given trees disagree.
     */
    public static List<TreeRange> difference(MerkleTree ltree, MerkleTree rtree)
    {
        if (!ltree.fullRange.equals(rtree.fullRange))
            throw new IllegalArgumentException("Difference only make sense on tree covering the same range (but " + ltree.fullRange + " != " + rtree.fullRange + ')');

        // ensure on-heap trees' inner node hashes have been computed
        ltree.fillInnerHashes();
        rtree.fillInnerHashes();

        List<TreeRange> diff = new ArrayList<>();
        TreeRange active = new TreeRange(ltree.fullRange.left, ltree.fullRange.right, 0);

        Node lnode = ltree.root;
        Node rnode = rtree.root;

        if (lnode.hashesDiffer(rnode))
        {
            if (lnode instanceof Leaf || rnode instanceof Leaf)
            {
                logger.trace("Digest mismatch detected among leaf nodes {}, {}", lnode, rnode);
                diff.add(active);
            }
            else
            {
                logger.trace("Digest mismatch detected, traversing trees [{}, {}]", ltree, rtree);
                if (FULLY_INCONSISTENT == differenceHelper(ltree, rtree, diff, active))
                {
                    logger.trace("Range {} fully inconsistent", active);
                    diff.add(active);
                }
            }
        }

        return diff;
    }

    enum Difference { CONSISTENT, FULLY_INCONSISTENT, PARTIALLY_INCONSISTENT }

    /**
     * TODO: This function could be optimized into a depth first traversal of the two trees in parallel.
     *
     * Takes two trees and a range for which they have hashes, but are inconsistent.
     * @return FULLY_INCONSISTENT if active is inconsistent, PARTIALLY_INCONSISTENT if only a subrange is inconsistent.
     */
    @VisibleForTesting
    static Difference differenceHelper(MerkleTree ltree, MerkleTree rtree, List<TreeRange> diff, TreeRange active)
    {
        if (active.depth == Byte.MAX_VALUE)
            return CONSISTENT;

        Token midpoint = ltree.partitioner().midpoint(active.left, active.right);
        // sanity check for midpoint calculation, see CASSANDRA-13052
        if (midpoint.equals(active.left) || midpoint.equals(active.right))
        {
            // If the midpoint equals either the left or the right, we have a range that's too small to split - we'll simply report the
            // whole range as inconsistent
            logger.trace("({}) No sane midpoint ({}) for range {} , marking whole range as inconsistent", active.depth, midpoint, active);
            return FULLY_INCONSISTENT;
        }

        TreeRange left = new TreeRange(active.left, midpoint, active.depth + 1);
        TreeRange right = new TreeRange(midpoint, active.right, active.depth + 1);
        logger.trace("({}) Hashing sub-ranges [{}, {}] for {} divided by midpoint {}", active.depth, left, right, active, midpoint);
        Node lnode, rnode;

        // see if we should recurse left
        lnode = ltree.find(left);
        rnode = rtree.find(left);

        Difference ldiff = CONSISTENT;
        if (null != lnode && null != rnode && lnode.hashesDiffer(rnode))
        {
            logger.trace("({}) Inconsistent digest on left sub-range {}: [{}, {}]", active.depth, left, lnode, rnode);

            if (lnode instanceof Leaf)
                ldiff = FULLY_INCONSISTENT;
            else
                ldiff = differenceHelper(ltree, rtree, diff, left);
        }
        else if (null == lnode || null == rnode)
        {
            logger.trace("({}) Left sub-range fully inconsistent {}", active.depth, left);
            ldiff = FULLY_INCONSISTENT;
        }

        // see if we should recurse right
        lnode = ltree.find(right);
        rnode = rtree.find(right);

        Difference rdiff = CONSISTENT;
        if (null != lnode && null != rnode && lnode.hashesDiffer(rnode))
        {
            logger.trace("({}) Inconsistent digest on right sub-range {}: [{}, {}]", active.depth, right, lnode, rnode);

            if (rnode instanceof Leaf)
                rdiff = FULLY_INCONSISTENT;
            else
                rdiff = differenceHelper(ltree, rtree, diff, right);
        }
        else if (null == lnode || null == rnode)
        {
            logger.trace("({}) Right sub-range fully inconsistent {}", active.depth, right);
            rdiff = FULLY_INCONSISTENT;
        }

        if (ldiff == FULLY_INCONSISTENT && rdiff == FULLY_INCONSISTENT)
        {
            // both children are fully inconsistent
            logger.trace("({}) Fully inconsistent range [{}, {}]", active.depth, left, right);
            return FULLY_INCONSISTENT;
        }
        else if (ldiff == FULLY_INCONSISTENT)
        {
            logger.trace("({}) Adding left sub-range to diff as fully inconsistent {}", active.depth, left);
            diff.add(left);
            return PARTIALLY_INCONSISTENT;
        }
        else if (rdiff == FULLY_INCONSISTENT)
        {
            logger.trace("({}) Adding right sub-range to diff as fully inconsistent {}", active.depth, right);
            diff.add(right);
            return PARTIALLY_INCONSISTENT;
        }
        logger.trace("({}) Range {} partially inconstent", active.depth, active);
        return PARTIALLY_INCONSISTENT;
    }

    /**
     * Exceptions that stop recursion early when we are sure that no answer
     * can be found.
     */
    static abstract class StopRecursion extends Exception
    {
        static class  TooDeep extends StopRecursion {}
        static class BadRange extends StopRecursion {}
    }

    /**
     * Find the {@link Node} node that matches the given {@code range}.
     *
     * @param range Range to find
     * @return {@link Node} found. If nothing found, return {@code null}
     */
    @VisibleForTesting
    private Node find(Range<Token> range)
    {
        try
        {
            return findHelper(root, fullRange, range);
        }
        catch (StopRecursion e)
        {
            return null;
        }
    }

    /**
     * @throws StopRecursion If no match could be found for the range.
     */
    private Node findHelper(Node current, Range<Token> activeRange, Range<Token> find) throws StopRecursion
    {
        while (true)
        {
            if (current instanceof Leaf)
            {
                if (!find.contains(activeRange))
                    throw new StopRecursion.BadRange(); // we are not fully contained in this range!

                return current;
            }

            assert current instanceof Inner;
            Inner inner = (Inner) current;

            if (find.contains(activeRange)) // this node is fully contained in the range
                return inner.fillInnerHashes();

            Token midpoint = inner.token();
            Range<Token>  leftRange = new Range<>(activeRange.left, midpoint);
            Range<Token> rightRange = new Range<>(midpoint, activeRange.right);

            // else: one of our children contains the range

            if (leftRange.contains(find)) // left child contains/matches the range
            {
                activeRange = leftRange;
                current = inner.left();
            }
            else if (rightRange.contains(find)) // right child contains/matches the range
            {
                activeRange = rightRange;
                current = inner.right();
            }
            else
            {
                throw new StopRecursion.BadRange();
            }
        }
    }

    /**
     * Splits the range containing the given token, if no tree limits would be
     * violated. If the range would be split to a depth below hashdepth, or if
     * the tree already contains maxsize subranges, this operation will fail.
     *
     * @return True if the range was successfully split.
     */
    public boolean split(Token t)
    {
        if (size >= maxsize)
            return false;

        try
        {
            root = splitHelper(root, fullRange.left, fullRange.right, 0, t);
        }
        catch (StopRecursion.TooDeep e)
        {
            return false;
        }
        return true;
    }

    private OnHeapNode splitHelper(Node node, Token pleft, Token pright, int depth, Token t) throws StopRecursion.TooDeep
    {
        if (depth >= hashdepth)
            throw new StopRecursion.TooDeep();

        if (node instanceof Leaf)
        {
            Token midpoint = partitioner.midpoint(pleft, pright);

            // We should not create a non-sensical range where start and end are the same token (this is non-sensical because range are
            // start exclusive). Note that we shouldn't hit that unless the full range is very small or we are fairly deep
            if (midpoint.equals(pleft) || midpoint.equals(pright))
                throw new StopRecursion.TooDeep();

            // split
            size++;
            return new OnHeapInner(midpoint, new OnHeapLeaf(), new OnHeapLeaf());
        }
        // else: node.

        // recurse on the matching child
        assert node instanceof OnHeapInner;
        OnHeapInner inner = (OnHeapInner) node;

        if (Range.contains(pleft, inner.token(), t)) // left child contains token
            inner.left(splitHelper(inner.left(), pleft, inner.token(), depth + 1, t));
        else // else: right child contains token
            inner.right(splitHelper(inner.right(), inner.token(), pright, depth + 1, t));

        return inner;
    }

    /**
     * Returns a lazy iterator of invalid TreeRanges that need to be filled
     * in order to make the given Range valid.
     */
    TreeRangeIterator rangeIterator()
    {
        return new TreeRangeIterator(this);
    }

    EstimatedHistogram histogramOfRowSizePerLeaf()
    {
        HistogramBuilder histbuild = new HistogramBuilder();
        try (TreeRangeIterator trIter = new TreeRangeIterator(this))
        {
            for (TreeRange range : trIter)
            {
                histbuild.add(range.node.sizeOfRange());
            }
            return histbuild.buildWithStdevRangesAroundMean();
        }
    }

    EstimatedHistogram histogramOfRowCountPerLeaf()
    {
        HistogramBuilder histbuild = new HistogramBuilder();
        try (TreeRangeIterator trIter = new TreeRangeIterator(this))
        {
            for (TreeRange range : trIter)
            {
                histbuild.add(range.node.partitionsInRange());
            }
            return histbuild.buildWithStdevRangesAroundMean();
        }
    }

    public long rowCount()
    {
        long count = 0;
        try (TreeRangeIterator trIter = new TreeRangeIterator(this))
        {
            for (TreeRange range : trIter)
            {
                count += range.node.partitionsInRange();
            }
            return count;
        }
    }

    @Override
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<MerkleTree root=");
        root.toString(buff, 8);
        buff.append('>');
        return buff.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof MerkleTree))
            return false;
        MerkleTree that = (MerkleTree) other;

        return this.root.equals(that.root)
            && this.fullRange.equals(that.fullRange)
            && this.partitioner == that.partitioner
            && this.hashdepth == that.hashdepth
            && this.maxsize == that.maxsize
            && this.size == that.size;
    }

    /**
     * The public interface to a range in the tree.
     *
     * NB: A TreeRange should not be returned by a public method unless the
     * parents of the range it represents are already invalidated, since it
     * will allow someone to modify the hash. Alternatively, a TreeRange
     * may be created with a null tree, indicating that it is read only.
     */
    public static class TreeRange extends Range<Token>
    {
        private final MerkleTree tree;
        public final int depth;
        private final Node node;

        TreeRange(MerkleTree tree, Token left, Token right, int depth, Node node)
        {
            super(left, right);
            this.tree = tree;
            this.depth = depth;
            this.node = node;
        }

        TreeRange(Token left, Token right, int depth)
        {
            this(null, left, right, depth, null);
        }

        public void hash(byte[] hash)
        {
            assert tree != null : "Not intended for modification!";
            node.hash(hash);
        }

        /**
         * @param entry Row to mix into the hash for this range.
         */
        public void addHash(RowHash entry)
        {
            addHash(entry.hash, entry.size);
        }

        void addHash(byte[] hash, long partitionSize)
        {
            assert tree != null : "Not intended for modification!";

            assert node instanceof OnHeapLeaf;
            ((OnHeapLeaf) node).addHash(hash, partitionSize);
        }

        public void addAll(Iterator<RowHash> entries)
        {
            while (entries.hasNext()) addHash(entries.next());
        }

        @Override
        public String toString()
        {
            return "#<TreeRange " + super.toString() + " depth=" + depth + '>';
        }
    }

    /**
     * Returns the leaf (range) of a given tree in increasing order.
     * If the full range covered by the tree don't wrap, then it will return the
     * ranges in increasing order.
     * If the full range wrap, the first *and* last range returned by the
     * iterator will be the wrapping range. It is the only case where the same
     * leaf will be returned twice.
     */
    public static class TreeRangeIterator extends AbstractIterator<TreeRange> implements Iterable<TreeRange>, PeekingIterator<TreeRange>
    {
        // stack of ranges to visit
        private final ArrayDeque<TreeRange> tovisit;
        // interesting range
        private final MerkleTree tree;

        TreeRangeIterator(MerkleTree tree)
        {
            tovisit = new ArrayDeque<>();
            tovisit.add(new TreeRange(tree, tree.fullRange.left, tree.fullRange.right, 0, tree.root));
            this.tree = tree;
        }

        /**
         * Find the next TreeRange.
         *
         * @return The next TreeRange.
         */
        public TreeRange computeNext()
        {
            while (!tovisit.isEmpty())
            {
                TreeRange active = tovisit.pop();

                if (active.node instanceof Leaf)
                {
                    // found a leaf invalid range
                    if (active.isWrapAround() && !tovisit.isEmpty())
                        // put to be taken again last
                        tovisit.addLast(active);
                    return active;
                }

                Inner node = (Inner)active.node;
                TreeRange left = new TreeRange(tree, active.left, node.token(), active.depth + 1, node.left());
                TreeRange right = new TreeRange(tree, node.token(), active.right, active.depth + 1, node.right());

                if (right.isWrapAround())
                {
                    // whatever is on the left is 'after' everything we have seen so far (it has greater tokens)
                    tovisit.addLast(left);
                    tovisit.addFirst(right);
                }
                else
                {
                    // do left first then right
                    tovisit.addFirst(right);
                    tovisit.addFirst(left);
                }
            }
            return endOfData();
        }

        public Iterator<TreeRange> iterator()
        {
            return this;
        }
    }

    /**
     * Hash value representing a row, to be used to pass hashes to the MerkleTree.
     * The byte[] hash value should contain a digest of the key and value of the row
     * created using a very strong hash function.
     */
    public static class RowHash
    {
        public final Token token;
        public final byte[] hash;
        public final long size;

        public RowHash(Token token, byte[] hash, long size)
        {
            this.token = token;
            this.hash  = hash;
            this.size  = size;
        }

        @Override
        public String toString()
        {
            return "#<RowHash " + token + ' ' + (hash == null ? "null" : Hex.bytesToHex(hash)) + " @ " + size + " bytes>";
        }
    }

    public void serialize(DataOutputPlus out, int version) throws IOException
    {
        out.writeByte(hashdepth);
        out.writeLong(maxsize);
        out.writeLong(size);
        out.writeUTF(partitioner.getClass().getCanonicalName());
        Token.serializer.serialize(fullRange.left, out, version);
        Token.serializer.serialize(fullRange.right, out, version);
        root.serialize(out, version);
    }

    public long serializedSize(int version)
    {
        long size = 1 // mt.hashdepth
                  + sizeof(maxsize)
                  + sizeof(this.size)
                  + sizeof(partitioner.getClass().getCanonicalName());
        size += Token.serializer.serializedSize(fullRange.left, version);
        size += Token.serializer.serializedSize(fullRange.right, version);
        size += root.serializedSize(version);
        return size;
    }

    public static MerkleTree deserialize(DataInputPlus in, int version) throws IOException
    {
        return deserialize(in, DatabaseDescriptor.useOffheapMerkleTrees(), version);
    }

    public static MerkleTree deserialize(DataInputPlus in, boolean offHeapRequested, int version) throws IOException
    {
        int hashDepth = in.readByte();
        long maxSize = in.readLong();
        int innerNodeCount = Ints.checkedCast(in.readLong());

        IPartitioner partitioner;
        try
        {
            partitioner = FBUtilities.newPartitioner(in.readUTF());
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }

        Token left = Token.serializer.deserialize(in, partitioner, version);
        Token right = Token.serializer.deserialize(in, partitioner, version);
        Range<Token> fullRange = new Range<>(left, right);
        Node root = deserializeTree(in, partitioner, innerNodeCount, offHeapRequested, version);
        return new MerkleTree(root, partitioner, fullRange, hashDepth, maxSize, innerNodeCount);
    }

    private static boolean shouldUseOffHeapTrees(IPartitioner partitioner, boolean offHeapRequested)
    {
        boolean offHeapSupported = partitioner instanceof Murmur3Partitioner || partitioner instanceof RandomPartitioner;

        if (offHeapRequested && !offHeapSupported && !warnedOnce)
        {
            logger.warn("Configuration requests off-heap merkle trees, but partitioner does not support it. Ignoring.");
            warnedOnce = true;
        }

        return offHeapRequested && offHeapSupported;
    }
    private static boolean warnedOnce;

    private static ByteBuffer allocate(int innerNodeCount, IPartitioner partitioner)
    {
        int size = offHeapBufferSize(innerNodeCount, partitioner);
        logger.debug("Allocating direct buffer of size {} for an off-heap merkle tree", size);
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        if (Ref.DEBUG_ENABLED)
            MemoryUtil.setAttachment(buffer, new Ref.DirectBufferRef<>(null, null));
        return buffer;
    }

    private static Node deserializeTree(DataInputPlus in, IPartitioner partitioner, int innerNodeCount, boolean offHeapRequested, int version) throws IOException
    {
        return shouldUseOffHeapTrees(partitioner, offHeapRequested)
             ? deserializeOffHeap(in, partitioner, innerNodeCount, version)
             : OnHeapNode.deserialize(in, partitioner, version);
    }

    /*
     * Coordinating multiple trees from multiple replicas can get expensive.
     * On the deserialization path, we know in advance what the tree looks like,
     * So we can pre-size an offheap buffer and deserialize into that.
     */
    MerkleTree tryMoveOffHeap() throws IOException
    {
        return root instanceof OnHeapNode && shouldUseOffHeapTrees(partitioner, DatabaseDescriptor.useOffheapMerkleTrees())
             ? moveOffHeap()
             : this;
    }

    @VisibleForTesting
    MerkleTree moveOffHeap() throws IOException
    {
        assert root instanceof OnHeapNode;
        root.fillInnerHashes(); // ensure on-heap trees' inner node hashes have been computed
        ByteBuffer buffer = allocate(Ints.checkedCast(size), partitioner);
        int pointer = ((OnHeapNode) root).serializeOffHeap(buffer, partitioner);
        OffHeapNode newRoot = fromPointer(pointer, buffer, partitioner);
        return new MerkleTree(newRoot, partitioner, fullRange, hashdepth, maxsize, size);
    }

    private static OffHeapNode deserializeOffHeap(DataInputPlus in, IPartitioner partitioner, int innerNodeCount, int version) throws IOException
    {
        ByteBuffer buffer = allocate(innerNodeCount, partitioner);
        int pointer = OffHeapNode.deserialize(in, buffer, partitioner, version);
        return fromPointer(pointer, buffer, partitioner);
    }

    private static OffHeapNode fromPointer(int pointer, ByteBuffer buffer, IPartitioner partitioner)
    {
        return pointer >= 0 ? new OffHeapInner(buffer, pointer, partitioner) : new OffHeapLeaf(buffer, ~pointer);
    }

    private static int offHeapBufferSize(int innerNodeCount, IPartitioner partitioner)
    {
        return innerNodeCount * OffHeapInner.maxOffHeapSize(partitioner) + (innerNodeCount + 1) * OffHeapLeaf.maxOffHeapSize();
    }

    interface Node
    {
        byte[] hash();

        boolean hasEmptyHash();

        void hash(byte[] hash);

        boolean hashesDiffer(Node other);

        default Node fillInnerHashes()
        {
            return this;
        }

        default long sizeOfRange()
        {
            return 0;
        }

        default long partitionsInRange()
        {
            return 0;
        }

        void serialize(DataOutputPlus out, int version) throws IOException;
        int serializedSize(int version);

        void toString(StringBuilder buff, int maxdepth);

        static String toString(byte[] hash)
        {
            return hash == null
                 ? "null"
                 : '[' + Hex.bytesToHex(hash) + ']';
        }

        boolean equals(Node node);
    }

    static abstract class OnHeapNode implements Node
    {
        long sizeOfRange;
        long partitionsInRange;

        protected byte[] hash;

        OnHeapNode(byte[] hash)
        {
            if (hash == null)
                throw new IllegalArgumentException();

            this.hash = hash;
        }

        public byte[] hash()
        {
            return hash;
        }

        public boolean hasEmptyHash()
        {
            //noinspection ArrayEquality
            return hash == EMPTY_HASH;
        }

        public void hash(byte[] hash)
        {
            if (hash == null)
                throw new IllegalArgumentException();

            this.hash = hash;
        }

        public boolean hashesDiffer(Node other)
        {
            return other instanceof OnHeapNode
                 ? hashesDiffer( (OnHeapNode) other)
                 : hashesDiffer((OffHeapNode) other);
        }

        private boolean hashesDiffer(OnHeapNode other)
        {
            return !Arrays.equals(hash(), other.hash());
        }

        private boolean hashesDiffer(OffHeapNode other)
        {
            return compare(hash(), other.buffer(), other.hashBytesOffset(), HASH_SIZE) != 0;
        }

        @Override
        public long sizeOfRange()
        {
            return sizeOfRange;
        }

        @Override
        public long partitionsInRange()
        {
            return partitionsInRange;
        }

        static OnHeapNode deserialize(DataInputPlus in, IPartitioner p, int version) throws IOException
        {
            byte ident = in.readByte();

            switch (ident)
            {
                case Inner.IDENT:
                    return OnHeapInner.deserializeWithoutIdent(in, p, version);
                case Leaf.IDENT:
                    return OnHeapLeaf.deserializeWithoutIdent(in);
                default:
                    throw new IOException("Unexpected node type: " + ident);
            }
        }

        abstract int serializeOffHeap(ByteBuffer buffer, IPartitioner p) throws IOException;
    }

    static abstract class OffHeapNode implements Node
    {
        protected final ByteBuffer buffer;
        protected final int offset;

        OffHeapNode(ByteBuffer buffer, int offset)
        {
            this.buffer = buffer;
            this.offset = offset;
        }

        ByteBuffer buffer()
        {
            return buffer;
        }

        public byte[] hash()
        {
            final int position = buffer.position();
            buffer.position(hashBytesOffset());
            byte[] array = new byte[HASH_SIZE];
            buffer.get(array);
            buffer.position(position);
            return array;
        }

        public boolean hasEmptyHash()
        {
            return compare(buffer(), hashBytesOffset(), HASH_SIZE, EMPTY_HASH) == 0;
        }

        public void hash(byte[] hash)
        {
            throw new UnsupportedOperationException();
        }

        public boolean hashesDiffer(Node other)
        {
            return other instanceof OnHeapNode
                 ? hashesDiffer((OnHeapNode) other)
                 : hashesDiffer((OffHeapNode) other);
        }

        private boolean hashesDiffer(OnHeapNode other)
        {
            return compare(buffer(), hashBytesOffset(), HASH_SIZE, other.hash()) != 0;
        }

        private boolean hashesDiffer(OffHeapNode other)
        {
            int thisOffset = hashBytesOffset();
            int otherOffset = other.hashBytesOffset();

            for (int i = 0; i < HASH_SIZE; i += 8)
                if (buffer().getLong(thisOffset + i) != other.buffer().getLong(otherOffset + i))
                    return true;

            return false;
        }

        void release()
        {
            Object attachment = MemoryUtil.getAttachment(buffer);
            if (attachment instanceof Ref.DirectBufferRef)
                ((Ref.DirectBufferRef) attachment).release();
            FileUtils.clean(buffer);
        }

        abstract int hashBytesOffset();

        static int deserialize(DataInputPlus in, ByteBuffer buffer, IPartitioner p, int version) throws IOException
        {
            byte ident = in.readByte();

            switch (ident)
            {
                case Inner.IDENT:
                    return OffHeapInner.deserializeWithoutIdent(in, buffer, p, version);
                case Leaf.IDENT:
                    return  OffHeapLeaf.deserializeWithoutIdent(in, buffer);
                default:
                    throw new IOException("Unexpected node type: " + ident);
            }
        }
    }

    /**
     * A leaf node in the MerkleTree. Because the MerkleTree represents a much
     * larger perfect binary tree of depth hashdepth, a Leaf object contains
     * the value that would be contained in the perfect tree at its position.
     *
     * When rows are added to the MerkleTree using TreeRange.validate(), the
     * tree extending below the Leaf is generated in memory, but only the root
     * is stored in the Leaf.
     */
    interface Leaf extends Node
    {
        static final byte IDENT = 1;

        default void serialize(DataOutputPlus out, int version) throws IOException
        {
            byte[] hash = hash();
            assert hash.length == HASH_SIZE: String.format("Expected hash length to be %d, but given %d", HASH_SIZE, hash.length);

            out.writeByte(Leaf.IDENT);

            if (!hasEmptyHash())
            {
                out.writeByte(HASH_SIZE);
                out.write(hash);
            }
            else
            {
                out.writeByte(0);
            }
        }

        default int serializedSize(int version)
        {
            return 2 + (hasEmptyHash() ? 0 : HASH_SIZE);
        }

        default void toString(StringBuilder buff, int maxdepth)
        {
            buff.append(toString());
        }

        default boolean equals(Node other)
        {
            return other instanceof Leaf && !hashesDiffer(other);
        }
    }

    static class OnHeapLeaf extends OnHeapNode implements Leaf
    {
        OnHeapLeaf()
        {
            super(EMPTY_HASH);
        }

        OnHeapLeaf(byte[] hash)
        {
            super(hash);
        }

        /**
         * Mixes the given value into our hash. If our hash is null,
         * our hash will become the given value.
         */
        void addHash(byte[] partitionHash, long partitionSize)
        {
            if (hasEmptyHash())
                hash(partitionHash);
            else
                xorOntoLeft(hash, partitionHash);

            sizeOfRange += partitionSize;
            partitionsInRange += 1;
        }

        static OnHeapLeaf deserializeWithoutIdent(DataInputPlus in) throws IOException
        {
            int size = in.readByte();
            switch (size)
            {
                case HASH_SIZE:
                    byte[] hash = new byte[HASH_SIZE];
                    in.readFully(hash);
                    return new OnHeapLeaf(hash);
                case 0:
                    return new OnHeapLeaf();
                default:
                    throw new IllegalStateException(format("Hash of size %d encountered, expecting %d or %d", size, HASH_SIZE, 0));
            }
        }

        int serializeOffHeap(ByteBuffer buffer, IPartitioner p)
        {
            if (buffer.remaining() < OffHeapLeaf.maxOffHeapSize())
                throw new IllegalStateException("Insufficient remaining bytes to deserialize a Leaf node off-heap");

            if (hash.length != HASH_SIZE)
                throw new IllegalArgumentException("Hash of unexpected size when serializing a Leaf off-heap: " + hash.length);

            final int position = buffer.position();
            buffer.put(hash);
            return ~position;
        }

        @Override
        public String toString()
        {
            return "#<OnHeapLeaf " + Node.toString(hash()) + '>';
        }
    }

    static class OffHeapLeaf extends OffHeapNode implements Leaf
    {
        static final int HASH_BYTES_OFFSET = 0;

        OffHeapLeaf(ByteBuffer buffer, int offset)
        {
            super(buffer, offset);
        }

        public int hashBytesOffset()
        {
            return offset + HASH_BYTES_OFFSET;
        }

        static int deserializeWithoutIdent(DataInput in, ByteBuffer buffer) throws IOException
        {
            if (buffer.remaining() < maxOffHeapSize())
                throw new IllegalStateException("Insufficient remaining bytes to deserialize a Leaf node off-heap");

            final int position = buffer.position();

            int hashLength = in.readByte();
            if (hashLength > 0)
            {
                if (hashLength != HASH_SIZE)
                    throw new IllegalStateException("Hash of unexpected size when deserializing an off-heap Leaf node: " + hashLength);

                byte[] hashBytes = getTempArray(HASH_SIZE);
                in.readFully(hashBytes, 0, HASH_SIZE);
                buffer.put(hashBytes, 0, HASH_SIZE);
            }
            else
            {
                buffer.put(EMPTY_HASH, 0, HASH_SIZE);
            }

            return ~position;
        }

        static int maxOffHeapSize()
        {
            return HASH_SIZE;
        }

        @Override
        public String toString()
        {
            return "#<OffHeapLeaf " + Node.toString(hash()) + '>';
        }
    }

    /**
     * An inner node in the MerkleTree. Inners can contain cached hash values, which
     * are the binary hash of their two children.
     */
    interface Inner extends Node
    {
        static final byte IDENT = 2;

        public Token token();

        public Node left();
        public Node right();

        default void serialize(DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(Inner.IDENT);
            Token.serializer.serialize(token(), out, version);
            left().serialize(out, version);
            right().serialize(out, version);
        }

        default int serializedSize(int version)
        {
            return 1
                 + (int) Token.serializer.serializedSize(token(), version)
                 + left().serializedSize(version)
                 + right().serializedSize(version);
        }

        default void toString(StringBuilder buff, int maxdepth)
        {
            buff.append("#<").append(getClass().getSimpleName())
                .append(' ').append(token())
                .append(" hash=").append(Node.toString(hash()))
                .append(" children=[");

            if (maxdepth < 1)
            {
                buff.append('#');
            }
            else
            {
                Node left = left();
                if (left == null)
                    buff.append("null");
                else
                    left.toString(buff, maxdepth - 1);

                buff.append(' ');

                Node right = right();
                if (right == null)
                    buff.append("null");
                else
                    right.toString(buff, maxdepth - 1);
            }

            buff.append("]>");
        }

        default boolean equals(Node other)
        {
            if (!(other instanceof Inner))
                return false;
            Inner that = (Inner) other;
            return !hashesDiffer(other) && this.left().equals(that.left()) && this.right().equals(that.right());
        }

        default void unsafeInvalidate()
        {
        }
    }

    static class OnHeapInner extends OnHeapNode implements Inner
    {
        private final Token token;

        private OnHeapNode left;
        private OnHeapNode right;

        private boolean computed;

        OnHeapInner(Token token, OnHeapNode left, OnHeapNode right)
        {
            super(EMPTY_HASH);

            this.token = token;
            this.left = left;
            this.right = right;
        }

        public Token token()
        {
            return token;
        }

        public OnHeapNode left()
        {
            return left;
        }

        public OnHeapNode right()
        {
            return right;
        }

        void left(OnHeapNode child)
        {
            left = child;
        }

        void right(OnHeapNode child)
        {
            right = child;
        }

        @Override
        public Node fillInnerHashes()
        {
            if (!computed) // hash and size haven't been calculated; compute children then compute this
            {
                left.fillInnerHashes();
                right.fillInnerHashes();

                if (!left.hasEmptyHash() && !right.hasEmptyHash())
                    hash = xor(left.hash(), right.hash());
                else if (left.hasEmptyHash())
                    hash = right.hash();
                else if (right.hasEmptyHash())
                    hash = left.hash();

                sizeOfRange       = left.sizeOfRange()       + right.sizeOfRange();
                partitionsInRange = left.partitionsInRange() + right.partitionsInRange();

                computed = true;
            }

            return this;
        }

        static OnHeapInner deserializeWithoutIdent(DataInputPlus in, IPartitioner p, int version) throws IOException
        {
            Token token = Token.serializer.deserialize(in, p, version);
            OnHeapNode  left = OnHeapNode.deserialize(in, p, version);
            OnHeapNode right = OnHeapNode.deserialize(in, p, version);
            return new OnHeapInner(token, left, right);
        }

        int serializeOffHeap(ByteBuffer buffer, IPartitioner partitioner) throws IOException
        {
            if (buffer.remaining() < OffHeapInner.maxOffHeapSize(partitioner))
                throw new IllegalStateException("Insufficient remaining bytes to deserialize Inner node off-heap");

            final int offset = buffer.position();

            int tokenSize = partitioner.getTokenFactory().byteSize(token);
            buffer.putShort(offset + OffHeapInner.TOKEN_LENGTH_OFFSET, Shorts.checkedCast(tokenSize));
            buffer.position(offset + OffHeapInner.TOKEN_BYTES_OFFSET);
            partitioner.getTokenFactory().serialize(token, buffer);

            int  leftPointer =  left.serializeOffHeap(buffer, partitioner);
            int rightPointer = right.serializeOffHeap(buffer, partitioner);

            buffer.putInt(offset + OffHeapInner.LEFT_CHILD_POINTER_OFFSET,  leftPointer);
            buffer.putInt(offset + OffHeapInner.RIGHT_CHILD_POINTER_OFFSET, rightPointer);

            int  leftHashOffset = OffHeapInner.hashBytesOffset(leftPointer);
            int rightHashOffset = OffHeapInner.hashBytesOffset(rightPointer);

            for (int i = 0; i < HASH_SIZE; i += 8)
            {
                buffer.putLong(offset + OffHeapInner.HASH_BYTES_OFFSET + i,
                               buffer.getLong(leftHashOffset  + i) ^ buffer.getLong(rightHashOffset + i));
            }

            return offset;
        }

        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            toString(buff, 1);
            return buff.toString();
        }

        @Override
        public void unsafeInvalidate()
        {
            computed = false;
        }
    }

    static class OffHeapInner extends OffHeapNode implements Inner
    {
        /**
         * All we want to keep here is just a pointer to the start of the Inner leaf in the
         * direct buffer. From there, we'll be able to deserialize the following, in this order:
         *
         * 1. pointer to left child (int)
         * 2. pointer to right child (int)
         * 3. hash bytes (space allocated as HASH_MAX_SIZE)
         * 4. token length (short)
         * 5. token bytes (variable length)
         */
        static final int LEFT_CHILD_POINTER_OFFSET  = 0;
        static final int RIGHT_CHILD_POINTER_OFFSET = 4;
        static final int HASH_BYTES_OFFSET          = 8;
        static final int TOKEN_LENGTH_OFFSET        = 8 + HASH_SIZE;
        static final int TOKEN_BYTES_OFFSET         = TOKEN_LENGTH_OFFSET + 2;

        private final IPartitioner partitioner;

        OffHeapInner(ByteBuffer buffer, int offset, IPartitioner partitioner)
        {
            super(buffer, offset);
            this.partitioner = partitioner;
        }

        public Token token()
        {
            int length = buffer.getShort(offset + TOKEN_LENGTH_OFFSET);
            return partitioner.getTokenFactory().fromByteBuffer(buffer, offset + TOKEN_BYTES_OFFSET, length);
        }

        public Node left()
        {
            return child(LEFT_CHILD_POINTER_OFFSET);
        }

        public Node right()
        {
            return child(RIGHT_CHILD_POINTER_OFFSET);
        }

        private Node child(int childOffset)
        {
            int pointer = buffer.getInt(offset + childOffset);
            return pointer >= 0 ? new OffHeapInner(buffer, pointer, partitioner) : new OffHeapLeaf(buffer, ~pointer);
        }

        public int hashBytesOffset()
        {
            return offset + HASH_BYTES_OFFSET;
        }

        static int deserializeWithoutIdent(DataInputPlus in, ByteBuffer buffer, IPartitioner partitioner, int version) throws IOException
        {
            if (buffer.remaining() < maxOffHeapSize(partitioner))
                throw new IllegalStateException("Insufficient remaining bytes to deserialize Inner node off-heap");

            final int offset = buffer.position();

            int tokenSize = Token.serializer.deserializeSize(in);
            byte[] tokenBytes = getTempArray(tokenSize);
            in.readFully(tokenBytes, 0, tokenSize);

            buffer.putShort(offset + OffHeapInner.TOKEN_LENGTH_OFFSET, Shorts.checkedCast(tokenSize));
            buffer.position(offset + OffHeapInner.TOKEN_BYTES_OFFSET);
            buffer.put(tokenBytes, 0, tokenSize);

            int leftPointer  = OffHeapNode.deserialize(in, buffer, partitioner, version);
            int rightPointer = OffHeapNode.deserialize(in, buffer, partitioner, version);

            buffer.putInt(offset + OffHeapInner.LEFT_CHILD_POINTER_OFFSET,  leftPointer);
            buffer.putInt(offset + OffHeapInner.RIGHT_CHILD_POINTER_OFFSET, rightPointer);

            int leftHashOffset  = hashBytesOffset(leftPointer);
            int rightHashOffset = hashBytesOffset(rightPointer);

            for (int i = 0; i < HASH_SIZE; i += 8)
            {
                buffer.putLong(offset + OffHeapInner.HASH_BYTES_OFFSET + i,
                               buffer.getLong(leftHashOffset  + i) ^ buffer.getLong(rightHashOffset + i));
            }

            return offset;
        }

        static int maxOffHeapSize(IPartitioner partitioner)
        {
            return 4 // left pointer
                 + 4 // right pointer
                 + HASH_SIZE
                 + 2 + partitioner.getMaxTokenSize();
        }

        static int hashBytesOffset(int pointer)
        {
            return pointer >= 0 ? pointer + OffHeapInner.HASH_BYTES_OFFSET : ~pointer + OffHeapLeaf.HASH_BYTES_OFFSET;
        }

        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            toString(buff, 1);
            return buff.toString();
        }
    }

    /**
     * @return The bitwise XOR of the inputs.
     */
    static byte[] xor(byte[] left, byte[] right)
    {
        assert left.length == right.length;

        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++)
            out[i] = (byte)((left[i] & 0xFF) ^ (right[i] & 0xFF));
        return out;
    }

    /**
     * Bitwise XOR of the inputs, in place on the left array.
     */
    private static void xorOntoLeft(byte[] left, byte[] right)
    {
        assert left.length == right.length;

        for (int i = 0; i < left.length; i++)
            left[i] = (byte) ((left[i] & 0xFF) ^ (right[i] & 0xFF));
    }

    /**
     * Estimate the allowable depth while keeping the resulting heap usage of this tree under the provided
     * number of bytes. This is important for ensuring that we do not allocate overly large trees that could
     * OOM the JVM and cause instability.
     *
     * Calculated using the following logic:
     *
     * Let T = size of a tree of depth n
     *
     * T = #leafs  * sizeof(leaf) + #inner  * sizeof(inner)
     * T = 2^n     * L            + 2^n - 1 * I
     *
     * T = 2^n * L + 2^n * I - I;
     *
     * So to solve for n given sizeof(tree_n) T:
     *
     * n = floor(log_2((T + I) / (L + I))
     *
     * @param numBytes The number of bytes to fit the tree within
     * @param bytesPerHash The number of bytes stored in a leaf node, for example 2 * murmur128 will be 256 bits
     *                    or 32 bytes
     * @return the estimated depth that will fit within the provided number of bytes
     */
    public static int estimatedMaxDepthForBytes(IPartitioner partitioner, long numBytes, int bytesPerHash)
    {
        byte[] hashLeft = new byte[bytesPerHash];
        byte[] hashRigth = new byte[bytesPerHash];
        OnHeapLeaf left = new OnHeapLeaf(hashLeft);
        OnHeapLeaf right = new OnHeapLeaf(hashRigth);
        Inner inner = new OnHeapInner(partitioner.getMinimumToken(), left, right);
        inner.fillInnerHashes();

        // Some partioners have variable token sizes, try to estimate as close as we can by using the same
        // heap estimate as the memtables use.
        long innerTokenSize = ObjectSizes.measureDeep(partitioner.getMinimumToken());
        long realInnerTokenSize = partitioner.getMinimumToken().getHeapSize();

        long sizeOfLeaf = ObjectSizes.measureDeep(left);
        long sizeOfInner = ObjectSizes.measureDeep(inner) -
                           (ObjectSizes.measureDeep(left) + ObjectSizes.measureDeep(right) + innerTokenSize) +
                           realInnerTokenSize;

        long adjustedBytes = Math.max(1, (numBytes + sizeOfInner) / (sizeOfLeaf + sizeOfInner));
        return Math.max(1, (int) Math.floor(Math.log(adjustedBytes) / Math.log(2)));
    }

    /*
     * Test-only methods.
     */

    /**
     * Invalidates the ranges containing the given token.
     * Useful for testing.
     */
    @VisibleForTesting
    void unsafeInvalidate(Token t)
    {
        unsafeInvalidateHelper(root, fullRange.left, t);
    }

    private void unsafeInvalidateHelper(Node node, Token pleft, Token t)
    {
        node.hash(EMPTY_HASH);

        if (node instanceof Leaf)
            return;

        assert node instanceof Inner;
        Inner inner = (Inner) node;
        inner.unsafeInvalidate();

        if (Range.contains(pleft, inner.token(), t))
            unsafeInvalidateHelper(inner.left(), pleft, t); // left child contains token
        else
            unsafeInvalidateHelper(inner.right(), inner.token(), t); // right child contains token
    }

    /**
     * Hash the given range in the tree. The range must have been generated
     * with recursive applications of partitioner.midpoint().
     *
     * NB: Currently does not support wrapping ranges that do not end with
     * partitioner.getMinimumToken().
     *
     * @return {@link #EMPTY_HASH} if any subrange of the range is invalid, or if the exact
     *         range cannot be calculated using this tree.
     */
    @VisibleForTesting
    byte[] hash(Range<Token> range)
    {
        return find(range).hash();
    }

    interface Consumer<E extends Exception>
    {
        void accept(Node node) throws E;
    }

    @VisibleForTesting
    <E extends Exception> boolean ifHashesRange(Range<Token> range, Consumer<E> consumer) throws E
    {
        try
        {
            Node node = findHelper(root, new Range<>(fullRange.left, fullRange.right), range);
            boolean hasHash = !node.hasEmptyHash();
            if (hasHash)
                consumer.accept(node);
            return hasHash;
        }
        catch (StopRecursion e)
        {
            return false;
        }
    }

    @VisibleForTesting
    boolean hashesRange(Range<Token> range)
    {
        return ifHashesRange(range, n -> {});
    }

    /**
     * For testing purposes.
     * Gets the smallest range containing the token.
     */
    @VisibleForTesting
    public TreeRange get(Token t)
    {
        return getHelper(root, fullRange.left, fullRange.right, t);
    }

    private TreeRange getHelper(Node node, Token pleft, Token pright, Token t)
    {
        int depth = 0;

        while (true)
        {
            if (node instanceof Leaf)
            {
                // we've reached a hash: wrap it up and deliver it
                return new TreeRange(this, pleft, pright, depth, node);
            }

            assert node instanceof Inner;
            Inner inner = (Inner) node;

            if (Range.contains(pleft, inner.token(), t)) // left child contains token
            {
                pright = inner.token();
                node = inner.left();
            }
            else // right child contains token
            {
                pleft = inner.token();
                node = inner.right();
            }

            depth++;
        }
    }

    private void fillInnerHashes()
    {
        root.fillInnerHashes();
    }
}
