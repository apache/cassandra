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
import java.io.Serializable;
import java.util.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

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
public class MerkleTree implements Serializable
{
    public static final MerkleTreeSerializer serializer = new MerkleTreeSerializer();
    private static final long serialVersionUID = 2L;

    public static final byte RECOMMENDED_DEPTH = Byte.MAX_VALUE - 1;

    public static final int CONSISTENT = 0;
    public static final int FULLY_INCONSISTENT = 1;
    public static final int PARTIALLY_INCONSISTENT = 2;
    private static final byte[] EMPTY_HASH = new byte[0];

    public final byte hashdepth;

    /** The top level range that this MerkleTree covers. */
    public final Range<Token> fullRange;
    private final IPartitioner partitioner;

    private long maxsize;
    private long size;
    private Hashable root;

    public static class MerkleTreeSerializer implements IVersionedSerializer<MerkleTree>
    {
        public void serialize(MerkleTree mt, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(mt.hashdepth);
            out.writeLong(mt.maxsize);
            out.writeLong(mt.size);
            out.writeUTF(mt.partitioner.getClass().getCanonicalName());
            // full range
            Token.serializer.serialize(mt.fullRange.left, out);
            Token.serializer.serialize(mt.fullRange.right, out);
            Hashable.serializer.serialize(mt.root, out, version);
        }

        public MerkleTree deserialize(DataInput in, int version) throws IOException
        {
            byte hashdepth = in.readByte();
            long maxsize = in.readLong();
            long size = in.readLong();
            IPartitioner partitioner;
            try
            {
                partitioner = FBUtilities.newPartitioner(in.readUTF());
            }
            catch (ConfigurationException e)
            {
                throw new IOException(e);
            }

            // full range
            Token left = Token.serializer.deserialize(in);
            Token right = Token.serializer.deserialize(in);
            Range<Token> fullRange = new Range<>(left, right, partitioner);

            MerkleTree mt = new MerkleTree(partitioner, fullRange, hashdepth, maxsize);
            mt.size = size;
            mt.root = Hashable.serializer.deserialize(in, version);
            return mt;
        }

        public long serializedSize(MerkleTree mt, int version)
        {
            long size = 1 // mt.hashdepth
                 + TypeSizes.NATIVE.sizeof(mt.maxsize)
                 + TypeSizes.NATIVE.sizeof(mt.size)
                 + TypeSizes.NATIVE.sizeof(mt.partitioner.getClass().getCanonicalName());

            // full range
            size += Token.serializer.serializedSize(mt.fullRange.left, TypeSizes.NATIVE);
            size += Token.serializer.serializedSize(mt.fullRange.right, TypeSizes.NATIVE);

            size += Hashable.serializer.serializedSize(mt.root, version);
            return size;
        }
    }

    /**
     * @param partitioner The partitioner in use.
     * @param range the range this tree covers
     * @param hashdepth The maximum depth of the tree. 100/(2^depth) is the %
     *        of the key space covered by each subrange of a fully populated tree.
     * @param maxsize The maximum number of subranges in the tree.
     */
    public MerkleTree(IPartitioner partitioner, Range<Token> range, byte hashdepth, long maxsize)
    {
        assert hashdepth < Byte.MAX_VALUE;
        this.fullRange = Preconditions.checkNotNull(range);
        this.partitioner = Preconditions.checkNotNull(partitioner);
        this.hashdepth = hashdepth;
        this.maxsize = maxsize;

        size = 1;
        root = new Leaf(null);
    }


    static byte inc(byte in)
    {
        assert in < Byte.MAX_VALUE;
        return (byte)(in + 1);
    }

    /**
     * Initializes this tree by splitting it until hashdepth is reached,
     * or until an additional level of splits would violate maxsize.
     *
     * NB: Replaces all nodes in the tree.
     */
    public void init()
    {
        // determine the depth to which we can safely split the tree
        byte sizedepth = (byte)(Math.log10(maxsize) / Math.log10(2));
        byte depth = (byte)Math.min(sizedepth, hashdepth);

        root = initHelper(fullRange.left, fullRange.right, (byte)0, depth);
        size = (long)Math.pow(2, depth);
    }

    private Hashable initHelper(Token left, Token right, byte depth, byte max)
    {
        if (depth == max)
            // we've reached the leaves
            return new Leaf();
        Token midpoint = partitioner.midpoint(left, right);

        if (midpoint.equals(left) || midpoint.equals(right))
            return new Leaf();

        Hashable lchild =  initHelper(left, midpoint, inc(depth), max);
        Hashable rchild =  initHelper(midpoint, right, inc(depth), max);
        return new Inner(midpoint, lchild, rchild);
    }

    Hashable root()
    {
        return root;
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
            throw new IllegalArgumentException("Difference only make sense on tree covering the same range (but " + ltree.fullRange + " != " + rtree.fullRange + ")");

        List<TreeRange> diff = new ArrayList<>();
        TreeDifference active = new TreeDifference(ltree.fullRange.left, ltree.fullRange.right, (byte)0);

        Hashable lnode = ltree.find(active);
        Hashable rnode = rtree.find(active);
        byte[] lhash = lnode.hash();
        byte[] rhash = rnode.hash();
        active.setSize(lnode.sizeOfRange(), rnode.sizeOfRange());

        if (lhash != null && rhash != null && !Arrays.equals(lhash, rhash))
        {
            if (FULLY_INCONSISTENT == differenceHelper(ltree, rtree, diff, active))
                diff.add(active);
        }
        else if (lhash == null || rhash == null)
            diff.add(active);
        return diff;
    }

    /**
     * TODO: This function could be optimized into a depth first traversal of
     * the two trees in parallel.
     *
     * Takes two trees and a range for which they have hashes, but are inconsistent.
     * @return FULLY_INCONSISTENT if active is inconsistent, PARTIALLY_INCONSISTENT if only a subrange is inconsistent.
     */
    static int differenceHelper(MerkleTree ltree, MerkleTree rtree, List<TreeRange> diff, TreeRange active)
    {
        if (active.depth == Byte.MAX_VALUE)
            return CONSISTENT;

        Token midpoint = ltree.partitioner().midpoint(active.left, active.right);
        TreeDifference left = new TreeDifference(active.left, midpoint, inc(active.depth));
        TreeDifference right = new TreeDifference(midpoint, active.right, inc(active.depth));
        byte[] lhash, rhash;
        Hashable lnode, rnode;

        // see if we should recurse left
        lnode = ltree.find(left);
        rnode = rtree.find(left);
        lhash = lnode.hash();
        rhash = rnode.hash();
        left.setSize(lnode.sizeOfRange(), rnode.sizeOfRange());
        left.setRows(lnode.rowsInRange(), rnode.rowsInRange());

        int ldiff = CONSISTENT;
        boolean lreso = lhash != null && rhash != null;
        if (lreso && !Arrays.equals(lhash, rhash))
            ldiff = differenceHelper(ltree, rtree, diff, left);
        else if (!lreso)
            ldiff = FULLY_INCONSISTENT;

        // see if we should recurse right
        lnode = ltree.find(right);
        rnode = rtree.find(right);
        lhash = lnode.hash();
        rhash = rnode.hash();
        right.setSize(lnode.sizeOfRange(), rnode.sizeOfRange());
        right.setRows(lnode.rowsInRange(), rnode.rowsInRange());

        int rdiff = CONSISTENT;
        boolean rreso = lhash != null && rhash != null;
        if (rreso && !Arrays.equals(lhash, rhash))
            rdiff = differenceHelper(ltree, rtree, diff, right);
        else if (!rreso)
            rdiff = FULLY_INCONSISTENT;

        if (ldiff == FULLY_INCONSISTENT && rdiff == FULLY_INCONSISTENT)
        {
            // both children are fully inconsistent
            return FULLY_INCONSISTENT;
        }
        else if (ldiff == FULLY_INCONSISTENT)
        {
            diff.add(left);
            return PARTIALLY_INCONSISTENT;
        }
        else if (rdiff == FULLY_INCONSISTENT)
        {
            diff.add(right);
            return PARTIALLY_INCONSISTENT;
        }
        return PARTIALLY_INCONSISTENT;
    }

    /**
     * For testing purposes.
     * Gets the smallest range containing the token.
     */
    public TreeRange get(Token t)
    {
        return getHelper(root, fullRange.left, fullRange.right, (byte)0, t);
    }

    TreeRange getHelper(Hashable hashable, Token pleft, Token pright, byte depth, Token t)
    {
        if (hashable instanceof Leaf)
        {
            // we've reached a hash: wrap it up and deliver it
            return new TreeRange(this, pleft, pright, depth, hashable);
        }
        // else: node.

        Inner node = (Inner)hashable;
        if (Range.contains(pleft, node.token, t))
            // left child contains token
            return getHelper(node.lchild, pleft, node.token, inc(depth), t);
        // else: right child contains token
        return getHelper(node.rchild, node.token, pright, inc(depth), t);
    }

    /**
     * Invalidates the ranges containing the given token.
     * Useful for testing.
     */
    public void invalidate(Token t)
    {
        invalidateHelper(root, fullRange.left, t);
    }

    private void invalidateHelper(Hashable hashable, Token pleft, Token t)
    {
        hashable.hash(null);
        if (hashable instanceof Leaf)
            return;
        // else: node.

        Inner node = (Inner)hashable;
        if (Range.contains(pleft, node.token, t))
            // left child contains token
            invalidateHelper(node.lchild, pleft, t);
        else
            // right child contains token
            invalidateHelper(node.rchild, node.token, t);
    }

    /**
     * Hash the given range in the tree. The range must have been generated
     * with recursive applications of partitioner.midpoint().
     *
     * NB: Currently does not support wrapping ranges that do not end with
     * partitioner.getMinimumToken().
     *
     * @return Null if any subrange of the range is invalid, or if the exact
     *         range cannot be calculated using this tree.
     */
    public byte[] hash(Range<Token> range)
    {
        return find(range).hash();
    }

    /**
     * Find the {@link Hashable} node that matches the given {@code range}.
     *
     * @param range Range to find
     * @return {@link Hashable} found. If nothing found, return {@link Leaf} with null hash.
     */
    private Hashable find(Range<Token> range)
    {
        try
        {
            return findHelper(root, new Range<Token>(fullRange.left, fullRange.right), range);
        }
        catch (StopRecursion e)
        {
            return new Leaf();
        }
    }

    /**
     * @throws StopRecursion If no match could be found for the range.
     */
    private Hashable findHelper(Hashable current, Range<Token> activeRange, Range<Token> find) throws StopRecursion
    {
        if (current instanceof Leaf)
        {
            if (!find.contains(activeRange))
                // we are not fully contained in this range!
                throw new StopRecursion.BadRange();
            return current;
        }
        // else: node.

        Inner node = (Inner)current;
        Range<Token> leftRange = new Range<Token>(activeRange.left, node.token);
        Range<Token> rightRange = new Range<Token>(node.token, activeRange.right);

        if (find.contains(activeRange))
            // this node is fully contained in the range
            return node.calc();

        // else: one of our children contains the range

        if (leftRange.contains(find))
            // left child contains/matches the range
            return findHelper(node.lchild, leftRange, find);
        else if (rightRange.contains(find))
            // right child contains/matches the range
            return findHelper(node.rchild, rightRange, find);
        else
            throw new StopRecursion.BadRange();
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
        if (!(size < maxsize))
            return false;

        try
        {
            root = splitHelper(root, fullRange.left, fullRange.right, (byte)0, t);
        }
        catch (StopRecursion.TooDeep e)
        {
            return false;
        }
        return true;
    }

    private Hashable splitHelper(Hashable hashable, Token pleft, Token pright, byte depth, Token t) throws StopRecursion.TooDeep
    {
        if (depth >= hashdepth)
            throw new StopRecursion.TooDeep();

        if (hashable instanceof Leaf)
        {
            Token midpoint = partitioner.midpoint(pleft, pright);

            // We should not create a non-sensical range where start and end are the same token (this is non-sensical because range are
            // start exclusive). Note that we shouldn't hit that unless the full range is very small or we are fairly deep
            if (midpoint.equals(pleft) || midpoint.equals(pright))
                throw new StopRecursion.TooDeep();

            // split
            size++;
            return new Inner(midpoint, new Leaf(), new Leaf());
        }
        // else: node.

        // recurse on the matching child
        Inner node = (Inner)hashable;

        if (Range.contains(pleft, node.token, t))
            // left child contains token
            node.lchild(splitHelper(node.lchild, pleft, node.token, inc(depth), t));
        else
            // else: right child contains token
            node.rchild(splitHelper(node.rchild, node.token, pright, inc(depth), t));
        return node;
    }

    /**
     * Returns a lazy iterator of invalid TreeRanges that need to be filled
     * in order to make the given Range valid.
     */
    public TreeRangeIterator invalids()
    {
        return new TreeRangeIterator(this);
    }

    public EstimatedHistogram histogramOfRowSizePerLeaf()
    {
        HistogramBuilder histbuild = new HistogramBuilder();
        for (TreeRange range : new TreeRangeIterator(this))
        {
            histbuild.add(range.hashable.sizeOfRange);
        }
        return histbuild.buildWithStdevRangesAroundMean();
    }

    public EstimatedHistogram histogramOfRowCountPerLeaf()
    {
        HistogramBuilder histbuild = new HistogramBuilder();
        for (TreeRange range : new TreeRangeIterator(this))
        {
            histbuild.add(range.hashable.rowsInRange);
        }
        return histbuild.buildWithStdevRangesAroundMean();
    }

    @Override
    public String toString()
    {
        StringBuilder buff = new StringBuilder();
        buff.append("#<MerkleTree root=");
        root.toString(buff, 8);
        buff.append(">");
        return buff.toString();
    }

    public static class TreeDifference extends TreeRange
    {
        private static final long serialVersionUID = 6363654174549968183L;

        private long sizeOnLeft;
        private long sizeOnRight;
        private long rowsOnLeft;
        private long rowsOnRight;

        void setSize(long sizeOnLeft, long sizeOnRight)
        {
            this.sizeOnLeft = sizeOnLeft;
            this.sizeOnRight = sizeOnRight;
        }

        void setRows(long rowsOnLeft, long rowsOnRight)
        {
            this.rowsOnLeft = rowsOnLeft;
            this.rowsOnRight = rowsOnRight;
        }

        public long sizeOnLeft()
        {
            return sizeOnLeft;
        }

        public long sizeOnRight()
        {
            return sizeOnRight;
        }

        public long rowsOnLeft()
        {
            return rowsOnLeft;
        }

        public long rowsOnRight()
        {
            return rowsOnRight;
        }

        public TreeDifference(Token left, Token right, byte depth)
        {
            super(null, left, right, depth, null);
        }

        public long totalRows()
        {
            return rowsOnLeft + rowsOnRight;
        }

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
        public static final long serialVersionUID = 1L;
        private final MerkleTree tree;
        public final byte depth;
        private final Hashable hashable;

        TreeRange(MerkleTree tree, Token left, Token right, byte depth, Hashable hashable)
        {
            super(left, right);
            this.tree = tree;
            this.depth = depth;
            this.hashable = hashable;
        }

        public void hash(byte[] hash)
        {
            assert tree != null : "Not intended for modification!";
            hashable.hash(hash);
        }

        public byte[] hash()
        {
            return hashable.hash();
        }

        /**
         * @param entry Row to mix into the hash for this range.
         */
        public void addHash(RowHash entry)
        {
            assert tree != null : "Not intended for modification!";
            assert hashable instanceof Leaf;

            hashable.addHash(entry.hash, entry.size);
        }

        public void ensureHashInitialised()
        {
            assert tree != null : "Not intended for modification!";
            assert hashable instanceof Leaf;

            if (hashable.hash == null)
                hashable.hash = EMPTY_HASH;
        }

        public void addAll(Iterator<RowHash> entries)
        {
            while (entries.hasNext())
                addHash(entries.next());
        }

        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder("#<TreeRange ");
            buff.append(super.toString()).append(" depth=").append(depth);
            return buff.append(">").toString();
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
            tovisit = new ArrayDeque<TreeRange>();
            tovisit.add(new TreeRange(tree, tree.fullRange.left, tree.fullRange.right, (byte)0, tree.root));
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

                if (active.hashable instanceof Leaf)
                {
                    // found a leaf invalid range
                    if (active.isWrapAround() && !tovisit.isEmpty())
                        // put to be taken again last
                        tovisit.addLast(active);
                    return active;
                }

                Inner node = (Inner)active.hashable;
                TreeRange left = new TreeRange(tree, active.left, node.token, inc(active.depth), node.lchild);
                TreeRange right = new TreeRange(tree, node.token, active.right, inc(active.depth), node.rchild);

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
     * An inner node in the MerkleTree. Inners can contain cached hash values, which
     * are the binary hash of their two children.
     */
    static class Inner extends Hashable
    {
        public static final long serialVersionUID = 1L;
        static final byte IDENT = 2;
        public final Token token;
        private Hashable lchild;
        private Hashable rchild;

        private static final InnerSerializer serializer = new InnerSerializer();

        /**
         * Constructs an Inner with the given token and children, and a null hash.
         */
        public Inner(Token token, Hashable lchild, Hashable rchild)
        {
            super(null);
            this.token = token;
            this.lchild = lchild;
            this.rchild = rchild;
        }

        public Hashable lchild()
        {
            return lchild;
        }

        public Hashable rchild()
        {
            return rchild;
        }

        public void lchild(Hashable child)
        {
            lchild = child;
        }

        public void rchild(Hashable child)
        {
            rchild = child;
        }

        Hashable calc()
        {
            if (hash == null)
            {
                // hash and size haven't been calculated; calc children then compute
                Hashable lnode = lchild.calc();
                Hashable rnode = rchild.calc();
                // cache the computed value
                hash(lnode.hash, rnode.hash);
                sizeOfRange = lnode.sizeOfRange + rnode.sizeOfRange;
                rowsInRange = lnode.rowsInRange + rnode.rowsInRange;
            }
            return this;
        }

        /**
         * Recursive toString.
         */
        public void toString(StringBuilder buff, int maxdepth)
        {
            buff.append("#<").append(getClass().getSimpleName());
            buff.append(" ").append(token);
            buff.append(" hash=").append(Hashable.toString(hash()));
            buff.append(" children=[");
            if (maxdepth < 1)
            {
                buff.append("#");
            }
            else
            {
                if (lchild == null)
                    buff.append("null");
                else
                    lchild.toString(buff, maxdepth-1);
                buff.append(" ");
                if (rchild == null)
                    buff.append("null");
                else
                    rchild.toString(buff, maxdepth-1);
            }
            buff.append("]>");
        }

        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            toString(buff, 1);
            return buff.toString();
        }

        private static class InnerSerializer implements IVersionedSerializer<Inner>
        {
            public void serialize(Inner inner, DataOutputPlus out, int version) throws IOException
            {
                if (inner.hash == null)
                    out.writeInt(-1);
                else
                {
                    out.writeInt(inner.hash.length);
                    out.write(inner.hash);
                }
                Token.serializer.serialize(inner.token, out);
                Hashable.serializer.serialize(inner.lchild, out, version);
                Hashable.serializer.serialize(inner.rchild, out, version);
            }

            public Inner deserialize(DataInput in, int version) throws IOException
            {
                int hashLen = in.readInt();
                byte[] hash = hashLen >= 0 ? new byte[hashLen] : null;
                if (hash != null)
                    in.readFully(hash);
                Token token = Token.serializer.deserialize(in);
                Hashable lchild = Hashable.serializer.deserialize(in, version);
                Hashable rchild = Hashable.serializer.deserialize(in, version);
                return new Inner(token, lchild, rchild);
            }

            public long serializedSize(Inner inner, int version)
            {
                int size = inner.hash == null
                ? TypeSizes.NATIVE.sizeof(-1)
                        : TypeSizes.NATIVE.sizeof(inner.hash().length) + inner.hash().length;

                size += Token.serializer.serializedSize(inner.token, TypeSizes.NATIVE)
                + Hashable.serializer.serializedSize(inner.lchild, version)
                + Hashable.serializer.serializedSize(inner.rchild, version);
                return size;
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
    static class Leaf extends Hashable
    {
        public static final long serialVersionUID = 1L;
        static final byte IDENT = 1;
        private static final LeafSerializer serializer = new LeafSerializer();

        /**
         * Constructs a null hash.
         */
        public Leaf()
        {
            super(null);
        }

        public Leaf(byte[] hash)
        {
            super(hash);
        }

        public void toString(StringBuilder buff, int maxdepth)
        {
            buff.append(toString());
        }

        @Override
        public String toString()
        {
            return "#<Leaf " + Hashable.toString(hash()) + ">";
        }

        private static class LeafSerializer implements IVersionedSerializer<Leaf>
        {
            public void serialize(Leaf leaf, DataOutputPlus out, int version) throws IOException
            {
                if (leaf.hash == null)
                {
                    out.writeInt(-1);
                }
                else
                {
                    out.writeInt(leaf.hash.length);
                    out.write(leaf.hash);
                }
            }

            public Leaf deserialize(DataInput in, int version) throws IOException
            {
                int hashLen = in.readInt();
                byte[] hash = hashLen < 0 ? null : new byte[hashLen];
                if (hash != null)
                    in.readFully(hash);
                return new Leaf(hash);
            }

            public long serializedSize(Leaf leaf, int version)
            {
                return leaf.hash == null
                     ? TypeSizes.NATIVE.sizeof(-1)
                     : TypeSizes.NATIVE.sizeof(leaf.hash().length) + leaf.hash().length;
            }
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
            this.size = size;
        }

        @Override
        public String toString()
        {
            return "#<RowHash " + token + " " + Hashable.toString(hash) + " @ " + size + " bytes>";
        }
    }

    /**
     * Abstract class containing hashing logic, and containing a single hash field.
     */
    static abstract class Hashable implements Serializable
    {
        private static final long serialVersionUID = 1L;
        private static final IVersionedSerializer<Hashable> serializer = new HashableSerializer();

        protected byte[] hash;
        protected long sizeOfRange;
        protected long rowsInRange;

        protected Hashable(byte[] hash)
        {
            this.hash = hash;
        }

        public byte[] hash()
        {
            return hash;
        }

        public long sizeOfRange()
        {
            return sizeOfRange;
        }

        public long rowsInRange()
        {
            return rowsInRange;
        }

        void hash(byte[] hash)
        {
            this.hash = hash;
        }

        Hashable calc()
        {
            return this;
        }

        /**
         * Sets the value of this hash to binaryHash of its children.
         * @param lefthash Hash of left child.
         * @param righthash Hash of right child.
         */
        void hash(byte[] lefthash, byte[] righthash)
        {
            hash = binaryHash(lefthash, righthash);
        }

        /**
         * Mixes the given value into our hash. If our hash is null,
         * our hash will become the given value.
         */
        void addHash(byte[] righthash, long sizeOfRow)
        {
            if (hash == null)
                hash = righthash;
            else
                hash = binaryHash(hash, righthash);
            this.sizeOfRange += sizeOfRow;
            this.rowsInRange += 1;
        }

        /**
         * The primitive with which all hashing should be accomplished: hashes
         * a left and right value together.
         */
        static byte[] binaryHash(final byte[] left, final byte[] right)
        {
            return FBUtilities.xor(left, right);
        }

        public abstract void toString(StringBuilder buff, int maxdepth);

        public static String toString(byte[] hash)
        {
            if (hash == null)
                return "null";
            return "[" + Hex.bytesToHex(hash) + "]";
        }

        private static class HashableSerializer implements IVersionedSerializer<Hashable>
        {
            public void serialize(Hashable h, DataOutputPlus out, int version) throws IOException
            {
                if (h instanceof Inner)
                {
                    out.writeByte(Inner.IDENT);
                    Inner.serializer.serialize((Inner)h, out, version);
                }
                else if (h instanceof Leaf)
                {
                    out.writeByte(Leaf.IDENT);
                    Leaf.serializer.serialize((Leaf) h, out, version);
                }
                else
                    throw new IOException("Unexpected Hashable: " + h.getClass().getCanonicalName());
            }

            public Hashable deserialize(DataInput in, int version) throws IOException
            {
                byte ident = in.readByte();
                if (Inner.IDENT == ident)
                    return Inner.serializer.deserialize(in, version);
                else if (Leaf.IDENT == ident)
                    return Leaf.serializer.deserialize(in, version);
                else
                    throw new IOException("Unexpected Hashable: " + ident);
            }

            public long serializedSize(Hashable h, int version)
            {
                if (h instanceof Inner)
                    return 1 + Inner.serializer.serializedSize((Inner) h, version);
                else if (h instanceof Leaf)
                    return 1 + Leaf.serializer.serializedSize((Leaf) h, version);
                throw new AssertionError(h.getClass());
            }
        }
    }

    /**
     * Exceptions that stop recursion early when we are sure that no answer
     * can be found.
     */
    static abstract class StopRecursion extends Exception
    {
        static class BadRange extends StopRecursion
        {
            public BadRange(){ super(); }
        }

        static class InvalidHash extends StopRecursion
        {
            public InvalidHash(){ super(); }
        }

        static class TooDeep extends StopRecursion
        {
            public TooDeep(){ super(); }
        }
    }
}
