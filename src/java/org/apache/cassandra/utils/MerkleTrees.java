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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;


/**
 * Wrapper class for handling of multiple MerkleTrees at once.
 *
 * The MerkleTree's are divided in Ranges of non-overlapping tokens.
 */
public class MerkleTrees implements Iterable<Map.Entry<Range<Token>, MerkleTree>>
{
    public static final MerkleTreesSerializer serializer = new MerkleTreesSerializer();

    private Map<Range<Token>, MerkleTree> merkleTrees = new TreeMap<>(new TokenRangeComparator());

    private IPartitioner partitioner;

    /**
     * Creates empty MerkleTrees object.
     *
     * @param partitioner The partitioner to use
     */
    public MerkleTrees(IPartitioner partitioner)
    {
        this(partitioner, new ArrayList<>());
    }

    private MerkleTrees(IPartitioner partitioner, Collection<MerkleTree> merkleTrees)
    {
        this.partitioner = partitioner;
        addTrees(merkleTrees);
    }

    /**
     * Get the ranges that these merkle trees covers.
     *
     * @return
     */
    public Collection<Range<Token>> ranges()
    {
        return merkleTrees.keySet();
    }

    /**
     * Get the partitioner in use.
     *
     * @return
     */
    public IPartitioner partitioner()
    {
        return partitioner;
    }

    /**
     * Add merkle tree's with the defined maxsize and ranges.
     *
     * @param maxsize
     * @param ranges
     */
    public void addMerkleTrees(int maxsize, Collection<Range<Token>> ranges)
    {
        for (Range<Token> range : ranges)
        {
            addMerkleTree(maxsize, range);
        }
    }

    /**
     * Add a MerkleTree with the defined size and range.
     *
     * @param maxsize
     * @param range
     * @return The created merkle tree.
     */
    public MerkleTree addMerkleTree(int maxsize, Range<Token> range)
    {
        return addMerkleTree(maxsize, MerkleTree.RECOMMENDED_DEPTH, range);
    }

    @VisibleForTesting
    public MerkleTree addMerkleTree(int maxsize, byte hashdepth, Range<Token> range)
    {
        MerkleTree tree = new MerkleTree(partitioner, range, hashdepth, maxsize);
        addTree(tree);

        return tree;
    }

    /**
     * Get the MerkleTree.Range responsible for the given token.
     *
     * @param t
     * @return
     */
    @VisibleForTesting
    public MerkleTree.TreeRange get(Token t)
    {
        return getMerkleTree(t).get(t);
    }

    /**
     * Init all MerkleTree's with an even tree distribution.
     */
    public void init()
    {
        for (Range<Token> range : merkleTrees.keySet())
        {
            init(range);
        }
    }

    /**
     * Init a selected MerkleTree with an even tree distribution.
     *
     * @param range
     */
    public void init(Range<Token> range)
    {
        merkleTrees.get(range).init();
    }

    /**
     * Split the MerkleTree responsible for the given token.
     *
     * @param t
     * @return
     */
    public boolean split(Token t)
    {
        return getMerkleTree(t).split(t);
    }

    /**
     * Invalidate the MerkleTree responsible for the given token.
     *
     * @param t
     */
    @VisibleForTesting
    public void invalidate(Token t)
    {
        getMerkleTree(t).invalidate(t);
    }

    /**
     * Get the MerkleTree responsible for the given token range.
     *
     * @param range
     * @return
     */
    public MerkleTree getMerkleTree(Range<Token> range)
    {
        return merkleTrees.get(range);
    }

    public long size()
    {
        long size = 0;

        for (MerkleTree tree : merkleTrees.values())
        {
            size += tree.size();
        }

        return size;
    }

    @VisibleForTesting
    public void maxsize(Range<Token> range, int maxsize)
    {
        getMerkleTree(range).maxsize(maxsize);
    }

    /**
     * Get the MerkleTree responsible for the given token.
     *
     * @param t
     * @return The given MerkleTree or null if none exist.
     */
    private MerkleTree getMerkleTree(Token t)
    {
        for (Range<Token> range : merkleTrees.keySet())
        {
            if (range.contains(t))
                return merkleTrees.get(range);
        }

        throw new AssertionError("Expected tree for token " + t);
    }

    private void addTrees(Collection<MerkleTree> trees)
    {
        for (MerkleTree tree : trees)
        {
            addTree(tree);
        }
    }

    private void addTree(MerkleTree tree)
    {
        assert validateNonOverlapping(tree) : "Range [" + tree.fullRange + "] is intersecting an existing range";

        merkleTrees.put(tree.fullRange, tree);
    }

    private boolean validateNonOverlapping(MerkleTree tree)
    {
        for (Range<Token> range : merkleTrees.keySet())
        {
            if (tree.fullRange.intersects(range))
                return false;
        }

        return true;
    }

    /**
     * Get an iterator for all the invalids generated by the MerkleTrees.
     *
     * @return
     */
    public TreeRangeIterator invalids()
    {
        return new TreeRangeIterator();
    }

    /**
     * Log the row count per leaf for all MerkleTrees.
     *
     * @param logger
     */
    public void logRowCountPerLeaf(Logger logger)
    {
        for (MerkleTree tree : merkleTrees.values())
        {
            tree.histogramOfRowCountPerLeaf().log(logger);
        }
    }

    /**
     * Log the row size per leaf for all MerkleTrees.
     *
     * @param logger
     */
    public void logRowSizePerLeaf(Logger logger)
    {
        for (MerkleTree tree : merkleTrees.values())
        {
            tree.histogramOfRowSizePerLeaf().log(logger);
        }
    }

    @VisibleForTesting
    public byte[] hash(Range<Token> range)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        boolean hashed = false;

        try
        {
            for (Range<Token> rt : merkleTrees.keySet())
            {
                if (rt.intersects(range))
                {
                    byte[] bytes = merkleTrees.get(rt).hash(range);
                    if (bytes != null)
                    {
                        baos.write(bytes);
                        hashed = true;
                    }
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to append merkle tree hash to result");
        }

        return hashed ? baos.toByteArray() : null;
    }

    /**
     * Get an iterator of all ranges and their MerkleTrees.
     */
    public Iterator<Map.Entry<Range<Token>, MerkleTree>> iterator()
    {
        return merkleTrees.entrySet().iterator();
    }

    public long rowCount()
    {
        long totalCount = 0;
        for (MerkleTree tree : merkleTrees.values())
        {
            totalCount += tree.rowCount();
        }
        return totalCount;
    }

    public class TreeRangeIterator extends AbstractIterator<MerkleTree.TreeRange> implements
            Iterable<MerkleTree.TreeRange>,
            PeekingIterator<MerkleTree.TreeRange>
    {
        private final Iterator<MerkleTree> it;

        private MerkleTree.TreeRangeIterator current = null;

        private TreeRangeIterator()
        {
            it = merkleTrees.values().iterator();
        }

        public MerkleTree.TreeRange computeNext()
        {
            if (current == null || !current.hasNext())
                return nextIterator();

            return current.next();
        }

        private MerkleTree.TreeRange nextIterator()
        {
            if (it.hasNext())
            {
                current = it.next().invalids();

                return current.next();
            }

            return endOfData();
        }

        public Iterator<MerkleTree.TreeRange> iterator()
        {
            return this;
        }
    }

    /**
     * Get the differences between the two sets of MerkleTrees.
     *
     * @param ltree
     * @param rtree
     * @return
     */
    public static List<Range<Token>> difference(MerkleTrees ltree, MerkleTrees rtree)
    {
        List<Range<Token>> differences = new ArrayList<>();
        for (MerkleTree tree : ltree.merkleTrees.values())
        {
            differences.addAll(MerkleTree.difference(tree, rtree.getMerkleTree(tree.fullRange)));
        }
        return differences;
    }

    public static class MerkleTreesSerializer implements IVersionedSerializer<MerkleTrees>
    {
        public void serialize(MerkleTrees trees, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(trees.merkleTrees.size());
            for (MerkleTree tree : trees.merkleTrees.values())
            {
                MerkleTree.serializer.serialize(tree, out, version);
            }
        }

        public MerkleTrees deserialize(DataInputPlus in, int version) throws IOException
        {
            IPartitioner partitioner = null;
            int nTrees = in.readInt();
            Collection<MerkleTree> trees = new ArrayList<>(nTrees);
            if (nTrees > 0)
            {
                for (int i = 0; i < nTrees; i++)
                {
                    MerkleTree tree = MerkleTree.serializer.deserialize(in, version);
                    trees.add(tree);

                    if (partitioner == null)
                        partitioner = tree.partitioner();
                    else
                        assert tree.partitioner() == partitioner;
                }
            }

            return new MerkleTrees(partitioner, trees);
        }

        public long serializedSize(MerkleTrees trees, int version)
        {
            assert trees != null;

            long size = TypeSizes.sizeof(trees.merkleTrees.size());
            for (MerkleTree tree : trees.merkleTrees.values())
            {
                size += MerkleTree.serializer.serializedSize(tree, version);
            }
            return size;
        }

    }

    private static class TokenRangeComparator implements Comparator<Range<Token>>
    {
        @Override
        public int compare(Range<Token> rt1, Range<Token> rt2)
        {
            if (rt1.left.compareTo(rt2.left) == 0)
                return 0;

            return rt1.compareTo(rt2);
        }
    }
}
