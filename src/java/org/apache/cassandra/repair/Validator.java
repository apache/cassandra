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
package org.apache.cassandra.repair;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTree.RowHash;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Handles the building of a merkle tree for a column family.
 *
 * Lifecycle:
 * 1. prepare() - Initialize tree with samples.
 * 2. add() - 0 or more times, to add hashes to the tree.
 * 3. complete() - Enqueues any operations that were blocked waiting for a valid tree.
 */
public class Validator implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(Validator.class);

    public final RepairJobDesc desc;
    public final InetAddressAndPort initiator;
    public final int nowInSec;
    private final boolean evenTreeDistribution;
    public final boolean isIncremental;

    // null when all rows with the min token have been consumed
    private long validated;
    private MerkleTrees trees;
    // current range being updated
    private MerkleTree.TreeRange range;
    // iterator for iterating sub ranges (MT's leaves)
    private MerkleTrees.TreeRangeIterator ranges;
    // last key seen
    private DecoratedKey lastKey;

    private final PreviewKind previewKind;

    public Validator(RepairJobDesc desc, InetAddressAndPort initiator, int nowInSec, PreviewKind previewKind)
    {
        this(desc, initiator, nowInSec, false, false, previewKind);
    }

    public Validator(RepairJobDesc desc, InetAddressAndPort initiator, int nowInSec, boolean isIncremental, PreviewKind previewKind)
    {
        this(desc, initiator, nowInSec, false, isIncremental, previewKind);
    }

    public Validator(RepairJobDesc desc, InetAddressAndPort initiator, int nowInSec, boolean evenTreeDistribution, boolean isIncremental, PreviewKind previewKind)
    {
        this.desc = desc;
        this.initiator = initiator;
        this.nowInSec = nowInSec;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
        validated = 0;
        range = null;
        ranges = null;
        this.evenTreeDistribution = evenTreeDistribution;
    }

    public void prepare(ColumnFamilyStore cfs, MerkleTrees tree)
    {
        this.trees = tree;

        if (!tree.partitioner().preservesOrder() || evenTreeDistribution)
        {
            // You can't beat an even tree distribution for md5
            tree.init();
        }
        else
        {
            List<DecoratedKey> keys = new ArrayList<>();
            Random random = new Random();

            for (Range<Token> range : tree.ranges())
            {
                for (DecoratedKey sample : cfs.keySamples(range))
                {
                    assert range.contains(sample.getToken()) : "Token " + sample.getToken() + " is not within range " + desc.ranges;
                    keys.add(sample);
                }

                if (keys.isEmpty())
                {
                    // use an even tree distribution
                    tree.init(range);
                }
                else
                {
                    int numKeys = keys.size();
                    // sample the column family using random keys from the index
                    while (true)
                    {
                        DecoratedKey dk = keys.get(random.nextInt(numKeys));
                        if (!tree.split(dk.getToken()))
                            break;
                    }
                    keys.clear();
                }
            }
        }
        logger.debug("Prepared AEService trees of size {} for {}", trees.size(), desc);
        ranges = tree.invalids();
    }

    /**
     * Called (in order) for every row present in the CF.
     * Hashes the row, and adds it to the tree being built.
     *
     * @param partition Partition to add hash
     */
    public void add(UnfilteredRowIterator partition)
    {
        assert Range.isInRanges(partition.partitionKey().getToken(), desc.ranges) : partition.partitionKey().getToken() + " is not contained in " + desc.ranges;
        assert lastKey == null || lastKey.compareTo(partition.partitionKey()) < 0
               : "partition " + partition.partitionKey() + " received out of order wrt " + lastKey;
        lastKey = partition.partitionKey();

        if (range == null)
            range = ranges.next();

        // generate new ranges as long as case 1 is true
        if (!findCorrectRange(lastKey.getToken()))
        {
            // add the empty hash, and move to the next range
            ranges = trees.invalids();
            findCorrectRange(lastKey.getToken());
        }

        assert range.contains(lastKey.getToken()) : "Token not in MerkleTree: " + lastKey.getToken();
        // case 3 must be true: mix in the hashed row
        RowHash rowHash = rowHash(partition);
        if (rowHash != null)
        {
            range.addHash(rowHash);
        }
    }

    public boolean findCorrectRange(Token t)
    {
        while (!range.contains(t) && ranges.hasNext())
        {
            range = ranges.next();
        }

        return range.contains(t);
    }

    /**
     * Hasher that concatenates the hash code from 2 hash functions (murmur3_128) with different
     * seeds and counts the number of bytes we hashed.
     *
     * Everything hashed by this class is hashed by both hash functions and the
     * resulting hashcode is a concatenation of the output bytes from each.
     *
     * Idea from Guavas Hashing.ConcatenatedHashFunction, but that is package-private so we can't use it
     */
    @VisibleForTesting
    static class CountingHasher implements Hasher
    {
        @VisibleForTesting
        static final HashFunction[] hashFunctions = new HashFunction[2];

        static
        {
            for (int i = 0; i < hashFunctions.length; i++)
                hashFunctions[i] = Hashing.murmur3_128(i * 1000);
        }
        private long count;
        private final int bits;
        private final Hasher[] underlying = new Hasher[2];

        CountingHasher()
        {
            int bits = 0;
            for (int i = 0; i < underlying.length; i++)
            {
                this.underlying[i] = hashFunctions[i].newHasher();
                bits += hashFunctions[i].bits();
            }
            this.bits = bits;
        }

        public Hasher putByte(byte b)
        {
            count += 1;
            for (Hasher h : underlying)
                h.putByte(b);
            return this;
        }

        public Hasher putBytes(byte[] bytes)
        {
            count += bytes.length;
            for (Hasher h : underlying)
                h.putBytes(bytes);
            return this;
        }

        public Hasher putBytes(byte[] bytes, int offset, int length)
        {
            count += length;
            for (Hasher h : underlying)
                h.putBytes(bytes, offset, length);
            return this;
        }

        public Hasher putBytes(ByteBuffer byteBuffer)
        {
            count += byteBuffer.remaining();
            for (Hasher h : underlying)
                h.putBytes(byteBuffer.duplicate());
            return this;
        }

        public Hasher putShort(short i)
        {
            count += Short.BYTES;
            for (Hasher h : underlying)
                h.putShort(i);
            return this;
        }

        public Hasher putInt(int i)
        {
            count += Integer.BYTES;
            for (Hasher h : underlying)
                h.putInt(i);
            return this;
        }

        public Hasher putLong(long l)
        {
            count += Long.BYTES;
            for (Hasher h : underlying)
                h.putLong(l);
            return this;
        }

        public Hasher putFloat(float v)
        {
            count += Float.BYTES;
            for (Hasher h : underlying)
                h.putFloat(v);
            return this;
        }

        public Hasher putDouble(double v)
        {
            count += Double.BYTES;
            for (Hasher h : underlying)
                h.putDouble(v);
            return this;
        }

        public Hasher putBoolean(boolean b)
        {
            count += Byte.BYTES;
            for (Hasher h : underlying)
                h.putBoolean(b);
            return this;
        }

        public Hasher putChar(char c)
        {
            count += Character.BYTES;
            for (Hasher h : underlying)
                h.putChar(c);
            return this;
        }

        public Hasher putUnencodedChars(CharSequence charSequence)
        {
            throw new UnsupportedOperationException();
        }

        public Hasher putString(CharSequence charSequence, Charset charset)
        {
            throw new UnsupportedOperationException();
        }

        public <T> Hasher putObject(T t, Funnel<? super T> funnel)
        {
            throw new UnsupportedOperationException();
        }

        public HashCode hash()
        {
            byte[] res = new byte[bits / 8];
            int i = 0;
            for (Hasher hasher : underlying)
            {
                HashCode newHash = hasher.hash();
                i += newHash.writeBytesTo(res, i, newHash.bits() / 8);
            }
            return HashCode.fromBytes(res);
        }

        public long getCount()
        {
            return count;
        }
    }

    private MerkleTree.RowHash rowHash(UnfilteredRowIterator partition)
    {
        validated++;
        // MerkleTree uses XOR internally, so we want lots of output bits here
        CountingHasher hasher = new CountingHasher();
        UnfilteredRowIterators.digest(partition, hasher, MessagingService.current_version);
        // only return new hash for merkle tree in case digest was updated - see CASSANDRA-8979
        return hasher.count > 0
             ? new MerkleTree.RowHash(partition.partitionKey().getToken(), hasher.hash().asBytes(), hasher.count)
             : null;
    }

    /**
     * Registers the newly created tree for rendezvous in Stage.ANTIENTROPY.
     */
    public void complete()
    {
        completeTree();

        StageManager.getStage(Stage.ANTI_ENTROPY).execute(this);

        if (logger.isDebugEnabled())
        {
            // log distribution of rows in tree
            logger.debug("Validated {} partitions for {}.  Partitions per leaf are:", validated, desc.sessionId);
            trees.logRowCountPerLeaf(logger);
            logger.debug("Validated {} partitions for {}.  Partition sizes are:", validated, desc.sessionId);
            trees.logRowSizePerLeaf(logger);
        }
    }

    @VisibleForTesting
    public void completeTree()
    {
        assert ranges != null : "Validator was not prepared()";

        ranges = trees.invalids();

        while (ranges.hasNext())
        {
            range = ranges.next();
            range.ensureHashInitialised();
        }
    }

    /**
     * Called when some error during the validation happened.
     * This sends RepairStatus to inform the initiator that the validation has failed.
     * The actual reason for failure should be looked up in the log of the host calling this function.
     */
    public void fail()
    {
        logger.error("Failed creating a merkle tree for {}, {} (see log for details)", desc, initiator);
        // send fail message only to nodes >= version 2.0
        MessagingService.instance().sendOneWay(new ValidationComplete(desc).createMessage(), initiator);
    }

    /**
     * Called after the validation lifecycle to respond with the now valid tree. Runs in Stage.ANTIENTROPY.
     */
    public void run()
    {
        // respond to the request that triggered this validation
        if (!initiator.equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            logger.info("{} Sending completed merkle tree to {} for {}.{}", previewKind.logPrefix(desc.sessionId), initiator, desc.keyspace, desc.columnFamily);
            Tracing.traceRepair("Sending completed merkle tree to {} for {}.{}", initiator, desc.keyspace, desc.columnFamily);
        }
        MessagingService.instance().sendOneWay(new ValidationComplete(desc, trees).createMessage(), initiator);
    }
}
