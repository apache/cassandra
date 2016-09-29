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

import java.net.InetAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;

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
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTree.RowHash;
import org.apache.cassandra.utils.MerkleTrees;

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
    public final InetAddress initiator;
    public final int gcBefore;
    private final boolean evenTreeDistribution;

    // null when all rows with the min token have been consumed
    private long validated;
    private MerkleTrees trees;
    // current range being updated
    private MerkleTree.TreeRange range;
    // iterator for iterating sub ranges (MT's leaves)
    private MerkleTrees.TreeRangeIterator ranges;
    // last key seen
    private DecoratedKey lastKey;

    public Validator(RepairJobDesc desc, InetAddress initiator, int gcBefore)
    {
        this(desc, initiator, gcBefore, false);
    }

    public Validator(RepairJobDesc desc, InetAddress initiator, int gcBefore, boolean evenTreeDistribution)
    {
        this.desc = desc;
        this.initiator = initiator;
        this.gcBefore = gcBefore;
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

    static class CountingDigest extends MessageDigest
    {
        private long count;
        private MessageDigest underlying;

        public CountingDigest(MessageDigest underlying)
        {
            super(underlying.getAlgorithm());
            this.underlying = underlying;
        }

        @Override
        protected void engineUpdate(byte input)
        {
            underlying.update(input);
            count += 1;
        }

        @Override
        protected void engineUpdate(byte[] input, int offset, int len)
        {
            underlying.update(input, offset, len);
            count += len;
        }

        @Override
        protected byte[] engineDigest()
        {
            return underlying.digest();
        }

        @Override
        protected void engineReset()
        {
            underlying.reset();
        }

    }

    private MerkleTree.RowHash rowHash(UnfilteredRowIterator partition)
    {
        validated++;
        // MerkleTree uses XOR internally, so we want lots of output bits here
        CountingDigest digest = new CountingDigest(FBUtilities.newMessageDigest("SHA-256"));
        UnfilteredRowIterators.digest(null, partition, digest, MessagingService.current_version);
        // only return new hash for merkle tree in case digest was updated - see CASSANDRA-8979
        return digest.count > 0
             ? new MerkleTree.RowHash(partition.partitionKey().getToken(), digest.digest(), digest.count)
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
        if (!initiator.equals(FBUtilities.getBroadcastAddress()))
        {
            logger.info("[repair #{}] Sending completed merkle tree to {} for {}.{}", desc.sessionId, initiator, desc.keyspace, desc.columnFamily);
            Tracing.traceRepair("Sending completed merkle tree to {} for {}.{}", initiator, desc.keyspace, desc.columnFamily);
        }
        MessagingService.instance().sendOneWay(new ValidationComplete(desc, trees).createMessage(), initiator);
    }
}
