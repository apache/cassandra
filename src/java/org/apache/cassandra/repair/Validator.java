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
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTree.RowHash;

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

    // null when all rows with the min token have been consumed
    private long validated;
    private MerkleTree tree;
    // current range being updated
    private MerkleTree.TreeRange range;
    // iterator for iterating sub ranges (MT's leaves)
    private MerkleTree.TreeRangeIterator ranges;
    // last key seen
    private DecoratedKey lastKey;

    public Validator(RepairJobDesc desc, InetAddress initiator, int gcBefore)
    {
        this.desc = desc;
        this.initiator = initiator;
        this.gcBefore = gcBefore;
        validated = 0;
        range = null;
        ranges = null;
    }

    public void prepare(ColumnFamilyStore cfs, MerkleTree tree)
    {
        this.tree = tree;

        if (!tree.partitioner().preservesOrder())
        {
            // You can't beat an even tree distribution for md5
            tree.init();
        }
        else
        {
            List<DecoratedKey> keys = new ArrayList<>();
            for (DecoratedKey sample : cfs.keySamples(desc.range))
            {
                assert desc.range.contains(sample.getToken()): "Token " + sample.getToken() + " is not within range " + desc.range;
                keys.add(sample);
            }

            if (keys.isEmpty())
            {
                // use an even tree distribution
                tree.init();
            }
            else
            {
                int numkeys = keys.size();
                Random random = new Random();
                // sample the column family using random keys from the index
                while (true)
                {
                    DecoratedKey dk = keys.get(random.nextInt(numkeys));
                    if (!tree.split(dk.getToken()))
                        break;
                }
            }
        }
        logger.debug("Prepared AEService tree of size {} for {}", tree.size(), desc);
        ranges = tree.invalids();
    }

    /**
     * Called (in order) for every row present in the CF.
     * Hashes the row, and adds it to the tree being built.
     *
     * @param row Row to add hash
     */
    public void add(AbstractCompactedRow row)
    {
        assert desc.range.contains(row.key.getToken()) : row.key.getToken() + " is not contained in " + desc.range;
        assert lastKey == null || lastKey.compareTo(row.key) < 0
               : "row " + row.key + " received out of order wrt " + lastKey;
        lastKey = row.key;

        if (range == null)
            range = ranges.next();

        // generate new ranges as long as case 1 is true
        while (!range.contains(row.key.getToken()))
        {
            // add the empty hash, and move to the next range
            range.ensureHashInitialised();
            range = ranges.next();
        }

        // case 3 must be true: mix in the hashed row
        RowHash rowHash = rowHash(row);
        if (rowHash != null)
        {
            range.addHash(rowHash);
        }
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

    private MerkleTree.RowHash rowHash(AbstractCompactedRow row)
    {
        validated++;
        // MerkleTree uses XOR internally, so we want lots of output bits here
        CountingDigest digest = new CountingDigest(FBUtilities.newMessageDigest("SHA-256"));
        row.update(digest);
        // only return new hash for merkle tree in case digest was updated - see CASSANDRA-8979
        if (digest.count > 0)
        {
            return new MerkleTree.RowHash(row.key.getToken(), digest.digest(), digest.count);
        }
        else
        {
            return null;
        }
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
            tree.histogramOfRowCountPerLeaf().log(logger);
            logger.debug("Validated {} partitions for {}.  Partition sizes are:", validated, desc.sessionId);
            tree.histogramOfRowSizePerLeaf().log(logger);
        }
    }

    @VisibleForTesting
    public void completeTree()
    {
        assert ranges != null : "Validator was not prepared()";

        if (range != null)
            range.ensureHashInitialised();
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
            logger.info(String.format("[repair #%s] Sending completed merkle tree to %s for %s/%s", desc.sessionId, initiator, desc.keyspace, desc.columnFamily));
        MessagingService.instance().sendOneWay(new ValidationComplete(desc, tree).createMessage(), initiator);
    }
}
