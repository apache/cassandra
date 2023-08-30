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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTree.RowHash;
import org.apache.cassandra.utils.MerkleTrees;

import static org.apache.cassandra.net.Verb.VALIDATION_RSP;

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
    public final long nowInSec;
    private final boolean evenTreeDistribution;
    public final boolean isIncremental;
    public final SharedContext ctx;

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
    public final ValidationState state;
    public TopPartitionTracker.Collector topPartitionCollector;

    public Validator(ValidationState state, long nowInSec, PreviewKind previewKind)
    {
        this(SharedContext.Global.instance, state, nowInSec, false, false, previewKind);
    }

    public Validator(SharedContext ctx, ValidationState state, long nowInSec, boolean isIncremental, PreviewKind previewKind)
    {
        this(ctx, state, nowInSec, false, isIncremental, previewKind);
    }

    public Validator(ValidationState state, long nowInSec, boolean isIncremental, PreviewKind previewKind)
    {
        this(SharedContext.Global.instance, state, nowInSec, false, isIncremental, previewKind);
    }

    public Validator(SharedContext ctx, ValidationState state, long nowInSec, boolean evenTreeDistribution, boolean isIncremental, PreviewKind previewKind)
    {
        this.ctx = ctx;
        this.state = state;
        this.desc = state.desc;
        this.initiator = state.initiator;
        this.nowInSec = nowInSec;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
        validated = 0;
        range = null;
        ranges = null;
        this.evenTreeDistribution = evenTreeDistribution;
    }

    public void prepare(ColumnFamilyStore cfs, MerkleTrees trees, TopPartitionTracker.Collector topPartitionCollector)
    {
        this.trees = trees;
        this.topPartitionCollector = topPartitionCollector;

        if (!trees.partitioner().preservesOrder() || evenTreeDistribution)
        {
            // You can't beat even trees distribution for md5
            trees.init();
        }
        else
        {
            List<DecoratedKey> keys = new ArrayList<>();
            Random random = ctx.random().get();

            for (Range<Token> range : trees.ranges())
            {
                for (DecoratedKey sample : cfs.keySamples(range))
                {
                    assert range.contains(sample.getToken()) : "Token " + sample.getToken() + " is not within range " + desc.ranges;
                    keys.add(sample);
                }

                if (keys.isEmpty())
                {
                    // use even trees distribution
                    trees.init(range);
                }
                else
                {
                    int numKeys = keys.size();
                    // sample the column family using random keys from the index
                    while (true)
                    {
                        DecoratedKey dk = keys.get(random.nextInt(numKeys));
                        if (!trees.split(dk.getToken()))
                            break;
                    }
                    keys.clear();
                }
            }
        }
        logger.debug("Prepared AEService trees of size {} for {}", this.trees.size(), desc);
        ranges = trees.rangeIterator();
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
            ranges = trees.rangeIterator();
            findCorrectRange(lastKey.getToken());
        }

        assert range.contains(lastKey.getToken()) : "Token not in MerkleTree: " + lastKey.getToken();
        // case 3 must be true: mix in the hashed row
        RowHash rowHash = rowHash(partition);
        if (rowHash != null)
        {
            if(topPartitionCollector != null)
                topPartitionCollector.trackPartitionSize(partition.partitionKey(), rowHash.size);
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

    private MerkleTree.RowHash rowHash(UnfilteredRowIterator partition)
    {
        validated++;
        // MerkleTree uses XOR internally, so we want lots of output bits here
        Digest digest = Digest.forValidator();
        UnfilteredRowIterators.digest(partition, digest, MessagingService.current_version);
        // only return new hash for merkle tree in case digest was updated - see CASSANDRA-8979
        return digest.inputBytes() > 0
             ? new MerkleTree.RowHash(partition.partitionKey().getToken(), digest.digest(), digest.inputBytes())
             : null;
    }

    /**
     * Registers the newly created tree for rendezvous in Stage.ANTIENTROPY.
     */
    public void complete()
    {
        assert ranges != null : "Validator was not prepared()";

        if (logger.isDebugEnabled())
        {
            // log distribution of rows in tree
            logger.debug("Validated {} partitions for {}.  Partitions per leaf are:", validated, desc.sessionId);
            trees.logRowCountPerLeaf(logger);
            logger.debug("Validated {} partitions for {}.  Partition sizes are:", validated, desc.sessionId);
            trees.logRowSizePerLeaf(logger);
        }

        state.phase.sendingTrees();
        Stage.ANTI_ENTROPY.execute(this);
    }

    /**
     * Called when some error during the validation happened.
     * This sends RepairStatus to inform the initiator that the validation has failed.
     * The actual reason for failure should be looked up in the log of the host calling this function.
     */
    public void fail(Throwable e)
    {
        state.phase.fail(e);
        respond(new ValidationResponse(desc));
    }

    /**
     * Called after the validation lifecycle to respond with the now valid tree. Runs in Stage.ANTIENTROPY.
     */
    public void run()
    {
        if (initiatorIsRemote())
        {
            logger.info("{} Sending completed merkle tree to {} for {}.{}", previewKind.logPrefix(desc.sessionId), initiator, desc.keyspace, desc.columnFamily);
            Tracing.traceRepair("Sending completed merkle tree to {} for {}.{}", initiator, desc.keyspace, desc.columnFamily);
        }
        else
        {
            logger.info("{} Local completed merkle tree for {} for {}.{}", previewKind.logPrefix(desc.sessionId), initiator, desc.keyspace, desc.columnFamily);
            Tracing.traceRepair("Local completed merkle tree for {} for {}.{}", initiator, desc.keyspace, desc.columnFamily);

        }
        state.phase.success();
        respond(new ValidationResponse(desc, trees));
    }

    public PreviewKind getPreviewKind()
    {
        return previewKind;
    }

    private boolean initiatorIsRemote()
    {
        return !FBUtilities.getBroadcastAddressAndPort().equals(initiator);
    }

    @VisibleForTesting
    void respond(ValidationResponse response)
    {
        if (initiatorIsRemote())
        {
            RepairMessage.sendMessageWithRetries(ctx, response, VALIDATION_RSP, initiator);
            return;
        }

        /*
         * For local initiators, DO NOT send the message to self over loopback. This is a wasted ser/de loop
         * and a ton of garbage. Instead, move the trees off heap and invoke message handler. We could do it
         * directly, since this method will only be called from {@code Stage.ANTI_ENTROPY}, but we do instead
         * execute a {@code Runnable} on the stage - in case that assumption ever changes by accident.
         */
        Stage.ANTI_ENTROPY.execute(() ->
        {
            ValidationResponse movedResponse = response;
            try
            {
                movedResponse = response.tryMoveOffHeap();
            }
            catch (IOException e)
            {
                logger.error("Failed to move local merkle tree for {} off heap", desc, e);
            }
            ctx.repair().handleMessage(Message.out(VALIDATION_RSP, movedResponse));
        });
    }
}
