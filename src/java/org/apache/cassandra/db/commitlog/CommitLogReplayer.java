/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

public class CommitLogReplayer implements CommitLogReadHandler
{
    @VisibleForTesting
    public static long MAX_OUTSTANDING_REPLAY_BYTES = Long.getLong("cassandra.commitlog_max_outstanding_replay_bytes", 1024 * 1024 * 64);
    @VisibleForTesting
    public static MutationInitiator mutationInitiator = new MutationInitiator();
    static final String IGNORE_REPLAY_ERRORS_PROPERTY = Config.PROPERTY_PREFIX + "commitlog.ignorereplayerrors";
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayer.class);
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = Integer.getInteger(Config.PROPERTY_PREFIX + "commitlog_max_outstanding_replay_count", 1024);

    private final Set<Keyspace> keyspacesReplayed;
    private final Queue<Future<Integer>> futures;

    private final AtomicInteger replayedCount;
    private final Map<UUID, IntervalSet<CommitLogPosition>> cfPersisted;
    private final CommitLogPosition globalPosition;

    // Used to throttle speed of replay of mutations if we pass the max outstanding count
    private long pendingMutationBytes = 0;

    private final ReplayFilter replayFilter;
    private final CommitLogArchiver archiver;

    @VisibleForTesting
    protected CommitLogReader commitLogReader;

    CommitLogReplayer(CommitLog commitLog,
                      CommitLogPosition globalPosition,
                      Map<UUID, IntervalSet<CommitLogPosition>> cfPersisted,
                      ReplayFilter replayFilter)
    {
        this.keyspacesReplayed = new NonBlockingHashSet<Keyspace>();
        this.futures = new ArrayDeque<Future<Integer>>();
        // count the number of replayed mutation. We don't really care about atomicity, but we need it to be a reference.
        this.replayedCount = new AtomicInteger();
        this.cfPersisted = cfPersisted;
        this.globalPosition = globalPosition;
        this.replayFilter = replayFilter;
        this.archiver = commitLog.archiver;
        this.commitLogReader = new CommitLogReader();
    }

    public static CommitLogReplayer construct(CommitLog commitLog)
    {
        // compute per-CF and global replay intervals
        Map<UUID, IntervalSet<CommitLogPosition>> cfPersisted = new HashMap<>();
        ReplayFilter replayFilter = ReplayFilter.create();

        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            // but, if we've truncated the cf in question, then we need to need to start replay after the truncation
            CommitLogPosition truncatedAt = SystemKeyspace.getTruncatedPosition(cfs.metadata.cfId);
            if (truncatedAt != null)
            {
                // Point in time restore is taken to mean that the tables need to be replayed even if they were
                // deleted at a later point in time. Any truncation record after that point must thus be cleared prior
                // to replay (CASSANDRA-9195).
                long restoreTime = commitLog.archiver.restorePointInTime;
                long truncatedTime = SystemKeyspace.getTruncatedAt(cfs.metadata.cfId);
                if (truncatedTime > restoreTime)
                {
                    if (replayFilter.includes(cfs.metadata))
                    {
                        logger.info("Restore point in time is before latest truncation of table {}.{}. Clearing truncation record.",
                                    cfs.metadata.ksName,
                                    cfs.metadata.cfName);
                        SystemKeyspace.removeTruncationRecord(cfs.metadata.cfId);
                        truncatedAt = null;
                    }
                }
            }

            IntervalSet<CommitLogPosition> filter = persistedIntervals(cfs.getLiveSSTables(), truncatedAt);
            cfPersisted.put(cfs.metadata.cfId, filter);
        }
        CommitLogPosition globalPosition = firstNotCovered(cfPersisted.values());
        logger.debug("Global replay position is {} from columnfamilies {}", globalPosition, FBUtilities.toString(cfPersisted));
        return new CommitLogReplayer(commitLog, globalPosition, cfPersisted, replayFilter);
    }

    public void replayPath(File file, boolean tolerateTruncation) throws IOException
    {
        commitLogReader.readCommitLogSegment(this, file, globalPosition, CommitLogReader.ALL_MUTATIONS, tolerateTruncation);
    }

    public void replayFiles(File[] clogs) throws IOException
    {
        commitLogReader.readAllFiles(this, clogs, globalPosition);
    }

    /**
     * Flushes all keyspaces associated with this replayer in parallel, blocking until their flushes are complete.
     * @return the number of mutations replayed
     */
    public int blockForWrites()
    {
        for (Map.Entry<UUID, AtomicInteger> entry : commitLogReader.getInvalidMutations())
            logger.warn("Skipped {} mutations from unknown (probably removed) CF with id {}", entry.getValue(), entry.getKey());

        // wait for all the writes to finish on the mutation stage
        FBUtilities.waitOnFutures(futures);
        logger.trace("Finished waiting on mutations from recovery");

        // flush replayed keyspaces
        futures.clear();
        boolean flushingSystem = false;

        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Keyspace keyspace : keyspacesReplayed)
        {
            if (keyspace.getName().equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                flushingSystem = true;

            futures.addAll(keyspace.flush());
        }

        // also flush batchlog incase of any MV updates
        if (!flushingSystem)
            futures.add(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceFlush());

        FBUtilities.waitOnFutures(futures);

        return replayedCount.get();
    }

    /*
     * Wrapper around initiating mutations read from the log to make it possible
     * to spy on initiated mutations for test
     */
    @VisibleForTesting
    public static class MutationInitiator
    {
        protected Future<Integer> initiateMutation(final Mutation mutation,
                                                   final long segmentId,
                                                   final int serializedSize,
                                                   final int entryLocation,
                                                   final CommitLogReplayer commitLogReplayer)
        {
            Runnable runnable = new WrappedRunnable()
            {
                public void runMayThrow()
                {
                    if (Schema.instance.getKSMetaData(mutation.getKeyspaceName()) == null)
                        return;
                    if (commitLogReplayer.pointInTimeExceeded(mutation))
                        return;

                    final Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());

                    // Rebuild the mutation, omitting column families that
                    //    a) the user has requested that we ignore,
                    //    b) have already been flushed,
                    // or c) are part of a cf that was dropped.
                    // Keep in mind that the cf.name() is suspect. do every thing based on the cfid instead.
                    Mutation newMutation = null;
                    for (PartitionUpdate update : commitLogReplayer.replayFilter.filter(mutation))
                    {
                        if (Schema.instance.getCF(update.metadata().cfId) == null)
                            continue; // dropped

                        // replay if current segment is newer than last flushed one or,
                        // if it is the last known segment, if we are after the commit log segment position
                        if (commitLogReplayer.shouldReplay(update.metadata().cfId, new CommitLogPosition(segmentId, entryLocation)))
                        {
                            if (newMutation == null)
                                newMutation = new Mutation(mutation.getKeyspaceName(), mutation.key());
                            newMutation.add(update);
                            commitLogReplayer.replayedCount.incrementAndGet();
                        }
                    }
                    if (newMutation != null)
                    {
                        assert !newMutation.isEmpty();

                        Keyspace.open(newMutation.getKeyspaceName()).apply(newMutation, false, true, false);
                        commitLogReplayer.keyspacesReplayed.add(keyspace);
                    }
                }
            };
            return StageManager.getStage(Stage.MUTATION).submit(runnable, serializedSize);
        }
    }

    /**
     * A set of known safe-to-discard commit log replay positions, based on
     * the range covered by on disk sstables and those prior to the most recent truncation record
     */
    public static IntervalSet<CommitLogPosition> persistedIntervals(Iterable<SSTableReader> onDisk, CommitLogPosition truncatedAt)
    {
        IntervalSet.Builder<CommitLogPosition> builder = new IntervalSet.Builder<>();
        for (SSTableReader reader : onDisk)
            builder.addAll(reader.getSSTableMetadata().commitLogIntervals);

        if (truncatedAt != null)
            builder.add(CommitLogPosition.NONE, truncatedAt);
        return builder.build();
    }

    /**
     * Find the earliest commit log position that is not covered by the known flushed ranges for some table.
     *
     * For efficiency this assumes that the first contiguously flushed interval we know of contains the moment that the
     * given table was constructed* and hence we can start replay from the end of that interval.
     *
     * If such an interval is not known, we must replay from the beginning.
     *
     * * This is not true only until if the very first flush of a table stalled or failed, while the second or latter
     *   succeeded. The chances of this happening are at most very low, and if the assumption does prove to be
     *   incorrect during replay there is little chance that the affected deployment is in production.
     */
    public static CommitLogPosition firstNotCovered(Collection<IntervalSet<CommitLogPosition>> ranges)
    {
        return ranges.stream()
                .map(intervals -> Iterables.getFirst(intervals.ends(), CommitLogPosition.NONE))
                .min(Ordering.natural())
                .get(); // iteration is per known-CF, there must be at least one.
    }

    abstract static class ReplayFilter
    {
        public abstract Iterable<PartitionUpdate> filter(Mutation mutation);

        public abstract boolean includes(CFMetaData metadata);

        public static ReplayFilter create()
        {
            // If no replaylist is supplied an empty array of strings is used to replay everything.
            if (System.getProperty("cassandra.replayList") == null)
                return new AlwaysReplayFilter();

            Multimap<String, String> toReplay = HashMultimap.create();
            for (String rawPair : System.getProperty("cassandra.replayList").split(","))
            {
                String[] pair = StringUtils.split(rawPair.trim(), '.');
                if (pair.length != 2)
                    throw new IllegalArgumentException("Each table to be replayed must be fully qualified with keyspace name, e.g., 'system.peers'");

                Keyspace ks = Schema.instance.getKeyspaceInstance(pair[0]);
                if (ks == null)
                    throw new IllegalArgumentException("Unknown keyspace " + pair[0]);
                ColumnFamilyStore cfs = ks.getColumnFamilyStore(pair[1]);
                if (cfs == null)
                    throw new IllegalArgumentException(String.format("Unknown table %s.%s", pair[0], pair[1]));

                toReplay.put(pair[0], pair[1]);
            }
            return new CustomReplayFilter(toReplay);
        }
    }

    private static class AlwaysReplayFilter extends ReplayFilter
    {
        public Iterable<PartitionUpdate> filter(Mutation mutation)
        {
            return mutation.getPartitionUpdates();
        }

        public boolean includes(CFMetaData metadata)
        {
            return true;
        }
    }

    private static class CustomReplayFilter extends ReplayFilter
    {
        private Multimap<String, String> toReplay;

        public CustomReplayFilter(Multimap<String, String> toReplay)
        {
            this.toReplay = toReplay;
        }

        public Iterable<PartitionUpdate> filter(Mutation mutation)
        {
            final Collection<String> cfNames = toReplay.get(mutation.getKeyspaceName());
            if (cfNames == null)
                return Collections.emptySet();

            return Iterables.filter(mutation.getPartitionUpdates(), new Predicate<PartitionUpdate>()
            {
                public boolean apply(PartitionUpdate upd)
                {
                    return cfNames.contains(upd.metadata().cfName);
                }
            });
        }

        public boolean includes(CFMetaData metadata)
        {
            return toReplay.containsEntry(metadata.ksName, metadata.cfName);
        }
    }

    /**
     * consult the known-persisted ranges for our sstables;
     * if the position is covered by one of them it does not need to be replayed
     *
     * @return true iff replay is necessary
     */
    private boolean shouldReplay(UUID cfId, CommitLogPosition position)
    {
        return !cfPersisted.get(cfId).contains(position);
    }

    protected boolean pointInTimeExceeded(Mutation fm)
    {
        long restoreTarget = archiver.restorePointInTime;

        for (PartitionUpdate upd : fm.getPartitionUpdates())
        {
            if (archiver.precision.toMillis(upd.maxTimestamp()) > restoreTarget)
                return true;
        }
        return false;
    }

    public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
    {
        pendingMutationBytes += size;
        futures.offer(mutationInitiator.initiateMutation(m,
                                                         desc.id,
                                                         size,
                                                         entryLocation,
                                                         this));
        // If there are finished mutations, or too many outstanding bytes/mutations
        // drain the futures in the queue
        while (futures.size() > MAX_OUTSTANDING_REPLAY_COUNT
               || pendingMutationBytes > MAX_OUTSTANDING_REPLAY_BYTES
               || (!futures.isEmpty() && futures.peek().isDone()))
        {
            pendingMutationBytes -= FBUtilities.waitOnFuture(futures.poll());
        }
    }

    public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
    {
        if (exception.permissible)
            logger.error("Ignoring commit log replay error likely due to incomplete flush to disk", exception);
        else if (Boolean.getBoolean(IGNORE_REPLAY_ERRORS_PROPERTY))
            logger.error("Ignoring commit log replay error", exception);
        else if (!CommitLog.handleCommitError("Failed commit log replay", exception))
        {
            logger.error("Replay stopped. If you wish to override this error and continue starting the node ignoring " +
                         "commit log replay problems, specify -D" + IGNORE_REPLAY_ERRORS_PROPERTY + "=true " +
                         "on the command line");
            throw new CommitLogReplayException(exception.getMessage(), exception);
        }
        return false;
    }

    /**
     * The logic for whether or not we throw on an error is identical for the replayer between recoverable or non.
     */
    public void handleUnrecoverableError(CommitLogReadException exception) throws IOException
    {
        // Don't care about return value, use this simply to throw exception as appropriate.
        shouldSkipSegmentOnError(exception);
    }

    @SuppressWarnings("serial")
    public static class CommitLogReplayException extends IOException
    {
        public CommitLogReplayException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public CommitLogReplayException(String message)
        {
            super(message);
        }
    }
}
