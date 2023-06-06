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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import org.apache.cassandra.io.util.File;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.utils.concurrent.Future;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_IGNORE_REPLAY_ERRORS;
import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_MAX_OUTSTANDING_REPLAY_BYTES;
import static org.apache.cassandra.config.CassandraRelevantProperties.COMMITLOG_MAX_OUTSTANDING_REPLAY_COUNT;
import static org.apache.cassandra.config.CassandraRelevantProperties.COMMIT_LOG_REPLAY_LIST;

public class CommitLogReplayer implements CommitLogReadHandler
{
    @VisibleForTesting
    public static long MAX_OUTSTANDING_REPLAY_BYTES = COMMITLOG_MAX_OUTSTANDING_REPLAY_BYTES.getLong();
    @VisibleForTesting
    public static MutationInitiator mutationInitiator = new MutationInitiator();
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayer.class);
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = COMMITLOG_MAX_OUTSTANDING_REPLAY_COUNT.getInt();

    private final Set<Keyspace> keyspacesReplayed;
    private final Queue<Future<Integer>> futures;

    private final AtomicInteger replayedCount;
    private final Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted;
    private final CommitLogPosition globalPosition;

    // Used to throttle speed of replay of mutations if we pass the max outstanding count
    private long pendingMutationBytes = 0;

    private final ReplayFilter replayFilter;
    private final CommitLogArchiver archiver;

    @VisibleForTesting
    protected boolean sawCDCMutation;

    @VisibleForTesting
    protected CommitLogReader commitLogReader;

    CommitLogReplayer(CommitLog commitLog,
                      CommitLogPosition globalPosition,
                      Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted,
                      ReplayFilter replayFilter)
    {
        this.keyspacesReplayed = new NonBlockingHashSet<>();
        this.futures = new ArrayDeque<>();
        // count the number of replayed mutation. We don't really care about atomicity, but we need it to be a reference.
        this.replayedCount = new AtomicInteger();
        this.cfPersisted = cfPersisted;
        this.globalPosition = globalPosition;
        this.replayFilter = replayFilter;
        this.archiver = commitLog.archiver;
        this.commitLogReader = new CommitLogReader();
    }

    public static CommitLogReplayer construct(CommitLog commitLog, UUID localHostId)
    {
        // compute per-CF and global replay intervals
        Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted = new HashMap<>();
        ReplayFilter replayFilter = ReplayFilter.create();

        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            // but, if we've truncated the cf in question, then we need to need to start replay after the truncation
            CommitLogPosition truncatedAt = SystemKeyspace.getTruncatedPosition(cfs.metadata.id);
            if (truncatedAt != null)
            {
                // Point in time restore is taken to mean that the tables need to be replayed even if they were
                // deleted at a later point in time. Any truncation record after that point must thus be cleared prior
                // to replay (CASSANDRA-9195).
                long restoreTime = commitLog.archiver.restorePointInTime;
                long truncatedTime = SystemKeyspace.getTruncatedAt(cfs.metadata.id);
                if (truncatedTime > restoreTime)
                {
                    if (replayFilter.includes(cfs.metadata))
                    {
                        logger.info("Restore point in time is before latest truncation of table {}.{}. Clearing truncation record.",
                                    cfs.metadata.keyspace,
                                    cfs.metadata.name);
                        SystemKeyspace.removeTruncationRecord(cfs.metadata.id);
                        truncatedAt = null;
                    }
                }
            }

            IntervalSet<CommitLogPosition> filter;
            final CommitLogPosition snapshotPosition = commitLog.archiver.snapshotCommitLogPosition;
            if (snapshotPosition == CommitLogPosition.NONE)
            {
                // normal path: snapshot position is not explicitly specified, find it from sstables
                if (!cfs.memtableWritesAreDurable())
                {
                    filter = persistedIntervals(cfs.getLiveSSTables(), truncatedAt, localHostId);
                }
                else
                {
                    if (commitLog.archiver.restorePointInTime == Long.MAX_VALUE)
                    {
                        // Normal restart, everything is persisted and restored by the memtable itself.
                        filter = new IntervalSet<>(CommitLogPosition.NONE, CommitLog.instance.getCurrentPosition());
                    }
                    else
                    {
                        // Point-in-time restore with a persistent memtable. In this case user should have restored
                        // the memtable from a snapshot and specified that snapshot's commit log position, reaching
                        // the "else" path below.
                        // If they haven't, do not filter any commit log data -- this supports a mode of operation where
                        // the user deletes old archived commit log segments when a snapshot completes -- but issue a
                        // message as this may be inefficient / not what the user wants.
                        logger.info("Point-in-time restore on a persistent memtable started without a snapshot time. " +
                                    "All commit log data will be replayed.");
                        filter = IntervalSet.empty();
                    }
                }
            }
            else
            {
                // If the positions is specified, it must override whatever we calculate.
                filter = new IntervalSet<>(CommitLogPosition.NONE, snapshotPosition);
            }
            cfPersisted.put(cfs.metadata.id, filter);
        }
        CommitLogPosition globalPosition = firstNotCovered(cfPersisted.values());
        logger.debug("Global replay position is {} from columnfamilies {}", globalPosition, FBUtilities.toString(cfPersisted));
        return new CommitLogReplayer(commitLog, globalPosition, cfPersisted, replayFilter);
    }

    public void replayPath(File file, boolean tolerateTruncation) throws IOException
    {
        sawCDCMutation = false;
        commitLogReader.readCommitLogSegment(this, file, globalPosition, CommitLogReader.ALL_MUTATIONS, tolerateTruncation);
        if (sawCDCMutation)
            handleCDCReplayCompletion(file);
    }

    public void replayFiles(File[] clogs) throws IOException
    {
        List<File> filteredLogs = CommitLogReader.filterCommitLogFiles(clogs);
        int i = 0;
        for (File file: filteredLogs)
        {
            i++;
            sawCDCMutation = false;
            commitLogReader.readCommitLogSegment(this, file, globalPosition, i == filteredLogs.size());
            if (sawCDCMutation)
                handleCDCReplayCompletion(file);
        }
    }


    /**
     * Upon replay completion, CDC needs to hard-link files in the CDC folder and calculate index files so consumers can
     * begin their work.
     */
    private void handleCDCReplayCompletion(File f) throws IOException
    {
        // Can only reach this point if CDC is enabled, thus we have a CDCSegmentManager
        ((CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager).addCDCSize(f.length());

        File dest = new File(DatabaseDescriptor.getCDCLogLocation(), f.name());

        // If hard link already exists, assume it's from a previous node run. If people are mucking around in the cdc_raw
        // directory that's on them.
        if (!dest.exists())
            FileUtils.createHardLink(f, dest);

        // The reader has already verified we can deserialize the descriptor.
        CommitLogDescriptor desc;
        try(RandomAccessReader reader = RandomAccessReader.open(f))
        {
            desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
            assert desc != null;
            assert f.length() < Integer.MAX_VALUE;
            CommitLogSegment.writeCDCIndexFile(desc, (int)f.length(), true);
        }
    }


    /**
     * Flushes all keyspaces associated with this replayer in parallel, blocking until their flushes are complete.
     * @return the number of mutations replayed
     */
    public int blockForWrites()
    {
        for (Map.Entry<TableId, AtomicInteger> entry : commitLogReader.getInvalidMutations())
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

            futures.addAll(keyspace.flush(ColumnFamilyStore.FlushReason.STARTUP));
        }

        // also flush batchlog incase of any MV updates
        if (!flushingSystem)
            futures.add(Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                .getColumnFamilyStore(SystemKeyspace.BATCHES)
                                .forceFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED));

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
                    if (Schema.instance.getKeyspaceMetadata(mutation.getKeyspaceName()) == null)
                        return;
                    if (commitLogReplayer.pointInTimeExceeded(mutation))
                        return;

                    final Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());

                    // Rebuild the mutation, omitting column families that
                    //    a) the user has requested that we ignore,
                    //    b) have already been flushed,
                    // or c) are part of a cf that was dropped.
                    // Keep in mind that the cf.name() is suspect. do every thing based on the cfid instead.
                    Mutation.PartitionUpdateCollector newPUCollector = null;
                    for (PartitionUpdate update : commitLogReplayer.replayFilter.filter(mutation))
                    {
                        if (Schema.instance.getTableMetadata(update.metadata().id) == null)
                            continue; // dropped

                        // replay if current segment is newer than last flushed one or,
                        // if it is the last known segment, if we are after the commit log segment position
                        if (commitLogReplayer.shouldReplay(update.metadata().id, new CommitLogPosition(segmentId, entryLocation)))
                        {
                            if (newPUCollector == null)
                                newPUCollector = new Mutation.PartitionUpdateCollector(mutation.getKeyspaceName(), mutation.key());
                            newPUCollector.add(update);
                            commitLogReplayer.replayedCount.incrementAndGet();
                        }
                    }
                    if (newPUCollector != null)
                    {
                        assert !newPUCollector.isEmpty();

                        Keyspace.open(newPUCollector.getKeyspaceName()).apply(newPUCollector.build(), false, true, false);
                        commitLogReplayer.keyspacesReplayed.add(keyspace);
                    }
                }
            };
            return Stage.MUTATION.submit(runnable, serializedSize);
        }
    }

    /**
     * A set of known safe-to-discard commit log replay positions, based on
     * the range covered by on disk sstables and those prior to the most recent truncation record
     */
    public static IntervalSet<CommitLogPosition> persistedIntervals(Iterable<SSTableReader> onDisk,
                                                                    CommitLogPosition truncatedAt,
                                                                    UUID localhostId)
    {
        IntervalSet.Builder<CommitLogPosition> builder = new IntervalSet.Builder<>();
        List<String> skippedSSTables = new ArrayList<>();
        for (SSTableReader reader : onDisk)
        {
            UUID originatingHostId = reader.getSSTableMetadata().originatingHostId;
            if (originatingHostId != null && originatingHostId.equals(localhostId))
                builder.addAll(reader.getSSTableMetadata().commitLogIntervals);
            else
                skippedSSTables.add(reader.getFilename());
        }

        if (!skippedSSTables.isEmpty()) {
            logger.warn("Origin of {} sstables is unknown or doesn't match the local node; commitLogIntervals for them were ignored", skippedSSTables.size());
            logger.debug("Ignored commitLogIntervals from the following sstables: {}", skippedSSTables);
        }

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

        public abstract boolean includes(TableMetadataRef metadata);

        /**
         * Creates filter for entities to replay mutations for upon commit log replay.
         *
         * @see org.apache.cassandra.config.CassandraRelevantProperties#COMMIT_LOG_REPLAY_LIST
         * */
        public static ReplayFilter create()
        {
            String replayList = COMMIT_LOG_REPLAY_LIST.getString();

            if (replayList == null)
                return new AlwaysReplayFilter();

            Multimap<String, String> toReplay = HashMultimap.create();
            for (String rawPair : replayList.split(","))
            {
                String trimmedRawPair = rawPair.trim();
                if (trimmedRawPair.isEmpty() || trimmedRawPair.endsWith("."))
                    throw new IllegalArgumentException(format("Invalid pair: '%s'", trimmedRawPair));

                String[] pair = StringUtils.split(trimmedRawPair, '.');

                if (pair.length > 2)
                    throw new IllegalArgumentException(format("%s property contains an item which " +
                                                              "is not in format 'keyspace' or 'keyspace.table' " +
                                                              "but it is '%s'",
                                                              COMMIT_LOG_REPLAY_LIST.getKey(),
                                                              String.join(".", pair)));

                String keyspaceName = pair[0];

                Keyspace ks = Schema.instance.getKeyspaceInstance(keyspaceName);
                if (ks == null)
                    throw new IllegalArgumentException("Unknown keyspace " + keyspaceName);

                if (pair.length == 1)
                {
                    for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
                        toReplay.put(keyspaceName, cfs.name);
                }
                else
                {
                    ColumnFamilyStore cfs = ks.getColumnFamilyStore(pair[1]);
                    if (cfs == null)
                        throw new IllegalArgumentException(format("Unknown table %s.%s", keyspaceName, pair[1]));

                    toReplay.put(keyspaceName, pair[1]);
                }
            }

            if (toReplay.isEmpty())
                logger.info("All tables will be included in commit log replay.");
            else
                logger.info("Tables to be replayed: {}", toReplay.asMap().toString());

            return new CustomReplayFilter(toReplay);
        }
    }

    private static class AlwaysReplayFilter extends ReplayFilter
    {
        public Iterable<PartitionUpdate> filter(Mutation mutation)
        {
            return mutation.getPartitionUpdates();
        }

        public boolean includes(TableMetadataRef metadata)
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
                    return cfNames.contains(upd.metadata().name);
                }
            });
        }

        public boolean includes(TableMetadataRef metadata)
        {
            return toReplay.containsEntry(metadata.keyspace, metadata.name);
        }
    }

    /**
     * consult the known-persisted ranges for our sstables;
     * if the position is covered by one of them it does not need to be replayed
     *
     * @return true iff replay is necessary
     */
    private boolean shouldReplay(TableId tableId, CommitLogPosition position)
    {
        return !cfPersisted.get(tableId).contains(position);
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
        if (DatabaseDescriptor.isCDCEnabled() && m.trackedByCDC())
            sawCDCMutation = true;

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
        else if (COMMITLOG_IGNORE_REPLAY_ERRORS.getBoolean())
            logger.error("Ignoring commit log replay error", exception);
        else if (!CommitLog.handleCommitError("Failed commit log replay", exception))
        {
            logger.error("Replay stopped. If you wish to override this error and continue starting the node ignoring " +
                         "commit log replay problems, specify -D{}=true on the command line",
                         COMMITLOG_IGNORE_REPLAY_ERRORS.getKey());
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
