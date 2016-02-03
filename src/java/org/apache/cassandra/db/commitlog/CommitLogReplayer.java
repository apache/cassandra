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

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.db.commitlog.SegmentReader.SyncSegment;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

public class CommitLogReplayer
{
    @VisibleForTesting
    public static long MAX_OUTSTANDING_REPLAY_BYTES = Long.getLong("cassandra.commitlog_max_outstanding_replay_bytes", 1024 * 1024 * 64);
    @VisibleForTesting
    public static MutationInitiator mutationInitiator = new MutationInitiator();
    static final String IGNORE_REPLAY_ERRORS_PROPERTY = "cassandra.commitlog.ignorereplayerrors";
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayer.class);
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = Integer.getInteger("cassandra.commitlog_max_outstanding_replay_count", 1024);
    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;

    private final Set<Keyspace> keyspacesRecovered;
    private final Queue<Future<Integer>> futures;
    private final Map<UUID, AtomicInteger> invalidMutations;
    private final AtomicInteger replayedCount;
    private final Map<UUID, ReplayPosition> cfPositions;
    private final ReplayPosition globalPosition;
    private final CRC32 checksum;
    private byte[] buffer;
    private long pendingMutationBytes = 0;

    private final ReplayFilter replayFilter;
    private final CommitLogArchiver archiver;

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
                                                   final long entryLocation,
                                                   final CommitLogReplayer clr)
        {
            Runnable runnable = new WrappedRunnable()
            {
                public void runMayThrow()
                {
                    if (Schema.instance.getKSMetaData(mutation.getKeyspaceName()) == null)
                        return;
                    if (clr.pointInTimeExceeded(mutation))
                        return;

                    final Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());

                    // Rebuild the mutation, omitting column families that
                    //    a) the user has requested that we ignore,
                    //    b) have already been flushed,
                    // or c) are part of a cf that was dropped.
                    // Keep in mind that the cf.name() is suspect. do every thing based on the cfid instead.
                    Mutation newMutation = null;
                    for (PartitionUpdate update : clr.replayFilter.filter(mutation))
                    {
                        if (Schema.instance.getCF(update.metadata().cfId) == null)
                            continue; // dropped

                        ReplayPosition rp = clr.cfPositions.get(update.metadata().cfId);

                        // replay if current segment is newer than last flushed one or,
                        // if it is the last known segment, if we are after the replay position
                        if (segmentId > rp.segment || (segmentId == rp.segment && entryLocation > rp.position))
                        {
                            if (newMutation == null)
                                newMutation = new Mutation(mutation.getKeyspaceName(), mutation.key());
                            newMutation.add(update);
                            clr.replayedCount.incrementAndGet();
                        }
                    }
                    if (newMutation != null)
                    {
                        assert !newMutation.isEmpty();

                        try
                        {
                            Uninterruptibles.getUninterruptibly(Keyspace.open(newMutation.getKeyspaceName()).applyFromCommitLog(newMutation));
                        }
                        catch (ExecutionException e)
                        {
                            throw Throwables.propagate(e.getCause());
                        }

                        clr.keyspacesRecovered.add(keyspace);
                    }
                }
            };
            return StageManager.getStage(Stage.MUTATION).submit(runnable, serializedSize);
        }
    }

    CommitLogReplayer(CommitLog commitLog, ReplayPosition globalPosition, Map<UUID, ReplayPosition> cfPositions, ReplayFilter replayFilter)
    {
        this.keyspacesRecovered = new NonBlockingHashSet<Keyspace>();
        this.futures = new ArrayDeque<Future<Integer>>();
        this.buffer = new byte[4096];
        this.invalidMutations = new HashMap<UUID, AtomicInteger>();
        // count the number of replayed mutation. We don't really care about atomicity, but we need it to be a reference.
        this.replayedCount = new AtomicInteger();
        this.checksum = new CRC32();
        this.cfPositions = cfPositions;
        this.globalPosition = globalPosition;
        this.replayFilter = replayFilter;
        this.archiver = commitLog.archiver;
    }

    public static CommitLogReplayer construct(CommitLog commitLog)
    {
        // compute per-CF and global replay positions
        Map<UUID, ReplayPosition> cfPositions = new HashMap<UUID, ReplayPosition>();
        Ordering<ReplayPosition> replayPositionOrdering = Ordering.from(ReplayPosition.comparator);
        ReplayFilter replayFilter = ReplayFilter.create();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            // it's important to call RP.gRP per-cf, before aggregating all the positions w/ the Ordering.min call
            // below: gRP will return NONE if there are no flushed sstables, which is important to have in the
            // list (otherwise we'll just start replay from the first flush position that we do have, which is not correct).
            ReplayPosition rp = ReplayPosition.getReplayPosition(cfs.getSSTables(SSTableSet.CANONICAL));

            // but, if we've truncated the cf in question, then we need to need to start replay after the truncation
            ReplayPosition truncatedAt = SystemKeyspace.getTruncatedPosition(cfs.metadata.cfId);
            if (truncatedAt != null)
            {
                // Point in time restore is taken to mean that the tables need to be recovered even if they were
                // deleted at a later point in time. Any truncation record after that point must thus be cleared prior
                // to recovery (CASSANDRA-9195).
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
                    }
                }
                else
                {
                    rp = replayPositionOrdering.max(Arrays.asList(rp, truncatedAt));
                }
            }

            cfPositions.put(cfs.metadata.cfId, rp);
        }
        ReplayPosition globalPosition = replayPositionOrdering.min(cfPositions.values());
        logger.trace("Global replay position is {} from columnfamilies {}", globalPosition, FBUtilities.toString(cfPositions));
        return new CommitLogReplayer(commitLog, globalPosition, cfPositions, replayFilter);
    }

    public void recover(File[] clogs) throws IOException
    {
        int i;
        for (i = 0; i < clogs.length; ++i)
            recover(clogs[i], i + 1 == clogs.length);
    }

    public int blockForWrites()
    {
        for (Map.Entry<UUID, AtomicInteger> entry : invalidMutations.entrySet())
            logger.warn(String.format("Skipped %d mutations from unknown (probably removed) CF with id %s", entry.getValue().intValue(), entry.getKey()));

        // wait for all the writes to finish on the mutation stage
        FBUtilities.waitOnFutures(futures);
        logger.trace("Finished waiting on mutations from recovery");

        // flush replayed keyspaces
        futures.clear();
        boolean flushingSystem = false;

        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Keyspace keyspace : keyspacesRecovered)
        {
            if (keyspace.getName().equals(SystemKeyspace.NAME))
                flushingSystem = true;

            futures.addAll(keyspace.flush());
        }

        // also flush batchlog incase of any MV updates
        if (!flushingSystem)
            futures.add(Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.BATCHES).forceFlush());

        FBUtilities.waitOnFutures(futures);

        return replayedCount.get();
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

    public void recover(File file, boolean tolerateTruncation) throws IOException
    {
        // just transform from the file name (no reading of headers) to determine version
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());

        try(ChannelProxy channel = new ChannelProxy(file);
            RandomAccessReader reader = RandomAccessReader.open(channel))
        {
            if (desc.version < CommitLogDescriptor.VERSION_21)
            {
                if (logAndCheckIfShouldSkip(file, desc))
                    return;
                if (globalPosition.segment == desc.id)
                    reader.seek(globalPosition.position);
                replaySyncSection(reader, (int) reader.length(), desc, desc.fileName(), tolerateTruncation);
                return;
            }
            final long segmentId = desc.id;
            try
            {
                desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
            }
            catch (Exception e)
            {
                desc = null;
            }

            if (desc == null) {
                handleReplayError(false, "Could not read commit log descriptor in file %s", file);
                return;
            }
            if (segmentId != desc.id)
            {
                handleReplayError(false, "Segment id mismatch (filename %d, descriptor %d) in file %s", segmentId, desc.id, file);
                // continue processing if ignored.
            }

            if (logAndCheckIfShouldSkip(file, desc))
                return;

            SegmentReader segmentReader;
            try
            {
                segmentReader = new SegmentReader(desc, reader, tolerateTruncation);
            }
            catch(Exception e)
            {
                handleReplayError(false, "unable to create segment reader for commit log file: %s", e);
                return;
            }

            try
            {
                boolean tolerateErrorsInSection = tolerateTruncation;
                for (SyncSegment syncSegment : segmentReader)
                {
                    tolerateErrorsInSection &= syncSegment.toleratesErrorsInSection;

                    // Skip over flushed section.
                    if (desc.id == globalPosition.segment && syncSegment.endPosition < globalPosition.position)
                        continue;
                    String errorContext = String.format("next section at %d in %s", syncSegment.fileStartPosition, desc.fileName());
                    if (!replaySyncSection(syncSegment.input, syncSegment.endPosition, desc, errorContext, tolerateErrorsInSection))
                        break;
                }
            }
            // unfortunately, AbstractIterator cannot throw a checked excpetion,
            // so check to see if a RuntimeException is wrapping an IOException
            catch (RuntimeException re)
            {
                if (re.getCause() instanceof IOException)
                    throw (IOException) re.getCause();
                throw re;
            }
            logger.debug("Finished reading {}", file);
        }
    }

    public boolean logAndCheckIfShouldSkip(File file, CommitLogDescriptor desc)
    {
        logger.debug("Replaying {} (CL version {}, messaging version {}, compression {})",
                    file.getPath(),
                    desc.version,
                    desc.getMessagingVersion(),
                    desc.compression);

        if (globalPosition.segment > desc.id)
        {
            logger.trace("skipping replay of fully-flushed {}", file);
            return true;
        }
        return false;
    }

    /**
     * Replays a sync section containing a list of mutations.
     *
     * @return Whether replay should continue with the next section.
     */
    private boolean replaySyncSection(FileDataInput reader, int end, CommitLogDescriptor desc, String errorContext, boolean tolerateErrors) throws IOException
    {
         /* read the logs populate Mutation and apply */
        while (reader.getFilePointer() < end && !reader.isEOF())
        {
            long mutationStart = reader.getFilePointer();
            if (logger.isTraceEnabled())
                logger.trace("Reading mutation at {}", mutationStart);

            long claimedCRC32;
            int serializedSize;
            try
            {
                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.trace("Encountered end of segment marker at {}", reader.getFilePointer());
                    return false;
                }

                // Mutation must be at LEAST 10 bytes:
                // 3 each for a non-empty Keyspace and Key (including the
                // 2-byte length from writeUTF/writeWithShortLength) and 4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    handleReplayError(tolerateErrors,
                                      "Invalid mutation size %d at %d in %s",
                                      serializedSize, mutationStart, errorContext);
                    return false;
                }

                long claimedSizeChecksum;
                if (desc.version < CommitLogDescriptor.VERSION_21)
                    claimedSizeChecksum = reader.readLong();
                else
                    claimedSizeChecksum = reader.readInt() & 0xffffffffL;
                checksum.reset();
                if (desc.version < CommitLogDescriptor.VERSION_20)
                    checksum.update(serializedSize);
                else
                    updateChecksumInt(checksum, serializedSize);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    handleReplayError(tolerateErrors,
                                      "Mutation size checksum failure at %d in %s",
                                      mutationStart, errorContext);
                    return false;
                }
                // ok.

                if (serializedSize > buffer.length)
                    buffer = new byte[(int) (1.2 * serializedSize)];
                reader.readFully(buffer, 0, serializedSize);
                if (desc.version < CommitLogDescriptor.VERSION_21)
                    claimedCRC32 = reader.readLong();
                else
                    claimedCRC32 = reader.readInt() & 0xffffffffL;
            }
            catch (EOFException eof)
            {
                handleReplayError(tolerateErrors,
                                  "Unexpected end of segment",
                                  mutationStart, errorContext);
                return false; // last CL entry didn't get completely written. that's ok.
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                handleReplayError(tolerateErrors,
                                  "Mutation checksum failure at %d in %s",
                                  mutationStart, errorContext);
                continue;
            }
            replayMutation(buffer, serializedSize, reader.getFilePointer(), desc);
        }
        return true;
    }

    /**
     * Deserializes and replays a commit log entry.
     */
    void replayMutation(byte[] inputBuffer, int size,
            final long entryLocation, final CommitLogDescriptor desc) throws IOException
    {

        final Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       desc.getMessagingVersion(),
                                                       SerializationHelper.Flag.LOCAL);
            // doublecheck that what we read is [still] valid for the current schema
            for (PartitionUpdate upd : mutation.getPartitionUpdates())
                upd.validate();
        }
        catch (UnknownColumnFamilyException ex)
        {
            if (ex.cfId == null)
                return;
            AtomicInteger i = invalidMutations.get(ex.cfId);
            if (i == null)
            {
                i = new AtomicInteger(1);
                invalidMutations.put(ex.cfId, i);
            }
            else
                i.incrementAndGet();
            return;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            File f = File.createTempFile("mutation", "dat");

            try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f)))
            {
                out.write(inputBuffer, 0, size);
            }

            // Checksum passed so this error can't be permissible.
            handleReplayError(false,
                              "Unexpected error deserializing mutation; saved to %s.  " +
                              "This may be caused by replaying a mutation against a table with the same name but incompatible schema.  " +
                              "Exception follows: %s",
                              f.getAbsolutePath(),
                              t);
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("replaying mutation for {}.{}: {}", mutation.getKeyspaceName(), mutation.key(), "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}");

        pendingMutationBytes += size;
        futures.offer(mutationInitiator.initiateMutation(mutation,
                                                         desc.id,
                                                         size,
                                                         entryLocation,
                                                         this));
        //If there are finished mutations, or too many outstanding bytes/mutations
        //drain the futures in the queue
        while (futures.size() > MAX_OUTSTANDING_REPLAY_COUNT
                || pendingMutationBytes > MAX_OUTSTANDING_REPLAY_BYTES
                || (!futures.isEmpty() && futures.peek().isDone()))
        {
            pendingMutationBytes -= FBUtilities.waitOnFuture(futures.poll());
        }
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

    static void handleReplayError(boolean permissible, String message, Object... messageArgs) throws IOException
    {
        String msg = String.format(message, messageArgs);
        IOException e = new CommitLogReplayException(msg);
        if (permissible)
            logger.error("Ignoring commit log replay error likely due to incomplete flush to disk", e);
        else if (Boolean.getBoolean(IGNORE_REPLAY_ERRORS_PROPERTY))
            logger.error("Ignoring commit log replay error", e);
        else if (!CommitLog.handleCommitError("Failed commit log replay", e))
        {
            logger.error("Replay stopped. If you wish to override this error and continue starting the node ignoring " +
                         "commit log replay problems, specify -D" + IGNORE_REPLAY_ERRORS_PROPERTY + "=true " +
                         "on the command line");
            throw e;
        }
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
