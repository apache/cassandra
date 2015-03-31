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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tjake.ICRC32;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ByteBufferDataInput;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CRC32Factory;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

public class CommitLogReplayer
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayer.class);
    private static final int MAX_OUTSTANDING_REPLAY_COUNT = Integer.getInteger("cassandra.commitlog_max_outstanding_replay_count", 1024);
    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;

    private final Set<Keyspace> keyspacesRecovered;
    private final List<Future<?>> futures;
    private final Map<UUID, AtomicInteger> invalidMutations;
    private final AtomicInteger replayedCount;
    private final Map<UUID, ReplayPosition> cfPositions;
    private final ReplayPosition globalPosition;
    private final ICRC32 checksum;
    private byte[] buffer;
    private byte[] uncompressedBuffer;

    CommitLogReplayer(ReplayPosition globalPosition, Map<UUID, ReplayPosition> cfPositions)
    {
        this.keyspacesRecovered = new NonBlockingHashSet<Keyspace>();
        this.futures = new ArrayList<Future<?>>();
        this.buffer = new byte[4096];
        this.uncompressedBuffer = new byte[4096];
        this.invalidMutations = new HashMap<UUID, AtomicInteger>();
        // count the number of replayed mutation. We don't really care about atomicity, but we need it to be a reference.
        this.replayedCount = new AtomicInteger();
        this.checksum = CRC32Factory.instance.create();
        this.cfPositions = cfPositions;
        this.globalPosition = globalPosition;
    }

    public static CommitLogReplayer create()
    {
        // compute per-CF and global replay positions
        Map<UUID, ReplayPosition> cfPositions = new HashMap<UUID, ReplayPosition>();
        Ordering<ReplayPosition> replayPositionOrdering = Ordering.from(ReplayPosition.comparator);
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            // it's important to call RP.gRP per-cf, before aggregating all the positions w/ the Ordering.min call
            // below: gRP will return NONE if there are no flushed sstables, which is important to have in the
            // list (otherwise we'll just start replay from the first flush position that we do have, which is not correct).
            ReplayPosition rp = ReplayPosition.getReplayPosition(cfs.getSSTables());

            // but, if we've truncted the cf in question, then we need to need to start replay after the truncation
            ReplayPosition truncatedAt = SystemKeyspace.getTruncatedPosition(cfs.metadata.cfId);
            if (truncatedAt != null)
                rp = replayPositionOrdering.max(Arrays.asList(rp, truncatedAt));

            cfPositions.put(cfs.metadata.cfId, rp);
        }
        ReplayPosition globalPosition = replayPositionOrdering.min(cfPositions.values());
        logger.debug("Global replay position is {} from columnfamilies {}", globalPosition, FBUtilities.toString(cfPositions));
        return new CommitLogReplayer(globalPosition, cfPositions);
    }

    public void recover(File[] clogs) throws IOException
    {
        for (final File file : clogs)
            recover(file);
    }

    public int blockForWrites()
    {
        for (Map.Entry<UUID, AtomicInteger> entry : invalidMutations.entrySet())
            logger.info(String.format("Skipped %d mutations from unknown (probably removed) CF with id %s", entry.getValue().intValue(), entry.getKey()));

        // wait for all the writes to finish on the mutation stage
        FBUtilities.waitOnFutures(futures);
        logger.debug("Finished waiting on mutations from recovery");

        // flush replayed keyspaces
        futures.clear();
        for (Keyspace keyspace : keyspacesRecovered)
            futures.addAll(keyspace.flush());
        FBUtilities.waitOnFutures(futures);
        return replayedCount.get();
    }

    private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - CommitLogSegment.SYNC_MARKER_SIZE)
        {
            // There was no room in the segment to write a final header. No data could be present here.
            return -1;
        }
        reader.seek(offset);
        ICRC32 crc = CRC32Factory.instance.create();
        crc.updateInt((int) (descriptor.id & 0xFFFFFFFFL));
        crc.updateInt((int) (descriptor.id >>> 32));
        crc.updateInt((int) reader.getPosition());
        int end = reader.readInt();
        long filecrc = reader.readInt() & 0xffffffffL;
        if (crc.getValue() != filecrc)
        {
            if (end != 0 || filecrc != 0)
            {
                logger.warn("Encountered bad header at position {} of commit log {}, with invalid CRC. The end of segment marker should be zero.", offset, reader.getPath());
            }
            return -1;
        }
        else if (end < offset || end > reader.length())
        {
            logger.warn("Encountered bad header at position {} of commit log {}, with bad position but valid CRC", offset, reader.getPath());
            return -1;
        }
        return end;
    }
    
    abstract static class ReplayFilter
    {
        public abstract Iterable<ColumnFamily> filter(Mutation mutation);

        public static ReplayFilter create()
        {
            // If no replaylist is supplied an empty array of strings is used to replay everything.
            if (System.getProperty("cassandra.replayList") == null)
                return new AlwaysReplayFilter();

            Multimap<String, String> toReplay = HashMultimap.create();
            for (String rawPair : System.getProperty("cassandra.replayList").split(","))
            {
                String[] pair = rawPair.trim().split("\\.");
                if (pair.length != 2)
                    throw new IllegalArgumentException("Each table to be replayed must be fully qualified with keyspace name, e.g., 'system.peers'");

                Keyspace ks = Schema.instance.getKeyspaceInstance(pair[0]);
                if (ks == null)
                    throw new IllegalArgumentException("Unknown keyspace " + pair[0]);
                if (ks.getColumnFamilyStore(pair[1]) == null)
                    throw new IllegalArgumentException(String.format("Unknown table %s.%s", pair[0], pair[1]));

                toReplay.put(pair[0], pair[1]);
            }
            return new CustomReplayFilter(toReplay);
        }
    }

    private static class AlwaysReplayFilter extends ReplayFilter
    {
        public Iterable<ColumnFamily> filter(Mutation mutation)
        {
            return mutation.getColumnFamilies();
        }
    }

    private static class CustomReplayFilter extends ReplayFilter
    {
        private Multimap<String, String> toReplay;

        public CustomReplayFilter(Multimap<String, String> toReplay)
        {
            this.toReplay = toReplay;
        }

        public Iterable<ColumnFamily> filter(Mutation mutation)
        {
            final Collection<String> cfNames = toReplay.get(mutation.getKeyspaceName());
            if (cfNames == null)
                return Collections.emptySet();

            return Iterables.filter(mutation.getColumnFamilies(), new Predicate<ColumnFamily>()
            {
                public boolean apply(ColumnFamily cf)
                {
                    return cfNames.contains(cf.metadata().cfName);
                }
            });
        }
    }

    public void recover(File file) throws IOException
    {
        final ReplayFilter replayFilter = ReplayFilter.create();
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());
        RandomAccessReader reader = RandomAccessReader.open(new File(file.getAbsolutePath()));
        try
        {
            if (desc.version < CommitLogDescriptor.VERSION_21)
            {
                if (logAndCheckIfShouldSkip(file, desc))
                    return;
                if (globalPosition.segment == desc.id)
                    reader.seek(globalPosition.position);
                replaySyncSection(reader, -1, desc, replayFilter);
                return;
            }

            final long segmentId = desc.id;
            try
            {
                desc = CommitLogDescriptor.readHeader(reader);
            }
            catch (IOException e)
            {
                desc = null;
            }
            if (desc == null) {
                logger.warn("Could not read commit log descriptor in file {}", file);
                return;
            }
            assert segmentId == desc.id;
            if (logAndCheckIfShouldSkip(file, desc))
                return;

            ICompressor compressor = null;
            if (desc.compression != null)
            {
                try
                {
                    compressor = CompressionParameters.createCompressor(desc.compression);
                }
                catch (ConfigurationException e)
                {
                    logger.warn("Unknown compression: {}", e.getMessage());
                    return;
                }
            }

            assert reader.length() <= Integer.MAX_VALUE;
            int end = (int) reader.getFilePointer();
            int replayEnd = end;

            while ((end = readSyncMarker(desc, end, reader)) >= 0)
            {
                int replayPos = replayEnd + CommitLogSegment.SYNC_MARKER_SIZE;

                if (logger.isDebugEnabled())
                    logger.trace("Replaying {} between {} and {}", file, reader.getFilePointer(), end);
                if (compressor != null)
                {
                    int uncompressedLength = reader.readInt();
                    replayEnd = replayPos + uncompressedLength;
                } else
                {
                    replayEnd = end;
                }

                if (segmentId == globalPosition.segment && replayEnd < globalPosition.position)
                    // Skip over flushed section.
                    continue;

                FileDataInput sectionReader = reader;
                if (compressor != null)
                    try
                    {
                        int start = (int) reader.getFilePointer();
                        int compressedLength = end - start;
                        if (logger.isDebugEnabled())
                            logger.trace("Decompressing {} between replay positions {} and {}",
                                         file,
                                         replayPos,
                                         replayEnd);
                        if (compressedLength > buffer.length)
                            buffer = new byte[(int) (1.2 * compressedLength)];
                        reader.readFully(buffer, 0, compressedLength);
                        int uncompressedLength = replayEnd - replayPos;
                        if (uncompressedLength > uncompressedBuffer.length)
                            uncompressedBuffer = new byte[(int) (1.2 * uncompressedLength)];
                        compressedLength = compressor.uncompress(buffer, 0, compressedLength, uncompressedBuffer, 0);
                        sectionReader = new ByteBufferDataInput(ByteBuffer.wrap(uncompressedBuffer), reader.getPath(), replayPos, 0);
                    }
                    catch (IOException e)
                    {
                        logger.error("Unexpected exception decompressing section {}", e);
                        continue;
                    }

                if (!replaySyncSection(sectionReader, replayEnd, desc, replayFilter))
                    break;
            }
        }
        finally
        {
            FileUtils.closeQuietly(reader);
            logger.info("Finished reading {}", file);
        }
    }

    public boolean logAndCheckIfShouldSkip(File file, CommitLogDescriptor desc)
    {
        logger.info("Replaying {} (CL version {}, messaging version {}, compression {})",
                    file.getPath(),
                    desc.version,
                    desc.getMessagingVersion(),
                    desc.compression);

        if (globalPosition.segment > desc.id)
        {
            logger.debug("skipping replay of fully-flushed {}", file);
            return true;
        }
        return false;
    }

    /**
     * Replays a sync section containing a list of mutations.
     *
     * @return Whether replay should continue with the next section.
     */
    private boolean replaySyncSection(FileDataInput reader, int end, CommitLogDescriptor desc,
            final ReplayFilter replayFilter) throws IOException, FileNotFoundException
    {
         /* read the logs populate Mutation and apply */
        while (reader.getFilePointer() < end && !reader.isEOF())
        {
            if (logger.isDebugEnabled())
                logger.trace("Reading mutation at {}", reader.getFilePointer());

            long claimedCRC32;
            int serializedSize;
            try
            {
                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.debug("Encountered end of segment marker at {}", reader.getFilePointer());
                    return false;
                }

                // Mutation must be at LEAST 10 bytes:
                // 3 each for a non-empty Keyspace and Key (including the
                // 2-byte length from writeUTF/writeWithShortLength) and 4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                    return false;

                long claimedSizeChecksum;
                if (desc.version < CommitLogDescriptor.VERSION_21)
                    claimedSizeChecksum = reader.readLong();
                else
                    claimedSizeChecksum = reader.readInt() & 0xffffffffL;
                checksum.reset();
                if (desc.version < CommitLogDescriptor.VERSION_20)
                    checksum.update(serializedSize);
                else
                    checksum.updateInt(serializedSize);

                if (checksum.getValue() != claimedSizeChecksum)
                    return false;
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
                return false; // last CL entry didn't get completely written. that's ok.
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                // this entry must not have been fsynced. probably the rest is bad too,
                // but just in case there is no harm in trying them (since we still read on an entry boundary)
                continue;
            }
            replayMutation(buffer, serializedSize, reader.getFilePointer(), desc, replayFilter);
        }
        return true;
    }

    /**
     * Deserializes and replays a commit log entry.
     */
    void replayMutation(byte[] inputBuffer, int size,
            final long entryLocation, final CommitLogDescriptor desc, final ReplayFilter replayFilter) throws IOException,
            FileNotFoundException
    {
        FastByteArrayInputStream bufIn = new FastByteArrayInputStream(inputBuffer, 0, size);
        final Mutation mutation;
        try
        {
            mutation = Mutation.serializer.deserialize(new DataInputStream(bufIn),
                                                       desc.getMessagingVersion(),
                                                       ColumnSerializer.Flag.LOCAL);
            // doublecheck that what we read is [still] valid for the current schema
            for (ColumnFamily cf : mutation.getColumnFamilies())
                for (Cell cell : cf)
                    cf.getComparator().validate(cell.name());
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
            DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
            try
            {
                out.write(inputBuffer, 0, size);
            }
            finally
            {
                out.close();
            }
            String st = String.format("Unexpected error deserializing mutation; saved to %s and ignored.  This may be caused by replaying a mutation against a table with the same name but incompatible schema.  Exception follows: ",
                                      f.getAbsolutePath());
            logger.error(st, t);
            return;
        }

        if (logger.isDebugEnabled())
            logger.debug("replaying mutation for {}.{}: {}", mutation.getKeyspaceName(), ByteBufferUtil.bytesToHex(mutation.key()), "{" + StringUtils.join(mutation.getColumnFamilies().iterator(), ", ") + "}");

        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                if (Schema.instance.getKSMetaData(mutation.getKeyspaceName()) == null)
                    return;
                if (pointInTimeExceeded(mutation))
                    return;

                final Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());

                // Rebuild the mutation, omitting column families that
                //    a) the user has requested that we ignore,
                //    b) have already been flushed,
                // or c) are part of a cf that was dropped.
                // Keep in mind that the cf.name() is suspect. do every thing based on the cfid instead.
                Mutation newMutation = null;
                for (ColumnFamily columnFamily : replayFilter.filter(mutation))
                {
                    if (Schema.instance.getCF(columnFamily.id()) == null)
                        continue; // dropped

                    ReplayPosition rp = cfPositions.get(columnFamily.id());

                    // replay if current segment is newer than last flushed one or,
                    // if it is the last known segment, if we are after the replay position
                    if (desc.id > rp.segment || (desc.id == rp.segment && entryLocation > rp.position))
                    {
                        if (newMutation == null)
                            newMutation = new Mutation(mutation.getKeyspaceName(), mutation.key());
                        newMutation.add(columnFamily);
                        replayedCount.incrementAndGet();
                    }
                }
                if (newMutation != null)
                {
                    assert !newMutation.isEmpty();
                    Keyspace.open(newMutation.getKeyspaceName()).apply(newMutation, false);
                    keyspacesRecovered.add(keyspace);
                }
            }
        };
        futures.add(StageManager.getStage(Stage.MUTATION).submit(runnable));
        if (futures.size() > MAX_OUTSTANDING_REPLAY_COUNT)
        {
            FBUtilities.waitOnFutures(futures);
            futures.clear();
        }
    }

    protected boolean pointInTimeExceeded(Mutation fm)
    {
        long restoreTarget = CommitLog.instance.archiver.restorePointInTime;

        for (ColumnFamily families : fm.getColumnFamilies())
        {
            if (CommitLog.instance.archiver.precision.toMillis(families.maxTimestamp()) > restoreTarget)
                return true;
        }
        return false;
    }
}
