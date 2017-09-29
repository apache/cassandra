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
package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadErrorReason;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler.CommitLogReadException;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

public class CommitLogReader
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReader.class);

    private static final int LEGACY_END_OF_SEGMENT_MARKER = 0;

    @VisibleForTesting
    public static final int ALL_MUTATIONS = -1;
    private final CRC32 checksum;
    private final Map<UUID, AtomicInteger> invalidMutations;

    private byte[] buffer;

    public CommitLogReader()
    {
        checksum = new CRC32();
        invalidMutations = new HashMap<>();
        buffer = new byte[4096];
    }

    public Set<Map.Entry<UUID, AtomicInteger>> getInvalidMutations()
    {
        return invalidMutations.entrySet();
    }

    /**
     * Reads all passed in files with no minimum, no start, and no mutation limit.
     */
    public void readAllFiles(CommitLogReadHandler handler, File[] files) throws IOException
    {
        readAllFiles(handler, files, CommitLogPosition.NONE);
    }

    private static boolean shouldSkip(File file) throws IOException, ConfigurationException
    {
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());
        if (desc.version < CommitLogDescriptor.VERSION_21)
        {
            return false;
        }
        try(RandomAccessReader reader = RandomAccessReader.open(file))
        {
            CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
            int end = reader.readInt();
            long filecrc = reader.readInt() & 0xffffffffL;
            return end == 0 && filecrc == 0;
        }
    }

    private static List<File> filterCommitLogFiles(File[] toFilter)
    {
        List<File> filtered = new ArrayList<>(toFilter.length);
        for (File file: toFilter)
        {
            try
            {
                if (shouldSkip(file))
                {
                    logger.info("Skipping playback of empty log: {}", file.getName());
                }
                else
                {
                    filtered.add(file);
                }
            }
            catch (Exception e)
            {
                // let recover deal with it
                filtered.add(file);
            }
        }

        return filtered;
    }

    /**
     * Reads all passed in files with minPosition, no start, and no mutation limit.
     */
    public void readAllFiles(CommitLogReadHandler handler, File[] files, CommitLogPosition minPosition) throws IOException
    {
        List<File> filteredLogs = filterCommitLogFiles(files);
        int i = 0;
        for (File file: filteredLogs)
        {
            i++;
            readCommitLogSegment(handler, file, minPosition, ALL_MUTATIONS, i == filteredLogs.size());
        }
    }

    /**
     * Reads passed in file fully
     */
    public void readCommitLogSegment(CommitLogReadHandler handler, File file, boolean tolerateTruncation) throws IOException
    {
        readCommitLogSegment(handler, file, CommitLogPosition.NONE, ALL_MUTATIONS, tolerateTruncation);
    }

    /**
     * Reads passed in file fully, up to mutationLimit count
     */
    @VisibleForTesting
    public void readCommitLogSegment(CommitLogReadHandler handler, File file, int mutationLimit, boolean tolerateTruncation) throws IOException
    {
        readCommitLogSegment(handler, file, CommitLogPosition.NONE, mutationLimit, tolerateTruncation);
    }

    /**
     * Reads mutations from file, handing them off to handler
     * @param handler Handler that will take action based on deserialized Mutations
     * @param file CommitLogSegment file to read
     * @param minPosition Optional minimum CommitLogPosition - all segments with id > or matching w/greater position will be read
     * @param mutationLimit Optional limit on # of mutations to replay. Local ALL_MUTATIONS serves as marker to play all.
     * @param tolerateTruncation Whether or not we should allow truncation of this file or throw if EOF found
     *
     * @throws IOException
     */
    public void readCommitLogSegment(CommitLogReadHandler handler,
                                     File file,
                                     CommitLogPosition minPosition,
                                     int mutationLimit,
                                     boolean tolerateTruncation) throws IOException
    {
        // just transform from the file name (no reading of headers) to determine version
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());

        try(RandomAccessReader reader = RandomAccessReader.open(file))
        {
            if (desc.version < CommitLogDescriptor.VERSION_21)
            {
                if (!shouldSkipSegmentId(file, desc, minPosition))
                {
                    if (minPosition.segmentId == desc.id)
                        reader.seek(minPosition.position);
                    ReadStatusTracker statusTracker = new ReadStatusTracker(mutationLimit, tolerateTruncation);
                    statusTracker.errorContext = desc.fileName();
                    readSection(handler, reader, minPosition, (int) reader.length(), statusTracker, desc);
                }
                return;
            }

            final long segmentIdFromFilename = desc.id;
            try
            {
                // The following call can either throw or legitimately return null. For either case, we need to check
                // desc outside this block and set it to null in the exception case.
                desc = CommitLogDescriptor.readHeader(reader, DatabaseDescriptor.getEncryptionContext());
            }
            catch (Exception e)
            {
                desc = null;
            }
            if (desc == null)
            {
                // don't care about whether or not the handler thinks we can continue. We can't w/out descriptor.
                // whether or not we continue with startup will depend on whether this is the last segment
                handler.handleUnrecoverableError(new CommitLogReadException(
                    String.format("Could not read commit log descriptor in file %s", file),
                    CommitLogReadErrorReason.UNRECOVERABLE_DESCRIPTOR_ERROR,
                    tolerateTruncation));
                return;
            }

            if (segmentIdFromFilename != desc.id)
            {
                if (handler.shouldSkipSegmentOnError(new CommitLogReadException(String.format(
                    "Segment id mismatch (filename %d, descriptor %d) in file %s", segmentIdFromFilename, desc.id, file),
                                                                                CommitLogReadErrorReason.RECOVERABLE_DESCRIPTOR_ERROR,
                                                                                false)))
                {
                    return;
                }
            }

            if (shouldSkipSegmentId(file, desc, minPosition))
                return;

            CommitLogSegmentReader segmentReader;
            try
            {
                segmentReader = new CommitLogSegmentReader(handler, desc, reader, tolerateTruncation);
            }
            catch(Exception e)
            {
                handler.handleUnrecoverableError(new CommitLogReadException(
                    String.format("Unable to create segment reader for commit log file: %s", e),
                    CommitLogReadErrorReason.UNRECOVERABLE_UNKNOWN_ERROR,
                    tolerateTruncation));
                return;
            }

            try
            {
                ReadStatusTracker statusTracker = new ReadStatusTracker(mutationLimit, tolerateTruncation);
                for (CommitLogSegmentReader.SyncSegment syncSegment : segmentReader)
                {
                    // Only tolerate truncation if we allow in both global and segment
                    statusTracker.tolerateErrorsInSection = tolerateTruncation & syncSegment.toleratesErrorsInSection;

                    // Skip segments that are completely behind the desired minPosition
                    if (desc.id == minPosition.segmentId && syncSegment.endPosition < minPosition.position)
                        continue;

                    statusTracker.errorContext = String.format("Next section at %d in %s", syncSegment.fileStartPosition, desc.fileName());

                    readSection(handler, syncSegment.input, minPosition, syncSegment.endPosition, statusTracker, desc);
                    if (!statusTracker.shouldContinue())
                        break;
                }
            }
            // Unfortunately AbstractIterator cannot throw a checked exception, so we check to see if a RuntimeException
            // is wrapping an IOException.
            catch (RuntimeException re)
            {
                if (re.getCause() instanceof IOException)
                    throw (IOException) re.getCause();
                throw re;
            }
            logger.debug("Finished reading {}", file);
        }
    }

    /**
     * Any segment with id >= minPosition.segmentId is a candidate for read.
     */
    private boolean shouldSkipSegmentId(File file, CommitLogDescriptor desc, CommitLogPosition minPosition)
    {
        logger.debug("Reading {} (CL version {}, messaging version {}, compression {})",
            file.getPath(),
            desc.version,
            desc.getMessagingVersion(),
            desc.compression);

        if (minPosition.segmentId > desc.id)
        {
            logger.trace("Skipping read of fully-flushed {}", file);
            return true;
        }
        return false;
    }

    /**
     * Reads a section of a file containing mutations
     *
     * @param handler Handler that will take action based on deserialized Mutations
     * @param reader FileDataInput / logical buffer containing commitlog mutations
     * @param minPosition CommitLogPosition indicating when we should start actively replaying mutations
     * @param end logical numeric end of the segment being read
     * @param statusTracker ReadStatusTracker with current state of mutation count, error state, etc
     * @param desc Descriptor for CommitLog serialization
     */
    private void readSection(CommitLogReadHandler handler,
                             FileDataInput reader,
                             CommitLogPosition minPosition,
                             int end,
                             ReadStatusTracker statusTracker,
                             CommitLogDescriptor desc) throws IOException
    {
        // seek rather than deserializing mutation-by-mutation to reach the desired minPosition in this SyncSegment
        if (desc.id == minPosition.segmentId && reader.getFilePointer() < minPosition.position)
            reader.seek(minPosition.position);

        while (statusTracker.shouldContinue() && reader.getFilePointer() < end && !reader.isEOF())
        {
            long mutationStart = reader.getFilePointer();
            if (logger.isTraceEnabled())
                logger.trace("Reading mutation at {}", mutationStart);

            long claimedCRC32;
            int serializedSize;
            try
            {
                // We rely on reading serialized size == 0 (LEGACY_END_OF_SEGMENT_MARKER) to identify the end
                // of a segment, which happens naturally due to the 0 padding of the empty segment on creation.
                // However, it's possible with 2.1 era commitlogs that the last mutation ended less than 4 bytes
                // from the end of the file, which means that we'll be unable to read an a full int and instead
                // read an EOF here
                if(end - reader.getFilePointer() < 4)
                {
                    logger.trace("Not enough bytes left for another mutation in this CommitLog segment, continuing");
                    statusTracker.requestTermination();
                    return;
                }

                // any of the reads may hit EOF
                serializedSize = reader.readInt();
                if (serializedSize == LEGACY_END_OF_SEGMENT_MARKER)
                {
                    logger.trace("Encountered end of segment marker at {}", reader.getFilePointer());
                    statusTracker.requestTermination();
                    return;
                }

                // Mutation must be at LEAST 10 bytes:
                //    3 for a non-empty Keyspace
                //    3 for a Key (including the 2-byte length from writeUTF/writeWithShortLength)
                //    4 bytes for column count.
                // This prevents CRC by being fooled by special-case garbage in the file; see CASSANDRA-2128
                if (serializedSize < 10)
                {
                    if (handler.shouldSkipSegmentOnError(new CommitLogReadException(
                                                    String.format("Invalid mutation size %d at %d in %s", serializedSize, mutationStart, statusTracker.errorContext),
                                                    CommitLogReadErrorReason.MUTATION_ERROR,
                                                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }
                    return;
                }

                long claimedSizeChecksum = CommitLogFormat.calculateClaimedChecksum(reader, desc.version);
                checksum.reset();
                CommitLogFormat.updateChecksum(checksum, serializedSize, desc.version);

                if (checksum.getValue() != claimedSizeChecksum)
                {
                    if (handler.shouldSkipSegmentOnError(new CommitLogReadException(
                                                    String.format("Mutation size checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                                                    CommitLogReadErrorReason.MUTATION_ERROR,
                                                    statusTracker.tolerateErrorsInSection)))
                    {
                        statusTracker.requestTermination();
                    }
                    return;
                }

                if (serializedSize > buffer.length)
                    buffer = new byte[(int) (1.2 * serializedSize)];
                reader.readFully(buffer, 0, serializedSize);

                claimedCRC32 = CommitLogFormat.calculateClaimedCRC32(reader, desc.version);
            }
            catch (EOFException eof)
            {
                if (handler.shouldSkipSegmentOnError(new CommitLogReadException(
                                                String.format("Unexpected end of segment at %d in %s", mutationStart, statusTracker.errorContext),
                                                CommitLogReadErrorReason.EOF,
                                                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }
                return;
            }

            checksum.update(buffer, 0, serializedSize);
            if (claimedCRC32 != checksum.getValue())
            {
                if (handler.shouldSkipSegmentOnError(new CommitLogReadException(
                                                String.format("Mutation checksum failure at %d in %s", mutationStart, statusTracker.errorContext),
                                                CommitLogReadErrorReason.MUTATION_ERROR,
                                                statusTracker.tolerateErrorsInSection)))
                {
                    statusTracker.requestTermination();
                }
                continue;
            }

            long mutationPosition = reader.getFilePointer();
            readMutation(handler, buffer, serializedSize, minPosition, (int)mutationPosition, desc);

            // Only count this as a processed mutation if it is after our min as we suppress reading of mutations that
            // are before this mark.
            if (mutationPosition >= minPosition.position)
                statusTracker.addProcessedMutation();
        }
    }

    /**
     * Deserializes and passes a Mutation to the ICommitLogReadHandler requested
     *
     * @param handler Handler that will take action based on deserialized Mutations
     * @param inputBuffer raw byte array w/Mutation data
     * @param size deserialized size of mutation
     * @param minPosition We need to suppress replay of mutations that are before the required minPosition
     * @param entryLocation filePointer offset of mutation within CommitLogSegment
     * @param desc CommitLogDescriptor being worked on
     */
    @VisibleForTesting
    protected void readMutation(CommitLogReadHandler handler,
                                byte[] inputBuffer,
                                int size,
                                CommitLogPosition minPosition,
                                final int entryLocation,
                                final CommitLogDescriptor desc) throws IOException
    {
        // For now, we need to go through the motions of deserializing the mutation to determine its size and move
        // the file pointer forward accordingly, even if we're behind the requested minPosition within this SyncSegment.
        boolean shouldReplay = entryLocation > minPosition.position;

        final Mutation mutation;
        try (RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size))
        {
            mutation = Mutation.serializer.deserialize(bufIn,
                                                       desc.getMessagingVersion(),
                                                       SerializationHelper.Flag.LOCAL);
            // doublecheck that what we read is still] valid for the current schema
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
            handler.handleUnrecoverableError(new CommitLogReadException(
                String.format(
                    "Unexpected error deserializing mutation; saved to %s.  " +
                    "This may be caused by replaying a mutation against a table with the same name but incompatible schema.  " +
                    "Exception follows: %s", f.getAbsolutePath(), t),
                CommitLogReadErrorReason.MUTATION_ERROR,
                false));
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("Read mutation for {}.{}: {}", mutation.getKeyspaceName(), mutation.key(),
                         "{" + StringUtils.join(mutation.getPartitionUpdates().iterator(), ", ") + "}");

        if (shouldReplay)
            handler.handleMutation(mutation, size, entryLocation, desc);
    }

    /**
     * Helper methods to deal with changing formats of internals of the CommitLog without polluting deserialization code.
     */
    private static class CommitLogFormat
    {
        public static long calculateClaimedChecksum(FileDataInput input, int commitLogVersion) throws IOException
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                case CommitLogDescriptor.VERSION_20:
                    return input.readLong();
                // Changed format in 2.1
                default:
                    return input.readInt() & 0xffffffffL;
            }
        }

        public static void updateChecksum(CRC32 checksum, int serializedSize, int commitLogVersion)
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                    checksum.update(serializedSize);
                    break;
                // Changed format in 2.0
                default:
                    updateChecksumInt(checksum, serializedSize);
                    break;
            }
        }

        public static long calculateClaimedCRC32(FileDataInput input, int commitLogVersion) throws IOException
        {
            switch (commitLogVersion)
            {
                case CommitLogDescriptor.VERSION_12:
                case CommitLogDescriptor.VERSION_20:
                    return input.readLong();
                // Changed format in 2.1
                default:
                    return input.readInt() & 0xffffffffL;
            }
        }
    }

    private static class ReadStatusTracker
    {
        private int mutationsLeft;
        public String errorContext = "";
        public boolean tolerateErrorsInSection;
        private boolean error;

        public ReadStatusTracker(int mutationLimit, boolean tolerateErrorsInSection)
        {
            this.mutationsLeft = mutationLimit;
            this.tolerateErrorsInSection = tolerateErrorsInSection;
        }

        public void addProcessedMutation()
        {
            if (mutationsLeft == ALL_MUTATIONS)
                return;
            --mutationsLeft;
        }

        public boolean shouldContinue()
        {
            return !error && (mutationsLeft != 0 || mutationsLeft == ALL_MUTATIONS);
        }

        public void requestTermination()
        {
            error = true;
        }
    }
}
