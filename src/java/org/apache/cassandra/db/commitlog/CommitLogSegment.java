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

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.zip.Checksum;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.PureJavaCrc32;

/*
 * A single commit log file on disk. Manages creation of the file and writing row mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public class CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegment.class);

    // The commit log entry overhead in bytes (int: length + long: head checksum + long: tail checksum)
    static final int ENTRY_OVERHEAD_SIZE = 4 + 8 + 8;

    // cache which cf is dirty in this segment to avoid having to lookup all ReplayPositions to decide if we can delete this segment
    private final HashMap<UUID, Integer> cfLastWrite = new HashMap<UUID, Integer>();

    public final long id;

    private final File logFile;
    private final RandomAccessFile logFileAccessor;

    private boolean needsSync = false;

    private final MappedByteBuffer buffer;
    private boolean closed;

    public final CommitLogDescriptor descriptor;

    /**
     * @return a newly minted segment file
     */
    public static CommitLogSegment freshSegment()
    {
        return new CommitLogSegment(null);
    }

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    CommitLogSegment(String filePath)
    {
        id = System.nanoTime();
        descriptor = new CommitLogDescriptor(id);
        logFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
        boolean isCreating = true;

        try
        {
            if (filePath != null)
            {
                File oldFile = new File(filePath);

                if (oldFile.exists())
                {
                    logger.debug("Re-using discarded CommitLog segment for {} from {}", id, filePath);
                    if (!oldFile.renameTo(logFile))
                        throw new IOException("Rename from " + filePath + " to " + id + " failed");
                    isCreating = false;
                }
            }

            // Open the initial the segment file
            logFileAccessor = new RandomAccessFile(logFile, "rw");

            if (isCreating)
                logger.debug("Creating new commit log segment {}", logFile.getPath());

            // Map the segment, extending or truncating it to the standard segment size
            logFileAccessor.setLength(DatabaseDescriptor.getCommitLogSegmentSize());

            buffer = logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
            buffer.putInt(CommitLog.END_OF_SEGMENT_MARKER);
            buffer.position(0);

            needsSync = true;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Completely discards a segment file by deleting it. (Potentially blocking operation)
     */
    public void discard(boolean deleteFile)
    {
        // TODO shouldn't we close the file when we're done writing to it, which comes (potentially) much earlier than it's eligible for recyling?
        close();
        try
        {
            if (deleteFile)
                FileUtils.deleteWithConfirm(logFile);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    public CommitLogSegment recycle()
    {
        // writes an end-of-segment marker at the very beginning of the file and closes it
        buffer.position(0);
        buffer.putInt(CommitLog.END_OF_SEGMENT_MARKER);
        buffer.position(0);

        try
        {
            sync();
        }
        catch (IOException e)
        {
            // This is a best effort thing anyway
            logger.warn("I/O error flushing " + this + " " + e);
        }

        close();

        return new CommitLogSegment(getPath());
    }

    /**
     * @return true if there is room to write() @param mutation to this segment
     */
    public boolean hasCapacityFor(RowMutation mutation)
    {
        long totalSize = RowMutation.serializer.serializedSize(mutation, MessagingService.current_version) + ENTRY_OVERHEAD_SIZE;
        return totalSize <= buffer.remaining();
    }

    /**
     * mark all of the column families we're modifying as dirty at this position
     */
    private void markDirty(RowMutation rowMutation, ReplayPosition repPos)
    {
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            // check for null cfm in case a cl write goes through after the cf is
            // defined but before a new segment is created.
            CFMetaData cfm = Schema.instance.getCFMetaData(columnFamily.id());
            if (cfm == null)
            {
                logger.error("Attempted to write commit log entry for unrecognized column family: " + columnFamily.id());
            }
            else
            {
                markCFDirty(cfm.cfId, repPos.position);
            }
        }
    }

   /**
     * Appends a row mutation onto the commit log.  Requres that hasCapacityFor has already been checked.
     *
     * @param   rowMutation   the mutation to append to the commit log.
     * @return  the position of the appended mutation
     */
    public ReplayPosition write(RowMutation rowMutation) throws IOException
    {
        assert !closed;
        ReplayPosition repPos = getContext();
        markDirty(rowMutation, repPos);

        Checksum checksum = new PureJavaCrc32();
        byte[] serializedRow = rowMutation.getSerializedBuffer(MessagingService.current_version);

        checksum.update(serializedRow.length);
        buffer.putInt(serializedRow.length);
        buffer.putLong(checksum.getValue());

        buffer.put(serializedRow);
        checksum.update(serializedRow, 0, serializedRow.length);
        buffer.putLong(checksum.getValue());

        if (buffer.remaining() >= 4)
        {
            // writes end of segment marker and rewinds back to position where it starts
            buffer.putInt(CommitLog.END_OF_SEGMENT_MARKER);
            buffer.position(buffer.position() - CommitLog.END_OF_SEGMENT_MARKER_SIZE);
        }

        needsSync = true;
        return repPos;
    }

    /**
     * Forces a disk flush for this segment file.
     */
    public void sync() throws IOException
    {
        if (needsSync)
        {
            buffer.force();
            needsSync = false;
        }
    }

    /**
     * @return the current ReplayPosition for this log segment
     */
    public ReplayPosition getContext()
    {
        return new ReplayPosition(id, buffer.position());
    }

    /**
     * @return the file path to this segment
     */
    public String getPath()
    {
        return logFile.getPath();
    }

    /**
     * @return the file name of this segment
     */
    public String getName()
    {
        return logFile.getName();
    }

    /**
     * Close the segment file.
     */
    public void close()
    {
        if (closed)
            return;

        try
        {
            logFileAccessor.close();
            closed = true;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Records the CF as dirty at a certain position.
     *
     * @param cfId      the column family ID that is now dirty
     * @param position  the position the last write for this CF was written at
     */
    private void markCFDirty(UUID cfId, Integer position)
    {
        cfLastWrite.put(cfId, position);
    }

    /**
     * Marks the ColumnFamily specified by cfId as clean for this log segment. If the
     * given context argument is contained in this file, it will only mark the CF as
     * clean if no newer writes have taken place.
     *
     * @param cfId    the column family ID that is now clean
     * @param context the optional clean offset
     */
    public void markClean(UUID cfId, ReplayPosition context)
    {
        Integer lastWritten = cfLastWrite.get(cfId);

        if (lastWritten != null && (!contains(context) || lastWritten < context.position))
        {
            cfLastWrite.remove(cfId);
        }
    }

    /**
     * @return a collection of dirty CFIDs for this segment file.
     */
    public Collection<UUID> getDirtyCFIDs()
    {
        return cfLastWrite.keySet();
    }

    /**
     * @return true if this segment is unused and safe to recycle or delete
     */
    public boolean isUnused()
    {
        return cfLastWrite.isEmpty();
    }

    /**
     * Check to see if a certain ReplayPosition is contained by this segment file.
     *
     * @param   context the replay position to be checked
     * @return  true if the replay position is contained by this segment file.
     */
    public boolean contains(ReplayPosition context)
    {
        return context.segment == id;
    }

    // For debugging, not fast
    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (UUID cfId : cfLastWrite.keySet())
        {
            CFMetaData m = Schema.instance.getCFMetaData(cfId);
            sb.append(m == null ? "<deleted>" : m.cfName).append(" (").append(cfId).append("), ");
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + getPath() + ')';
    }

    public int position()
    {
        return buffer.position();
    }
}
