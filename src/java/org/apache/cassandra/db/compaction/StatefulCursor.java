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

package org.apache.cassandra.db.compaction;

import java.util.Collection;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.UnfilteredValidation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.UnfilteredDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;

import static org.apache.cassandra.db.rows.Cell.INVALID_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_TTL;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

class StatefulCursor extends SSTableCursorReader
{
    private final Config.CorruptedTombstoneStrategy corruptedTombstoneStrategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    private final boolean corruptedTombstoneValidationEnabled = corruptedTombstoneStrategy != Config.CorruptedTombstoneStrategy.disabled;

    private PartitionDescriptor currPartition;
    /**
     * Holds the previous partition's header. The read side compares it for key order. The write
     * side takes it as the last written partition.
     */
    private PartitionDescriptor prevPartition;

    /** @see #partitionSwaps() */
    private long partitionSwaps = 0;

    /** Non-final: it is exchanged by {@link #detachUnfiltered}. */
    private UnfilteredDescriptor unfiltered;

    private boolean resetAfterDone = false;
    private long bytesReadPositionSnapshot = 0;

    private boolean isOpenRangeTombstonePresent = false;

    public StatefulCursor(SSTableReader reader, DiskAccessMode diskAccessMode)
    {
        this(reader, null, diskAccessMode);
    }

    /** @param bounds the segments to read, or null for the whole sstable; see {@link SSTableCursorReader#SSTableCursorReader(SSTableReader, Collection, DiskAccessMode)} */
    public StatefulCursor(SSTableReader reader, Collection<PartitionPositionBounds> bounds, DiskAccessMode diskAccessMode)
    {
        super(reader, bounds, diskAccessMode);
        // A deletion-only complex column must reach the merge as a position of its own, so that
        // its column-level deletion reaches the output. pauseAtEmptyComplexColumns defaults to
        // true on SSTableCursorReader for exactly this reason; no override needed here.
        currPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        prevPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        unfiltered = new UnfilteredDescriptor(reader.header.clusteringTypes().toArray(AbstractType[]::new));
    }

    public int readPartitionHeader()
    {
        // A range never spans a partition, so one left open belongs to the partition that ended.
        // Reporting it here names that partition; carrying the flag forward would blame the next
        // partition's first start bound instead, and would hide an unmatched close in it.
        // currPartition still holds the partition that just ended; the swap below moves it to prev.
        if (isOpenRangeTombstonePresent)
            corruptSSTable("Partition ended with an open range tombstone marker: " + currPartition.key());
        isOpenRangeTombstonePresent = false;

        swapCurrAndPrevPartition();
        int state = readPartitionHeader(currPartition);

        if (prevPartition.keyLength() != 0 && prevPartition.key().compareTo(currPartition.key()) >= 0)
            corruptSSTableKeysOOO();
        if (corruptedTombstoneValidationEnabled)
            validateInvalidPartitionDeletion();
        return state;
    }

    /**
     * Hands the write side the instance holding the previous partition's header and takes
     * {@code floater} in its place. This exchange is what these comments call a steal: a cursor
     * overwrites its two descriptors as it reads, so the write side takes the object rather than
     * its contents and hands back a spare, the floater.
     *
     * It detaches {@code prev} rather than {@code curr} because {@code curr} is the next read's
     * out-of-order comparison source.
     */
    PartitionDescriptor detachPrevPartition(PartitionDescriptor floater)
    {
        assert floater != prevPartition && floater != currPartition
             : "the floater is already one of this cursor's slots; a steal was not handed back";
        PartitionDescriptor detached = prevPartition;
        prevPartition = floater;
        return detached;
    }

    /**
     * Counts the exchanges of the curr and prev partition slots, so the write side can assert that
     * the slot it recorded still holds its partition. It counts swaps rather than reads because
     * {@link #resetAfterDone} advances the slots too.
     */
    long partitionSwaps()
    {
        return partitionSwaps;
    }

    /**
     * Hands the write side the instance holding the unfiltered it has just written out, and takes
     * {@code floater} in its place. See {@link #detachPrevPartition}.
     *
     * The steal cannot wait a round, and need not: the next touch of the slot is another load. A
     * cursor stolen from in any state but {@code UNFILTERED_END} mis-orders the merge silently.
     */
    UnfilteredDescriptor detachUnfiltered(UnfilteredDescriptor floater)
    {
        assert floater != unfiltered
             : "the floater is already this cursor's slot; a steal was not handed back";
        assert state() == UNFILTERED_END
             : "an unfiltered was stolen from a cursor that has not finished it: " + this;
        UnfilteredDescriptor detached = unfiltered;
        unfiltered = floater;
        return detached;
    }

    private int corruptSSTableKeysOOO()
    {
        return corruptSSTable("Keys out of order. Current key: " + keyToString(currentKey()) + " <= "  + keyToString(prevKey()));
    }

    private void swapCurrAndPrevPartition()
    {
        partitionSwaps++;
        PartitionDescriptor temp = currPartition;
        currPartition = prevPartition;
        prevPartition = temp;
    }

    public int skipUnfiltered()
    {
        if (isState(state(), CELL_HEADER_START | CELL_VALUE_START | CELL_END))
            return super.skipRowCells(unfiltered().dataStart(), unfiltered().size(), false);

        return super.skipUnfiltered(false);
    }

    public int skipStaticRow()
    {
        if (isState(state(), CELL_HEADER_START | CELL_VALUE_START | CELL_END))
            return super.skipRowCells(unfiltered().dataStart(), unfiltered().size(), false);

        return super.skipStaticRow(false);
    }

    /** @see SSTableCursorReader#rewindRowCells(UnfilteredDescriptor, boolean) */
    public int rewindRowCells(boolean isStatic)
    {
        return super.rewindRowCells(unfiltered, isStatic);
    }

    @Override
    public String toString()
    {
        return "StatefulCursor{" +
               "pHeader=" + currPartition() +
               ", rHeader=" + unfiltered() +
               ", state=" + state() +
               '}';
    }

    /**
     * @return true if this call reset the cursor, false if an earlier call already did
     */
    public boolean resetAfterDone()
    {
        if (resetAfterDone)
            return false;
        resetAfterDone = true;
        swapCurrAndPrevPartition();
        // Reset curr only. The prev slot must keep the last written partition.
        currPartition().resetPartition();
        unfiltered().resetUnfiltered();
        return true;
    }

    DecoratedKey currentKey()
    {
        return currPartition.key();
    }

    DecoratedKey prevKey()
    {
        return prevPartition.key();
    }

    public PartitionDescriptor currPartition()
    {
        return currPartition;
    }

    public UnfilteredDescriptor unfiltered()
    {
        return unfiltered;
    }

    public long bytesReadSinceSnapshot()
    {
        long latestByteReadPosition = bytesRead();
        long cursorBytesRead = latestByteReadPosition - bytesReadPositionSnapshot;
        bytesReadPositionSnapshot = latestByteReadPosition;
        return cursorBytesRead;
    }

    private String keyToString(DecoratedKey key)
    {
        String keyString;
        try
        {
            keyString = ssTableReader().metadata().partitionKeyType.getString(key.getKey());
        }
        catch (Throwable t)
        {
            keyString = "[corrupt token="+key.getToken()+"]";
        }
        return keyString;
    }

    public void readRowHeader()
    {
        super.readRowHeader(unfiltered);
        if (corruptedTombstoneValidationEnabled)
            validateInvalidRowDeletion();
    }

    public void readTombstoneMarker()
    {
        super.readTombstoneMarker(unfiltered);

        if (corruptedTombstoneValidationEnabled)
            validateInvalidTombstoneDeletion();

        boolean isStartBound = unfiltered.isStartBound();
        if (isOpenRangeTombstonePresent && isStartBound)
            corruptSSTable("Encountered an open range tombstone marker before the prev was closed: " + unfiltered);
        if (!isOpenRangeTombstonePresent && !isStartBound)
            corruptSSTable("Encountered an close/boundary range tombstone marker before an open one: " + unfiltered);
        isOpenRangeTombstonePresent = isStartBound || unfiltered.isBoundary();
        // TODO: can also add verification of open/close timestamp match
    }

    public void readStaticRowHeader()
    {
        super.readStaticRowHeader(unfiltered);
        if (corruptedTombstoneValidationEnabled)
            validateInvalidRowDeletion();
    }

    @Override
    public int readCellHeader()
    {
        int state = super.readCellHeader();
        // cellLiveness is valid only where a cell was produced; elsewhere it still holds an
        // earlier cell's values, possibly from an earlier row. The state test excludes
        // UNFILTERED_END, which the dropped-column filter can reach with no cell. producedCell
        // excludes a deletion-only complex column, which returns CELL_END with no cell fields.
        if (corruptedTombstoneValidationEnabled && isState(state, CELL_VALUE_START | CELL_END) && cellCursor().producedCell)
            validateInvalidCellDeletion();
        return state;
    }

    /**
     * Reports a corrupt value. The table, the key and the reader are always this cursor's own, so
     * a caller supplies only the message.
     */
    private void reportInvalid(String message)
    {
        UnfilteredValidation.handleInvalid(ssTableReader().metadata(), currPartition.key(), ssTableReader(), message);
    }

    /** The row path and the tombstone path both start with this check. */
    private void validateRowDeletionTime()
    {
        if (!unfiltered.deletionTime().validate())
            reportInvalid("rowDeletion="+unfiltered.deletionTime().toString());
    }

    private void validateInvalidTombstoneDeletion()
    {
        validateRowDeletionTime();
        if (unfiltered.isBoundary() && !unfiltered.deletionTime2().validate())
            reportInvalid("rowDeletion2="+unfiltered.deletionTime2().toString());
    }

    private void validateInvalidCellDeletion()
    {
        ReusableCellLivenessInfo cellLiveness = cellCursor().cellLiveness;
        if (hasInvalidCellDeletion(cellLiveness.ttl(), cellLiveness.localDeletionTime()))
            reportInvalid("cellLiveness="+cellLiveness);
    }

    /**
     * Mirrors {@link org.apache.cassandra.db.rows.AbstractCell#hasInvalidDeletions()}, where
     * {@code ttl != NO_TTL} is the reference's {@code isExpiring()}.
     */
    @VisibleForTesting
    static boolean hasInvalidCellDeletion(int ttl, long localExpirationTime)
    {
        return ttl < 0
               || localExpirationTime == INVALID_DELETION_TIME
               || localExpirationTime < 0
               || (ttl != NO_TTL && localExpirationTime == NO_DELETION_TIME);
    }

    /** Mirrors the primary-key liveness clause of {@link org.apache.cassandra.db.rows.AbstractRow#hasInvalidDeletions()}. */
    @VisibleForTesting
    static boolean hasInvalidRowLiveness(int ttl, long localExpirationTime)
    {
        return ttl != NO_TTL && (ttl < 0 || localExpirationTime < 0);
    }

    private void validateInvalidRowDeletion()
    {
        validateRowDeletionTime();
        ReusableLivenessInfo livenessInfo = unfiltered.livenessInfo();
        if (hasInvalidRowLiveness(livenessInfo.ttl(), livenessInfo.localExpirationTime()))
            reportInvalid("rowLiveness="+livenessInfo.toString());
    }

    private void validateInvalidPartitionDeletion()
    {
        if (!currPartition.deletionTime().validate())
            reportInvalid("partitionLevelDeletion="+currPartition.deletionTime().toString());
    }
}
