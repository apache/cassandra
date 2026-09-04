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

import static org.apache.cassandra.db.rows.Cell.INVALID_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_DELETION_TIME;
import static org.apache.cassandra.db.rows.Cell.NO_TTL;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_HEADER_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.CELL_VALUE_START;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.UNFILTERED_END;
import static org.apache.cassandra.io.sstable.SSTableCursorReader.State.isState;

// Cursor state
class StatefulCursor extends SSTableCursorReader
{
    private final Config.CorruptedTombstoneStrategy corruptedTombstoneStrategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    private final boolean corruptedTombstoneValidationEnabled = corruptedTombstoneStrategy != Config.CorruptedTombstoneStrategy.disabled;

    private PartitionDescriptor currPartition;
    /**
     * used for tracking reader and writer order, as well as last pk
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
        super(reader, diskAccessMode);
        bytesReadPositionSnapshot = position();
        currPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        prevPartition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
        unfiltered = new UnfilteredDescriptor(reader.header.clusteringTypes().toArray(AbstractType[]::new));
    }

    public int readPartitionHeader()
    {
        swapCurrAndPrevPartition();
        int state = readPartitionHeader(currPartition);

        if (prevPartition.keyLength() != 0 && prevPartition.key().compareTo(currPartition.key()) >= 0)
            corruptSSTableKeysOOO();
        if (corruptedTombstoneValidationEnabled)
            validateInvalidPartitionDeletion();
        return state;
    }

    /**
     * Hands the write side the instance holding the PREVIOUS partition's header and takes {@code floater} in
     * its place, so the caller owns a stable "last written partition" without copying the key.
     *
     * It is {@code prev} rather than {@code curr} because {@code curr} is the next read's out-of-order
     * validation source: stealing that would make the comparison run against a foreign key, which for valid
     * data still passes — the last written key is at or below this cursor's next key — so nothing would fail
     * and one class of corrupt input would silently stop being detected.
     *
     * Taking {@code prev} is safe because it has already been consumed: {@link #readPartitionHeader} compares
     * it against the freshly read {@code curr} before returning, and the swap means the floater only ever
     * becomes a load target, never a comparison source. Its stale contents are therefore unobservable,
     * including in the out-of-order message, which is built after the swap.
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
     * Times the curr/prev partition slots have been exchanged, so the write side can assert the deferral rule
     * its steal depends on: that the prev slot has advanced exactly once since the partition it recorded was
     * written, and therefore holds that partition. The executable form of "the prepare loop reads at most once
     * per cursor per round".
     *
     * Counts swaps rather than reads because {@link #resetAfterDone} also advances the slots — it swaps and
     * then resets only {@code curr} — and that is the path by which the last written partition reaches the prev
     * slot when the compaction finishes. A read counter would leave the steal on the finish path unaccounted
     * for.
     */
    long partitionSwaps()
    {
        return partitionSwaps;
    }

    /**
     * Hands the write side the instance holding the unfiltered this cursor has just had written out, and
     * takes {@code floater} in its place, so the caller owns a stable "last written unfiltered" without
     * copying the clustering.
     *
     * Unlike {@link #detachPrevPartition} this cannot be deferred by a round: there is one slot, so a value
     * left to wait would be overwritten by the next read. It does not need to be. Once the unfiltered has
     * been written, everything that reads the descriptor has run — the merge, the write, and the open
     * range-tombstone bookkeeping, which copies deletion times rather than retaining them — and the cursor
     * is at {@code UNFILTERED_END}, which the caller advances out of immediately after; the next load into
     * the floater comes later, in the following round's prepare-and-sort. From every state that advance can
     * reach, the next touch of the slot is another load into it: a {@link #readRowHeader} or
     * {@link #readTombstoneMarker} from {@code ROW_START}/{@code TOMBSTONE_START}, or
     * {@link #resetAfterDone}'s reset. At {@code PARTITION_END} nothing reads it
     * at all, because the clustering comparison short-circuits on that state. So the floater's stale contents
     * are unobservable.
     *
     * The {@code UNFILTERED_END} premise is asserted rather than argued, because it is what makes the steal
     * safe undeferred: a cursor stolen from in any other state would keep the floater across a clustering
     * comparison that reads it, and mis-order the merge silently.
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
     * @return true if reset, false if already been reset
     */
    public boolean resetAfterDone()
    {
        if (resetAfterDone)
            return false;
        resetAfterDone = true;
        swapCurrAndPrevPartition();
        // only current is reset, prev is still needed.
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
        long latestByteReadPosition = isEOF() ? uncompressedLength() : position();
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
        // Validate only where a cell was actually surfaced. Every path that surfaces one returns
        // CELL_VALUE_START or CELL_END, including a valueless final cell; UNFILTERED_END means the
        // dropped-column filter discarded every remaining column, and cellLiveness then still
        // describes the last DISCARDED cell rather than the current position.
        if (corruptedTombstoneValidationEnabled && isState(state, CELL_VALUE_START | CELL_END))
            validateInvalidCellDeletion();
        return state;
    }

    private void validateInvalidTombstoneDeletion()
    {
        if (!unfiltered.deletionTime().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowDeletion="+currPartition.deletionTime().toString());
        }
        if (unfiltered.isBoundary() && !unfiltered.deletionTime2().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowDeletion2="+currPartition.deletionTime().toString());
        }
    }

    private void validateInvalidCellDeletion()
    {
        ReusableCellLivenessInfo cellLiveness = cellCursor().cellLiveness;
        if (hasInvalidCellDeletion(cellLiveness.ttl(), cellLiveness.localDeletionTime())) {
            UnfilteredValidation.handleInvalid(
            ssTableReader().metadata(),
            currPartition.key(),
            ssTableReader(),
            "cellLiveness="+cellLiveness);
        }
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
        if (!unfiltered.deletionTime().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowDeletion="+currPartition.deletionTime().toString());
        }
        ReusableLivenessInfo livenessInfo = unfiltered.livenessInfo();
        if (hasInvalidRowLiveness(livenessInfo.ttl(), livenessInfo.localExpirationTime())) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "rowLiveness="+livenessInfo.toString());
        }

    }

    private void validateInvalidPartitionDeletion()
    {
        if (!currPartition.deletionTime().validate()) {
            UnfilteredValidation.handleInvalid(
                ssTableReader().metadata(),
                currPartition.key(),
                ssTableReader(),
                "partitionLevelDeletion="+currPartition.deletionTime().toString());
        }
    }
}
