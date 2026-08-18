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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime.ReusableDeletionTime;
import org.apache.cassandra.db.ReusableLivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.RandomAccessReader;

public class UnfilteredDescriptor extends ClusteringDescriptor
{
    private final ReusableLivenessInfo rowLivenessInfo = new ReusableLivenessInfo();
    private final ReusableDeletionTime deletionTime = ReusableDeletionTime.live();
    private final ReusableDeletionTime deletionTime2 = ReusableDeletionTime.live();

    private long position;
    private int flags;
    private int extendedFlags;

    private long unfilteredSize;
    private long unfilteredDataStart;
    private long prevUnfilteredSize;
    Columns rowColumns;
    // For supersets of < 64 columns: bit i set means the i-th column of rowColumns (in
    // iteration order) is ABSENT from this row; 0 means all present. Always 0 for markers,
    // all-columns rows, and >= 64-column supersets (which use the words below).
    private long missingColumnsMask;
    // For supersets of >= 64 columns: PRESENT-column mask words over superset iteration
    // order (bit i of word i/64 set means superset column i is present in this row).
    // Reusable grow-once scratch; valid only when useColumnsWords is set (a row that took
    // the large-subset decode), consumed by CellCursor within the same row.
    private long[] presentColumnsWords;
    private boolean useColumnsWords;

    public UnfilteredDescriptor(AbstractType<?>[] clusteringTypes)
    {
        super(clusteringTypes);
    }

    void loadTombstone(RandomAccessReader dataReader,
                       SerializationHeader serializationHeader,
                       int flags) throws IOException
    {
        this.flags = flags;
        this.extendedFlags = 0;
        rowColumns = null;
        missingColumnsMask = 0;
        useColumnsWords = false;
        byte clusteringKind = dataReader.readByte();
        if (clusteringKind == STATIC_CLUSTERING_KIND || clusteringKind == ROW_CLUSTERING_KIND) {
            // STATIC_CLUSTERING or CLUSTERING -> no deletion info, should not happen
            throw new IllegalStateException();
        }

        int columnsBound = dataReader.readUnsignedShort();
        loadClustering(dataReader, clusteringKind, columnsBound);
        unfilteredSize = dataReader.readUnsignedVInt();
        prevUnfilteredSize = dataReader.readUnsignedVInt(); // debug only, unused otherwise
        if (clusteringKind == EXCL_END_INCL_START_BOUNDARY_CLUSTERING_KIND ||
            clusteringKind == INCL_END_EXCL_START_BOUNDARY_CLUSTERING_KIND)
        {
            // boundary
            // CLOSE
            serializationHeader.readDeletionTime(dataReader, deletionTime);
            // OPEN
            serializationHeader.readDeletionTime(dataReader, deletionTime2);
        }
        else
        {
            // bound
            // CLOSE|OPEN
            serializationHeader.readDeletionTime(dataReader, deletionTime);
        }
    }

    void loadRow(RandomAccessReader dataReader,
                 SerializationHeader serializationHeader,
                 DeserializationHelper deserializationHelper,
                 int flags,
                 int extendedFlags) throws IOException {
        // body = whatever is covered by size, so inclusive of the prev_row_size inclusive of flags
        // (and the extended-flags byte too, when a non-static row carries one for a shadowable deletion)
        position = dataReader.getPosition() - (UnfilteredSerializer.isExtended(flags) ? 2 : 1);
        this.flags = flags;
        this.extendedFlags = extendedFlags;

        loadClustering(dataReader, ROW_CLUSTERING_KIND, this.clusteringTypes.length);

        rowColumns = serializationHeader.columns(false);

        loadCommonRowFields(dataReader, serializationHeader, deserializationHelper, flags);
    }

    void loadStaticRow(RandomAccessReader dataReader,
                       SerializationHeader serializationHeader,
                       DeserializationHelper deserializationHelper,
                       int flags,
                       int extendedFlags) throws IOException {
        // body = whatever is covered by size, so inclusive of the prev_row_size inclusive of flags
        position = dataReader.getPosition() - 2;
        this.flags = flags;
        this.extendedFlags = extendedFlags;
        // no clustering
        loadClustering(dataReader, STATIC_CLUSTERING_KIND, 0);
        rowColumns = serializationHeader.columns(true);

        loadCommonRowFields(dataReader, serializationHeader, deserializationHelper, flags);
    }

    private void loadCommonRowFields(RandomAccessReader dataReader,
                                     SerializationHeader serializationHeader,
                                     DeserializationHelper deserializationHelper,
                                     int flags) throws IOException
    {
        unfilteredSize = dataReader.readUnsignedVInt();
        unfilteredDataStart = dataReader.getPosition();
        prevUnfilteredSize = dataReader.readUnsignedVInt(); // debug only, unused otherwise

        SSTableCursorReader.readLivenessInfo(dataReader,
                                             serializationHeader,
                                             deserializationHelper,
                                             flags,
                                             rowLivenessInfo);

        if (UnfilteredSerializer.hasDeletion(flags))
        {
            // struct delta_deletion_time {
            //    varint delta_marked_for_delete_at;
            //    varint delta_local_deletion_time;
            //};
            serializationHeader.readDeletionTime(dataReader, deletionTime);
        }
        else
        {
            deletionTime.resetLive();
        }
        useColumnsWords = false;
        if (!UnfilteredSerializer.hasAllColumns(flags))
        {
            if (rowColumns.size() < 64)
            {
                // Garbage-free subset decode. Wire format per Columns.Serializer.deserializeSubset
                // (Columns.java): an unsigned vint bitmask of MISSING columns, bit i corresponding
                // to the i-th column of the superset in iteration order; 0 means all present.
                // rowColumns stays the superset; consumers filter via missingColumnsMask().
                long encoded = dataReader.readUnsignedVInt();
                // sanity: shifting out the valid column bits must leave nothing — any higher
                // bit set means the vint encodes more columns than the header knows about
                // (mirrors Columns.Serializer.deserializeSubset's corruption check)
                if ((encoded >>> rowColumns.size()) != 0)
                    throw new IOException("Invalid Columns subset bytes; too many bits set: " + Long.toBinaryString(encoded));
                missingColumnsMask = encoded;
            }
            else
            {
                // >= 64-column superset: LARGE-subset wire format (Columns.Serializer
                // .serializeLargeSubset) — vint delta = supersetCount - presentCount, then
                // either the present indices (presentCount < supersetCount/2) or the missing
                // indices, each an unsigned vint superset position. Decoded garbage-free
                // into reusable present-mask words; rowColumns stays the superset so
                // CellCursor's identity cache never rebuilds per row (the < 64 mask path's
                // design, extended to words).
                long encoded = dataReader.readUnsignedVInt();
                int supersetCount = rowColumns.size();
                if (encoded > supersetCount)
                    throw new IOException("Invalid large Columns subset: missing count " + encoded + " of " + supersetCount);
                int delta = (int) encoded;
                int columnCount = supersetCount - delta;
                int nWords = (supersetCount + 63) >>> 6;
                if (presentColumnsWords == null || presentColumnsWords.length < nWords)
                    presentColumnsWords = new long[nWords]; // grow-once, amortized zero
                if (columnCount < supersetCount / 2)
                {
                    // present-mode: start all-absent, set each present index
                    java.util.Arrays.fill(presentColumnsWords, 0, nWords, 0L);
                    for (int i = 0; i < columnCount; i++)
                    {
                        int idx = dataReader.readUnsignedVInt32();
                        if (idx < 0 || idx >= supersetCount)
                            throw new IOException("Invalid large Columns subset: present index " + idx + " of " + supersetCount);
                        presentColumnsWords[idx >>> 6] |= 1L << (idx & 63);
                    }
                }
                else
                {
                    // missing-mode: start all-present (last word trimmed to the column
                    // range), clear each missing index. delta == 0 degenerates to
                    // all-present, matching deserializeSubset's encoded == 0 case.
                    java.util.Arrays.fill(presentColumnsWords, 0, nWords, -1L);
                    if ((supersetCount & 63) != 0)
                        presentColumnsWords[nWords - 1] = -1L >>> (64 - (supersetCount & 63));
                    for (int i = 0; i < delta; i++)
                    {
                        int idx = dataReader.readUnsignedVInt32();
                        if (idx < 0 || idx >= supersetCount)
                            throw new IOException("Invalid large Columns subset: missing index " + idx + " of " + supersetCount);
                        presentColumnsWords[idx >>> 6] &= ~(1L << (idx & 63));
                    }
                }
                useColumnsWords = true;
                missingColumnsMask = 0;
            }
        }
        else
        {
            missingColumnsMask = 0;
        }
    }

    public void resetUnfiltered()
    {
        resetClustering();
        position = 0;
        flags = 0;
        extendedFlags = 0;
        unfilteredSize = 0;
        unfilteredDataStart = 0;
        prevUnfilteredSize = 0;
        rowColumns = null;
        missingColumnsMask = 0;
        useColumnsWords = false;
    }

    public long position()
    {
        return position;
    }

    public ReusableLivenessInfo livenessInfo()
    {
        return rowLivenessInfo;
    }

    public ReusableDeletionTime deletionTime()
    {
        return deletionTime;
    }

    public ReusableDeletionTime openDeletionTime()
    {
        return isBoundary() ? deletionTime2 : isEndBound() ? null : deletionTime;
    }

    public ReusableDeletionTime deletionTime2()
    {
        return deletionTime2;
    }

    // Row.Deletion.isShadowable(): deprecated since 4.0 (CASSANDRA-11500), reachable only on
    // old Materialized View data nothing still produces. A live deletion is never shadowable
    // (Row.Deletion's own constructor asserts this), so this is meaningful only alongside a
    // non-live deletionTime().
    public boolean isShadowableDeletion()
    {
        return UnfilteredSerializer.deletionIsShadowable(extendedFlags);
    }

    public int flags()
    {
        return flags;
    }

    public long size()
    {
        return unfilteredSize;
    }

    public long dataStart()
    {
        return unfilteredDataStart;
    }

    public Columns rowColumns()
    {
        return rowColumns;
    }

    /** See {@link #missingColumnsMask} */
    public long missingColumnsMask()
    {
        return missingColumnsMask;
    }

    /**
     * See {@link #presentColumnsWords}: present-column mask words for this row when the
     * superset has >= 64 columns and the row carried a subset encoding; null otherwise
     * (all columns present, or the < 64 mask path applies). The array is per-row scratch —
     * the consumer may mutate it but must finish before the next unfiltered is loaded.
     */
    public long[] presentColumnsWords()
    {
        return useColumnsWords ? presentColumnsWords : null;
    }

    @Override
    public String toString()
    {
        return "UnfilteredDescriptor{" +
               "rowLivenessInfo=" + rowLivenessInfo +
               ", deletionTime=" + deletionTime +
               ", position=" + position +
               ", flags=" + flags +
               ", extFlags=" + extendedFlags +
               ", unfilteredSize=" + unfilteredSize +
               ", prevUnfilteredSize=" + prevUnfilteredSize +
               ", unfilteredDataStart=" + unfilteredDataStart +
               ", rowColumns=" + rowColumns +
               ", clusteringTypes=" + Arrays.toString(clusteringTypes()) +
               '}';
    }
}
