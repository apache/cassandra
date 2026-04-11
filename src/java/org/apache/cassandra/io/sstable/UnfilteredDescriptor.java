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
                 int flags) throws IOException {
        // body = whatever is covered by size, so inclusive of the prev_row_size inclusive of flags
        position = dataReader.getPosition() - 1;
        this.flags = flags;
        this.extendedFlags = 0;

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
        if (!UnfilteredSerializer.hasAllColumns(flags))
        {
            // TODO: re-implement GC free
            rowColumns = Columns.serializer.deserializeSubset(rowColumns, dataReader);
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
