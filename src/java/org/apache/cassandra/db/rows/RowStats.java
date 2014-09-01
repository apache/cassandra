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
package org.apache.cassandra.db.rows;

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.LivenessInfo.NO_TIMESTAMP;
import static org.apache.cassandra.db.LivenessInfo.NO_TTL;
import static org.apache.cassandra.db.LivenessInfo.NO_DELETION_TIME;

/**
 * General statistics on rows (and and tombstones) for a given source.
 * <p>
 * Those stats are used to optimize the on-wire and on-disk storage of rows. More precisely,
 * the {@code minTimestamp}, {@code minLocalDeletionTime} and {@code minTTL} stats are used to
 * delta-encode those information for the sake of vint encoding. And {@code avgColumnSetPerRow}
 * is used to decide if cells should be stored in a sparse or dense way (see {@link UnfilteredSerializer}).
 * <p>
 * Note that due to their use, those stats can suffer to be somewhat inaccurate (the more incurrate
 * they are, the less effective the storage will be, but provided the stats are not completly wacky,
 * this shouldn't have too huge an impact on performance) and in fact they will not always be
 * accurate for reasons explained in {@link SerializationHeader#make}.
 */
public class RowStats
{
    // We should use this sparingly obviously
    public static final RowStats NO_STATS = new RowStats(NO_TIMESTAMP, NO_DELETION_TIME, NO_TTL, -1);

    public static final Serializer serializer = new Serializer();

    public final long minTimestamp;
    public final int minLocalDeletionTime;
    public final int minTTL;

    // Will be < 0 if the value is unknown
    public final int avgColumnSetPerRow;

    public RowStats(long minTimestamp,
                    int minLocalDeletionTime,
                    int minTTL,
                    int avgColumnSetPerRow)
    {
        this.minTimestamp = minTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime;
        this.minTTL = minTTL;
        this.avgColumnSetPerRow = avgColumnSetPerRow;
    }

    public boolean hasMinTimestamp()
    {
        return minTimestamp != NO_TIMESTAMP;
    }

    public boolean hasMinLocalDeletionTime()
    {
        return minLocalDeletionTime != NO_DELETION_TIME;
    }

    /**
     * Merge this stats with another one.
     * <p>
     * The comments of {@link SerializationHeader#make} applies here too, i.e. the result of
     * merging will be not totally accurate but we can live with that.
     */
    public RowStats mergeWith(RowStats that)
    {
        long minTimestamp = this.minTimestamp == NO_TIMESTAMP
                          ? that.minTimestamp
                          : (that.minTimestamp == NO_TIMESTAMP ? this.minTimestamp : Math.min(this.minTimestamp, that.minTimestamp));

        int minDelTime = this.minLocalDeletionTime == NO_DELETION_TIME
                       ? that.minLocalDeletionTime
                       : (that.minLocalDeletionTime == NO_DELETION_TIME ? this.minLocalDeletionTime : Math.min(this.minLocalDeletionTime, that.minLocalDeletionTime));

        int minTTL = this.minTTL == NO_TTL
                   ? that.minTTL
                   : (that.minTTL == NO_TTL ? this.minTTL : Math.min(this.minTTL, that.minTTL));

        int avgColumnSetPerRow = this.avgColumnSetPerRow < 0
                               ? that.avgColumnSetPerRow
                               : (that.avgColumnSetPerRow < 0 ? this.avgColumnSetPerRow : (this.avgColumnSetPerRow + that.avgColumnSetPerRow) / 2);

        return new RowStats(minTimestamp, minDelTime, minTTL, avgColumnSetPerRow);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowStats rowStats = (RowStats) o;

        if (avgColumnSetPerRow != rowStats.avgColumnSetPerRow) return false;
        if (minLocalDeletionTime != rowStats.minLocalDeletionTime) return false;
        if (minTTL != rowStats.minTTL) return false;
        if (minTimestamp != rowStats.minTimestamp) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minTimestamp, minLocalDeletionTime, minTTL, avgColumnSetPerRow);
    }

    @Override
    public String toString()
    {
        return String.format("RowStats(ts=%d, ldt=%d, ttl=%d, avgColPerRow=%d)", minTimestamp, minLocalDeletionTime, minTTL, avgColumnSetPerRow);
    }

    public static class Collector
    {
        private boolean isTimestampSet;
        private long minTimestamp = Long.MAX_VALUE;

        private boolean isDelTimeSet;
        private int minDeletionTime = Integer.MAX_VALUE;

        private boolean isTTLSet;
        private int minTTL = Integer.MAX_VALUE;

        private boolean isColumnSetPerRowSet;
        private long totalColumnsSet;
        private long rows;

        public void updateTimestamp(long timestamp)
        {
            if (timestamp == NO_TIMESTAMP)
                return;

            isTimestampSet = true;
            minTimestamp = Math.min(minTimestamp, timestamp);
        }

        public void updateLocalDeletionTime(int deletionTime)
        {
            if (deletionTime == NO_DELETION_TIME)
                return;

            isDelTimeSet = true;
            minDeletionTime = Math.min(minDeletionTime, deletionTime);
        }

        public void updateDeletionTime(DeletionTime deletionTime)
        {
            if (deletionTime.isLive())
                return;

            updateTimestamp(deletionTime.markedForDeleteAt());
            updateLocalDeletionTime(deletionTime.localDeletionTime());
        }

        public void updateTTL(int ttl)
        {
            if (ttl <= NO_TTL)
                return;

            isTTLSet = true;
            minTTL = Math.min(minTTL, ttl);
        }

        public void updateColumnSetPerRow(int columnSetInRow)
        {
            updateColumnSetPerRow(columnSetInRow, 1);
        }

        public void updateColumnSetPerRow(long totalColumnsSet, long rows)
        {
            if (totalColumnsSet < 0 || rows < 0)
                return;

            this.isColumnSetPerRowSet = true;
            this.totalColumnsSet += totalColumnsSet;
            this.rows += rows;
        }

        public RowStats get()
        {
            return new RowStats(isTimestampSet ? minTimestamp : NO_TIMESTAMP,
                                 isDelTimeSet ? minDeletionTime : NO_DELETION_TIME,
                                 isTTLSet ? minTTL : NO_TTL,
                                 isColumnSetPerRowSet ? (rows == 0 ? 0 : (int)(totalColumnsSet / rows)) : -1);
        }
    }

    public static class Serializer
    {
        public void serialize(RowStats stats, DataOutputPlus out) throws IOException
        {
            out.writeLong(stats.minTimestamp);
            out.writeInt(stats.minLocalDeletionTime);
            out.writeInt(stats.minTTL);
            out.writeInt(stats.avgColumnSetPerRow);
        }

        public int serializedSize(RowStats stats, TypeSizes sizes)
        {
            return sizes.sizeof(stats.minTimestamp)
                 + sizes.sizeof(stats.minLocalDeletionTime)
                 + sizes.sizeof(stats.minTTL)
                 + sizes.sizeof(stats.avgColumnSetPerRow);
        }

        public RowStats deserialize(DataInput in) throws IOException
        {
            long minTimestamp = in.readLong();
            int minLocalDeletionTime = in.readInt();
            int minTTL = in.readInt();
            int avgColumnSetPerRow = in.readInt();
            return new RowStats(minTimestamp, minLocalDeletionTime, minTTL, avgColumnSetPerRow);
        }
    }
}
