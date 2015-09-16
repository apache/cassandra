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

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Stats used for the encoding of the rows and tombstones of a given source.
 * <p>
 * Those stats are used to optimize the on-wire and on-disk storage of rows. More precisely,
 * the {@code minTimestamp}, {@code minLocalDeletionTime} and {@code minTTL} stats are used to
 * delta-encode those information for the sake of vint encoding.
 * <p>
 * Note that due to their use, those stats can suffer to be somewhat inaccurate (the more incurrate
 * they are, the less effective the storage will be, but provided the stats are not completly wacky,
 * this shouldn't have too huge an impact on performance) and in fact they will not always be
 * accurate for reasons explained in {@link SerializationHeader#make}.
 */
public class EncodingStats
{
    // Default values for the timestamp, deletion time and ttl. We use this both for NO_STATS, but also to serialize
    // an EncodingStats. Basically, we encode the diff of each value of to these epoch, which give values with better vint encoding.
    private static final long TIMESTAMP_EPOCH;
    private static final int DELETION_TIME_EPOCH;
    private static final int TTL_EPOCH = 0;
    static
    {
        // We want a fixed epoch, but that provide small values when substracted from our timestamp and deletion time.
        // So we somewhat arbitrary use the date of the summit 2015, which should hopefully roughly correspond to 3.0 release.
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"), Locale.US);
        c.set(Calendar.YEAR, 2015);
        c.set(Calendar.MONTH, Calendar.SEPTEMBER);
        c.set(Calendar.DAY_OF_MONTH, 22);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        TIMESTAMP_EPOCH = c.getTimeInMillis() * 1000; // timestamps should be in microseconds by convention
        DELETION_TIME_EPOCH = (int)(c.getTimeInMillis() / 1000); // local deletion times are in seconds
    }

    // We should use this sparingly obviously
    public static final EncodingStats NO_STATS = new EncodingStats(TIMESTAMP_EPOCH, DELETION_TIME_EPOCH, TTL_EPOCH);

    public static final Serializer serializer = new Serializer();

    public final long minTimestamp;
    public final int minLocalDeletionTime;
    public final int minTTL;

    public EncodingStats(long minTimestamp,
                         int minLocalDeletionTime,
                         int minTTL)
    {
        // Note that the exact value of those don't impact correctness, just the efficiency of the encoding. So when we
        // get a value for timestamp (resp. minLocalDeletionTime) that means 'no object had a timestamp' (resp. 'a local
        // deletion time'), then what value we store for minTimestamp (resp. minLocalDeletionTime) doesn't matter, and
        // it's thus more efficient to use our EPOCH numbers, since it will result in a guaranteed 1 byte encoding.

        this.minTimestamp = minTimestamp == LivenessInfo.NO_TIMESTAMP ? TIMESTAMP_EPOCH : minTimestamp;
        this.minLocalDeletionTime = minLocalDeletionTime == LivenessInfo.NO_EXPIRATION_TIME ? DELETION_TIME_EPOCH : minLocalDeletionTime;
        this.minTTL = minTTL;
    }

    /**
     * Merge this stats with another one.
     * <p>
     * The comments of {@link SerializationHeader#make} applies here too, i.e. the result of
     * merging will be not totally accurate but we can live with that.
     */
    public EncodingStats mergeWith(EncodingStats that)
    {
        long minTimestamp = this.minTimestamp == TIMESTAMP_EPOCH
                          ? that.minTimestamp
                          : (that.minTimestamp == TIMESTAMP_EPOCH ? this.minTimestamp : Math.min(this.minTimestamp, that.minTimestamp));

        int minDelTime = this.minLocalDeletionTime == DELETION_TIME_EPOCH
                       ? that.minLocalDeletionTime
                       : (that.minLocalDeletionTime == DELETION_TIME_EPOCH ? this.minLocalDeletionTime : Math.min(this.minLocalDeletionTime, that.minLocalDeletionTime));

        int minTTL = this.minTTL == TTL_EPOCH
                   ? that.minTTL
                   : (that.minTTL == TTL_EPOCH ? this.minTTL : Math.min(this.minTTL, that.minTTL));

        return new EncodingStats(minTimestamp, minDelTime, minTTL);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EncodingStats that = (EncodingStats) o;

        return this.minLocalDeletionTime == that.minLocalDeletionTime
            && this.minTTL == that.minTTL
            && this.minTimestamp == that.minTimestamp;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minTimestamp, minLocalDeletionTime, minTTL);
    }

    @Override
    public String toString()
    {
        return String.format("EncodingStats(ts=%d, ldt=%d, ttl=%d)", minTimestamp, minLocalDeletionTime, minTTL);
    }

    public static class Collector implements PartitionStatisticsCollector
    {
        private boolean isTimestampSet;
        private long minTimestamp = Long.MAX_VALUE;

        private boolean isDelTimeSet;
        private int minDeletionTime = Integer.MAX_VALUE;

        private boolean isTTLSet;
        private int minTTL = Integer.MAX_VALUE;

        public void update(LivenessInfo info)
        {
            if (info.isEmpty())
                return;

            updateTimestamp(info.timestamp());

            if (info.isExpiring())
            {
                updateTTL(info.ttl());
                updateLocalDeletionTime(info.localExpirationTime());
            }
        }

        public void update(Cell cell)
        {
            updateTimestamp(cell.timestamp());
            if (cell.isExpiring())
            {
                updateTTL(cell.ttl());
                updateLocalDeletionTime(cell.localDeletionTime());
            }
            else if (cell.isTombstone())
            {
                updateLocalDeletionTime(cell.localDeletionTime());
            }
        }

        public void update(DeletionTime deletionTime)
        {
            if (deletionTime.isLive())
                return;

            updateTimestamp(deletionTime.markedForDeleteAt());
            updateLocalDeletionTime(deletionTime.localDeletionTime());
        }

        public void updateTimestamp(long timestamp)
        {
            isTimestampSet = true;
            minTimestamp = Math.min(minTimestamp, timestamp);
        }

        public void updateLocalDeletionTime(int deletionTime)
        {
            isDelTimeSet = true;
            minDeletionTime = Math.min(minDeletionTime, deletionTime);
        }

        public void updateTTL(int ttl)
        {
            isTTLSet = true;
            minTTL = Math.min(minTTL, ttl);
        }

        public void updateColumnSetPerRow(long columnSetInRow)
        {
        }

        public void updateHasLegacyCounterShards(boolean hasLegacyCounterShards)
        {
            // We don't care about this but this come with PartitionStatisticsCollector
        }

        public EncodingStats get()
        {
            return new EncodingStats(isTimestampSet ? minTimestamp : TIMESTAMP_EPOCH,
                                     isDelTimeSet ? minDeletionTime : DELETION_TIME_EPOCH,
                                     isTTLSet ? minTTL : TTL_EPOCH);
        }

        public static EncodingStats collect(Row staticRow, Iterator<Row> rows, DeletionInfo deletionInfo)
        {
            Collector collector = new Collector();
            deletionInfo.collectStats(collector);
            if (!staticRow.isEmpty())
                Rows.collectStats(staticRow, collector);
            while (rows.hasNext())
                Rows.collectStats(rows.next(), collector);
            return collector.get();
        }
    }

    public static class Serializer
    {
        public void serialize(EncodingStats stats, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH);
            out.writeUnsignedVInt(stats.minLocalDeletionTime - DELETION_TIME_EPOCH);
            out.writeUnsignedVInt(stats.minTTL - TTL_EPOCH);
        }

        public int serializedSize(EncodingStats stats)
        {
            return TypeSizes.sizeofUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH)
                   + TypeSizes.sizeofUnsignedVInt(stats.minLocalDeletionTime - DELETION_TIME_EPOCH)
                   + TypeSizes.sizeofUnsignedVInt(stats.minTTL - TTL_EPOCH);
        }

        public EncodingStats deserialize(DataInputPlus in) throws IOException
        {
            long minTimestamp = in.readUnsignedVInt() + TIMESTAMP_EPOCH;
            int minLocalDeletionTime = (int)in.readUnsignedVInt() + DELETION_TIME_EPOCH;
            int minTTL = (int)in.readUnsignedVInt() + TTL_EPOCH;
            return new EncodingStats(minTimestamp, minLocalDeletionTime, minTTL);
        }
    }
}
