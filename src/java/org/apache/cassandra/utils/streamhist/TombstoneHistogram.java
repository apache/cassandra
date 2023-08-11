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
package org.apache.cassandra.utils.streamhist;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder.DataHolder;

/**
 * A snapshot or finished histrogram of tombstones for a sstable, as generated from {@link StreamingTombstoneHistogramBuilder}.
 */
public class TombstoneHistogram
{
    // Buffer with point-value pair
    private final DataHolder bin;

    /**
     * Creates a new histogram with max bin size of maxBinSize
     */
    TombstoneHistogram(DataHolder holder)
    {
        bin = new DataHolder(holder);
    }

    public static TombstoneHistogram createDefault()
    {
        return new TombstoneHistogram(new DataHolder(0, 1));
    }

    /**
     * Calculates estimated number of points in interval [-inf,b].
     *
     * @param b upper bound of a interval to calculate sum
     * @return estimated number of points in a interval [-inf,b].
     */
    public double sum(double b)
    {
        return bin.sum((int) b);
    }

    public int size()
    {
        return this.bin.size();
    }

    public <E extends Exception> void forEach(HistogramDataConsumer<E> histogramDataConsumer) throws E
    {
        this.bin.forEach(histogramDataConsumer);
    }

    public static HistogramSerializer getSerializer(Version version)
    {
        return version.hasUIntDeletionTime() ? HistogramSerializer.instance : LegacyHistogramSerializer.instance;
    }

    public static class HistogramSerializer implements ISerializer<TombstoneHistogram>
    {
        public static final HistogramSerializer instance = new HistogramSerializer();

        public void serialize(TombstoneHistogram histogram, DataOutputPlus out) throws IOException
        {
            final int size = histogram.size();
            final int maxBinSize = size; // we write this for legacy reasons
            out.writeInt(maxBinSize);
            out.writeInt(size);
            histogram.forEach((point, value) ->
                              {
                                  out.writeLong(point);
                                  out.writeInt(value);
                              });
        }

        public TombstoneHistogram deserialize(DataInputPlus in) throws IOException
        {
            in.readInt(); // max bin size
            int size = in.readInt();
            DataHolder dataHolder = new DataHolder(size, 1);
            for (int i = 0; i < size; i++)
            {
                // Already serialized sstable metadata may contain negative deletion-time values (see CASSANDRA-14092).
                // Just do a "safe cast" and it should be good. For safety, also do that for the 'value' (tombstone count).
                long localDeletionTime = StreamingTombstoneHistogramBuilder.saturatingCastToMaxDeletionTime((long) in.readLong());
                int count = StreamingTombstoneHistogramBuilder.saturatingCastToInt(in.readInt());

                dataHolder.addValue(localDeletionTime, count);
            }

            return new TombstoneHistogram(dataHolder);
        }

        public long serializedSize(TombstoneHistogram histogram)
        {
            int maxBinSize = 0;
            long size = TypeSizes.sizeof(maxBinSize);
            final int histSize = histogram.size();
            size += TypeSizes.sizeof(histSize);
            // size of entries = size * (8(double) + 8(long))
            size += histSize * (8L + 8L);
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TombstoneHistogram))
            return false;

        TombstoneHistogram that = (TombstoneHistogram) o;
        return bin.equals(that.bin);
    }

    @Override
    public int hashCode()
    {
        return bin.hashCode();
    }
    
    public static class LegacyHistogramSerializer extends HistogramSerializer
    {
        public static final LegacyHistogramSerializer instance = new LegacyHistogramSerializer();

        public void serialize(TombstoneHistogram histogram, DataOutputPlus out) throws IOException
        {
            final int size = histogram.size();
            final int maxBinSize = size; // we write this for legacy reasons
            out.writeInt(maxBinSize);
            out.writeInt(size);
            histogram.forEach((point, value) ->
                              {
                                  out.writeDouble((double) point);
                                  out.writeLong((long) value);
                              });
        }

        public TombstoneHistogram deserialize(DataInputPlus in) throws IOException
        {
            in.readInt(); // max bin size
            int size = in.readInt();
            DataHolder dataHolder = new DataHolder(size, 1);
            for (int i = 0; i < size; i++)
            {
                // Already serialized sstable metadata may contain negative deletion-time values (see CASSANDRA-14092).
                // Just do a "safe cast" and it should be good. For safety, also do that for the 'value' (tombstone count).
                int localDeletionTime = saturatingCastToLegacyMaxDeletionTime((long) in.readDouble());
                int count = StreamingTombstoneHistogramBuilder.saturatingCastToInt(in.readLong());

                dataHolder.addValue(localDeletionTime, count);
            }

            return new TombstoneHistogram(dataHolder);
        }

        public long serializedSize(TombstoneHistogram histogram)
        {
            int maxBinSize = 0;
            long size = TypeSizes.sizeof(maxBinSize);
            final int histSize = histogram.size();
            size += TypeSizes.sizeof(histSize);
            // size of entries = size * (8(double) + 8(long))
            size += histSize * (8L + 8L);
            return size;
        }

        private static int saturatingCastToLegacyMaxDeletionTime(long value)
        {
            return (value < 0L || value > Cell.MAX_DELETION_TIME_2038_LEGACY_CAP)
                   ? Cell.MAX_DELETION_TIME_2038_LEGACY_CAP
                   : (int) value;
        }
    }
}
