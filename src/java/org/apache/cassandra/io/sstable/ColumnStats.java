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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.utils.StreamingHistogram;

/**
 * ColumnStats holds information about the columns for one row inside sstable
 */
public class ColumnStats
{
    /** how many columns are there in the row */
    public final int columnCount;

    /** the largest (client-supplied) timestamp in the row */
    public final long minTimestamp;
    public final long maxTimestamp;
    public final int maxLocalDeletionTime;
    /** histogram of tombstone drop time */
    public final StreamingHistogram tombstoneHistogram;

    /** max and min column names according to comparator */
    public final List<ByteBuffer> minColumnNames;
    public final List<ByteBuffer> maxColumnNames;

    public ColumnStats(int columnCount, long minTimestamp, long maxTimestamp, int maxLocalDeletionTime, StreamingHistogram tombstoneHistogram, List<ByteBuffer> minColumnNames, List<ByteBuffer> maxColumnNames)
    {
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.columnCount = columnCount;
        this.tombstoneHistogram = tombstoneHistogram;
        this.minColumnNames = minColumnNames;
        this.maxColumnNames = maxColumnNames;
    }

    public static class MinTracker<T extends Comparable<T>>
    {
        private final T defaultValue;
        private boolean isSet = false;
        private T value;

        public MinTracker(T defaultValue)
        {
            this.defaultValue = defaultValue;
        }

        public void update(T value)
        {
            if (!isSet)
            {
                this.value = value;
                isSet = true;
            }
            else
            {
                if (value.compareTo(this.value) < 0)
                    this.value = value;
            }
        }

        public T get()
        {
            if (isSet)
                return value;
            return defaultValue;
        }
    }

    public static class MaxTracker<T extends Comparable<T>>
    {
        private final T defaultValue;
        private boolean isSet = false;
        private T value;

        public MaxTracker(T defaultValue)
        {
            this.defaultValue = defaultValue;
        }

        public void update(T value)
        {
            if (!isSet)
            {
                this.value = value;
                isSet = true;
            }
            else
            {
                if (value.compareTo(this.value) > 0)
                    this.value = value;
            }
        }

        public T get()
        {
            if (isSet)
                return value;
            return defaultValue;
        }
    }
}
