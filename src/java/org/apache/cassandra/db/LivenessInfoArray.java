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
package org.apache.cassandra.db;

import java.util.Arrays;

import org.apache.cassandra.utils.ObjectSizes;

/**
 * Utility class to store an array of liveness info efficiently.
 */
public class LivenessInfoArray
{
    private long[] timestamps;
    private int[] delTimesAndTTLs;

    public LivenessInfoArray(int initialCapacity)
    {
        this.timestamps = new long[initialCapacity];
        this.delTimesAndTTLs = new int[initialCapacity * 2];
        clear();
    }

    public void clear(int i)
    {
        timestamps[i] = LivenessInfo.NO_TIMESTAMP;
        delTimesAndTTLs[2 * i] = LivenessInfo.NO_DELETION_TIME;
        delTimesAndTTLs[2 * i + 1] = LivenessInfo.NO_TTL;
    }

    public void set(int i, LivenessInfo info)
    {
        set(i, info.timestamp(), info.ttl(), info.localDeletionTime());
    }

    public void set(int i, long timestamp, int ttl, int localDeletionTime)
    {
        this.timestamps[i] = timestamp;
        this.delTimesAndTTLs[2 * i] = localDeletionTime;
        this.delTimesAndTTLs[2 * i + 1] = ttl;
    }

    public long timestamp(int i)
    {
        return timestamps[i];
    }

    public int localDeletionTime(int i)
    {
        return delTimesAndTTLs[2 * i];
    }

    public int ttl(int i)
    {
        return delTimesAndTTLs[2 * i + 1];
    }

    public boolean isLive(int i, int nowInSec)
    {
        // See AbstractLivenessInfo.isLive().
        return localDeletionTime(i) == LivenessInfo.NO_DELETION_TIME
            || (ttl(i) != LivenessInfo.NO_TTL && nowInSec < localDeletionTime(i));
    }

    public int size()
    {
        return timestamps.length;
    }

    public void resize(int newSize)
    {
        int prevSize = size();

        timestamps = Arrays.copyOf(timestamps, newSize);
        delTimesAndTTLs = Arrays.copyOf(delTimesAndTTLs, newSize * 2);

        clear(prevSize, newSize);
    }

    public void swap(int i, int j)
    {
        long ts = timestamps[j];
        int ldt = delTimesAndTTLs[2 * j];
        int ttl = delTimesAndTTLs[2 * j + 1];

        move(i, j);

        timestamps[i] = ts;
        delTimesAndTTLs[2 * i] = ldt;
        delTimesAndTTLs[2 * i + 1] = ttl;
    }

    public void move(int i, int j)
    {
        timestamps[j] = timestamps[i];
        delTimesAndTTLs[2 * j] = delTimesAndTTLs[2 * i];
        delTimesAndTTLs[2 * j + 1] = delTimesAndTTLs[2 * i + 1];
    }

    public void clear()
    {
        clear(0, size());
    }

    private void clear(int from, int to)
    {
        Arrays.fill(timestamps, from, to, LivenessInfo.NO_TIMESTAMP);
        for (int i = from; i < to; i++)
        {
            delTimesAndTTLs[2 * i] = LivenessInfo.NO_DELETION_TIME;
            delTimesAndTTLs[2 * i + 1] = LivenessInfo.NO_TTL;
        }
    }

    public int dataSize()
    {
        return 16 * size();
    }

    public long unsharedHeapSize()
    {
        return ObjectSizes.sizeOfArray(timestamps)
             + ObjectSizes.sizeOfArray(delTimesAndTTLs);
    }

    public static Cursor newCursor()
    {
        return new Cursor();
    }

    public static class Cursor extends AbstractLivenessInfo
    {
        private LivenessInfoArray array;
        private int i;

        public Cursor setTo(LivenessInfoArray array, int i)
        {
            this.array = array;
            this.i = i;
            return this;
        }

        public long timestamp()
        {
            return array.timestamps[i];
        }

        public int localDeletionTime()
        {
            return array.delTimesAndTTLs[2 * i];
        }

        public int ttl()
        {
            return array.delTimesAndTTLs[2 * i + 1];
        }
    }
}

