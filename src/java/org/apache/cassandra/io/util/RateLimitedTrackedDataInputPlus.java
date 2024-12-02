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

package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.streaming.RateLimiter;

public class RateLimitedTrackedDataInputPlus extends TrackedDataInputPlus
{
    private long totalBytesToRead;
    private int currentAcquiredBytes;
    private int batchSize;
    private final RateLimiter limiter;

    public RateLimitedTrackedDataInputPlus(DataInput source, long limit, RateLimiter limiter, long totalToRead, int batchSize)
    {
        super(source, limit);
        this.totalBytesToRead = totalToRead;
        this.limiter = limiter;
        this.currentAcquiredBytes = 0;
        this.batchSize = batchSize;
    }

    public void reset(long bytesRead, long totalBytesToRead)
    {
        this.bytesRead = bytesRead;
        this.totalBytesToRead = totalBytesToRead;
    }

    public long getTotalBytesToRead()
    {
        return totalBytesToRead;
    }

    @VisibleForTesting
    public int getCurrentAcquiredBytes()
    {
        return currentAcquiredBytes;
    }

    public void checkCanRead(int size) throws IOException
    {
        super.checkCanRead(size);
        if (limiter != null && limiter.isRateLimited())
        {
            long maxRemainingBytesToAcquire = totalBytesToRead - bytesRead;
            while (currentAcquiredBytes < size)
            {
                int bytesToAcquire = (int) Math.min(batchSize, maxRemainingBytesToAcquire);
                assert bytesToAcquire > 0 : String.format("Trying to acquire %d bytes which is not greater than 0 and this means something wrong.", bytesToAcquire);
                limiter.acquire(bytesToAcquire);
                currentAcquiredBytes += bytesToAcquire;
                maxRemainingBytesToAcquire -= bytesToAcquire;
            }
            // reduce acquiredBytes
            currentAcquiredBytes -= size;
        }
    }
}
