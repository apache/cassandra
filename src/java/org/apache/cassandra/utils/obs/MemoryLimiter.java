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

package org.apache.cassandra.utils.obs;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.utils.FBUtilities;

public class MemoryLimiter
{
    public final long maxMemory;
    private final AtomicLong currentMemory;
    private final String exceptionFormat;

    public MemoryLimiter(long maxMemory, String exceptionFormat)
    {
        this.maxMemory = maxMemory;
        this.currentMemory = new AtomicLong();
        this.exceptionFormat = exceptionFormat;
    }

    public void increment(long bytesCount) throws ReachedMemoryLimitException
    {
        assert bytesCount >= 0;
        long bytesCountAfterAllocation = this.currentMemory.addAndGet(bytesCount);
        if (bytesCountAfterAllocation >= maxMemory)
        {
            this.currentMemory.addAndGet(-bytesCount);

            throw new ReachedMemoryLimitException(String.format(exceptionFormat,
                                                                FBUtilities.prettyPrintMemory(bytesCount),
                                                                FBUtilities.prettyPrintMemory(maxMemory),
                                                                FBUtilities.prettyPrintMemory(bytesCountAfterAllocation - bytesCount)));
        }
    }

    public void decrement(long bytesCount)
    {
        assert bytesCount >= 0;
        long result = this.currentMemory.addAndGet(-bytesCount);
        assert result >= 0;
    }

    public long memoryAllocated()
    {
        return currentMemory.get();
    }

    public static class ReachedMemoryLimitException extends Exception
    {
        public ReachedMemoryLimitException(String message)
        {
            super(message);
        }
    }
}
