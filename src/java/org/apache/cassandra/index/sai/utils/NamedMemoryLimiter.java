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
package org.apache.cassandra.index.sai.utils;

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.FBUtilities;

/**
 * A simple, thread-safe memory usage tracker, named to reflect a particular scope.
 */
@ThreadSafe
public final class NamedMemoryLimiter
{
    private static final Logger logger = LoggerFactory.getLogger(NamedMemoryLimiter.class);
    
    private final AtomicLong bytesUsed = new AtomicLong(0);
    private final String scope;

    private long limitBytes;

    public NamedMemoryLimiter(long limitBytes, String scope)
    {
        this.limitBytes = limitBytes;
        this.scope = scope;

        logger.info("[{}]: Memory limiter using limit of {}...", scope, FBUtilities.prettyPrintMemory(limitBytes));
    }

    /**
     * @return true if the current number of bytes allocated against the tracker has breached the limit, false otherwise
     */
    public boolean usageExceedsLimit()
    {
        return currentBytesUsed() > limitBytes;
    }

    public long increment(long bytes)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: Incrementing tracked memory usage by {} bytes from current usage of {}...", scope, bytes, currentBytesUsed());
        return bytesUsed.addAndGet(bytes);
    }

    public long decrement(long bytes)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: Decrementing tracked memory usage by {} bytes from current usage of {}...", scope, bytes, currentBytesUsed());
        return bytesUsed.addAndGet(-bytes);
    }

    public long currentBytesUsed()
    {
        return bytesUsed.get();
    }
    
    public long limitBytes()
    {
        return limitBytes;
    }

    @VisibleForTesting
    public void setLimitBytes(long bytes)
    {
        limitBytes = bytes;
    }
}
