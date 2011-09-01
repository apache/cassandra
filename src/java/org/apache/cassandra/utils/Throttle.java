/**
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

package org.apache.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the timing/state required to throttle a caller to a target throughput in
 * bytes per millisecond, when periodically passed an absolute count of bytes.
 */
public class Throttle
{
    private static Logger logger = LoggerFactory.getLogger(Throttle.class);

    private final String name;
    private final ThroughputFunction fun;

    // the bytes that had been handled the last time we delayed to throttle,
    // and the time in milliseconds when we last throttled
    private long bytesAtLastDelay;
    private long timeAtLastDelay;

    // current target bytes of throughput per millisecond
    private int targetBytesPerMS = -1;

    public Throttle(String name, ThroughputFunction fun)
    {
        this.name = name;
        this.fun = fun;
    }

    /** @param currentBytes Bytes of throughput since the beginning of the task. */
    public void throttle(long currentBytes)
    {
        throttleDelta(currentBytes - bytesAtLastDelay);
    }

    /** @param bytesDelta Bytes of throughput since the last call to throttle*(). */
    public void throttleDelta(long bytesDelta)
    {
        int newTargetBytesPerMS = fun.targetThroughput();
        if (newTargetBytesPerMS < 1)
            // throttling disabled
            return;

        // if the target changed, log
        if (newTargetBytesPerMS != targetBytesPerMS)
            logger.debug("{} target throughput now {} bytes/ms.", this, newTargetBytesPerMS);
        targetBytesPerMS = newTargetBytesPerMS;

        // time passed since last delay
        long msSinceLast = System.currentTimeMillis() - timeAtLastDelay;
        // the excess bytes in this period
        long excessBytes = bytesDelta - msSinceLast * targetBytesPerMS;

        // the time to delay to recap the deficit
        long timeToDelay = excessBytes / Math.max(1, targetBytesPerMS);
        if (timeToDelay > 0)
        {
            if (logger.isTraceEnabled())
                logger.trace(String.format("%s actual throughput was %d bytes in %d ms: throttling for %d ms",
                                           this, bytesDelta, msSinceLast, timeToDelay));
            try
            {
                Thread.sleep(timeToDelay);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
        bytesAtLastDelay += bytesDelta;
        timeAtLastDelay = System.currentTimeMillis();
    }

    @Override
    public String toString()
    {
        return "Throttle(for=" + name + ")";
    }
    
    public interface ThroughputFunction
    {
        /**
         * @return The instantaneous target throughput in bytes per millisecond. Targets less
         * than or equal to zero will disable throttling.
         */
        public int targetThroughput();
    }
}
