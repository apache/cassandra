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

package org.apache.cassandra.stress;

import java.util.concurrent.atomic.AtomicLong;

public interface WorkManager
{
    // -1 indicates consumer should terminate
    int takePermits(int count);

    // signal all consumers to terminate
    void stop();

    static final class FixedWorkManager implements WorkManager
    {

        final AtomicLong permits;

        public FixedWorkManager(long permits)
        {
            this.permits = new AtomicLong(permits);
        }

        @Override
        public int takePermits(int count)
        {
            while (true)
            {
                long cur = permits.get();
                if (cur == 0)
                    return -1;
                count = (int) Math.min(count, cur);
                long next = cur - count;
                if (permits.compareAndSet(cur, next))
                    return count;
            }
        }

        @Override
        public void stop()
        {
            permits.getAndSet(0);
        }
    }

    static final class ContinuousWorkManager implements WorkManager
    {

        volatile boolean stop = false;

        @Override
        public int takePermits(int count)
        {
            if (stop)
                return -1;
            return count;
        }

        @Override
        public void stop()
        {
            stop = true;
        }

    }
}
