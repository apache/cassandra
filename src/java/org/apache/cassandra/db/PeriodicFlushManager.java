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

package org.apache.cassandra.db;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.cassandra.concurrent.*;

/**
 *  Background flusher that force-flushes a column family periodically.
 */
class PeriodicFlushManager
{
    private static Logger logger_ = Logger.getLogger(PeriodicFlushManager.class);
    private static PeriodicFlushManager instance_;
    private static Lock lock_ = new ReentrantLock();
    private ScheduledExecutorService flusher_ = new DebuggableScheduledThreadPoolExecutor(1, new NamedThreadFactory("PERIODIC-FLUSHER-POOL"));

    public static PeriodicFlushManager instance()
    {
        if ( instance_ == null )
        {
            lock_.lock();
            try
            {
                if ( instance_ == null )
                    instance_ = new PeriodicFlushManager();
            }
            finally
            {
                lock_.unlock();
            }
        }
        return instance_;
    }

    public void submitPeriodicFlusher(final ColumnFamilyStore columnFamilyStore, int flushPeriodInMinutes)
    {        
        Runnable runnable= new Runnable()
        {
            public void run()
            {
                try
                {
                    columnFamilyStore.forceFlush();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        flusher_.scheduleWithFixedDelay(runnable, flushPeriodInMinutes, flushPeriodInMinutes, TimeUnit.MINUTES);       
    }
}
