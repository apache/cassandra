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

package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncResult implements IAsyncResult
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncResult.class);

    private byte[] result;
    private AtomicBoolean done = new AtomicBoolean(false);
    private Lock lock = new ReentrantLock();
    private Condition condition;
    private long startTime;
    private InetAddress from;

    public AsyncResult()
    {        
        condition = lock.newCondition();
        startTime = System.currentTimeMillis();
    }    
            
    public byte[] get(long timeout, TimeUnit tu) throws TimeoutException
    {
        lock.lock();
        try
        {            
            boolean bVal = true;
            try
            {
                if (!done.get())
                {
                    timeout = TimeUnit.MILLISECONDS.convert(timeout, tu);
                    long overall_timeout = timeout - (System.currentTimeMillis() - startTime);
                    bVal = overall_timeout > 0 && condition.await(overall_timeout, TimeUnit.MILLISECONDS);
                }
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError(ex);
            }
            
            if (!bVal && !done.get())
            {                                           
                throw new TimeoutException("Operation timed out.");
            }
        }
        finally
        {
            lock.unlock();
        }
        return result;
    }

    public void result(Message response)
    {        
        try
        {
            lock.lock();
            if (!done.get())
            {
                from = response.getFrom();
                result = response.getMessageBody();
                done.set(true);
                condition.signal();
            }
        }
        finally
        {
            lock.unlock();
        }        
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public InetAddress getFrom()
    {
        return from;
    }
}
