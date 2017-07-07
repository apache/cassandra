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

package org.apache.cassandra.utils.concurrent;

public class Transferer<T>
{
    private T mailbox = null;
    private boolean shutdown = false;

    public synchronized boolean transfer(T data) throws InterruptedException
    {
        if (data == null)
            throw new NullPointerException("data is null");

        if (shutdown){
            return false;
        }
        
        // wait while cell will be freed
        while (mailbox != null && !shutdown)
            wait();

        if (shutdown)
            return false;

        mailbox = data;
        notifyAll();

        // wait while data will be consumed
        while (mailbox != null && !shutdown)
            wait();
        
        if (mailbox == null) //consumed
            return true; 
        
        // shutdown
        mailbox = null; // free mailbox
        return false; // data was not transferred
    }

    public synchronized T receive() throws InterruptedException
    {
        while (mailbox == null)
            wait();

        T data = mailbox;
        mailbox = null;
        notifyAll();
        return data;
    }

    public synchronized T receiveUninterruptibly()
    {
        while (mailbox == null)
            try
            {
                wait();
            }
            catch (InterruptedException ignore)
            {
            }

        T data = mailbox;
        mailbox = null;
        notifyAll();
        return data;
    }

    public synchronized void shutdown()
    {
        shutdown = true;
        notifyAll();
    }

    /**
     * This method is needed to support restart AbstractCommitSegmentManager during tests
     */
    public synchronized void start()
    {
        shutdown = false;
        notifyAll();
    }

    public synchronized T tryReceive()
    {
        T data = mailbox;
        mailbox = null;
        notifyAll();
        return data;
    }
}
