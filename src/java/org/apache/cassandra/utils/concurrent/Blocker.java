/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Blocker
{
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition unblocked = lock.newCondition();
    private volatile boolean block = false;

    public void block(boolean block)
    {
        this.block = block;
        if (!block)
        {
            lock.lock();
            try
            {
                unblocked.signalAll();
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    public void ask()
    {
        if (block)
        {
            lock.lock();
            try
            {
                while (block)
                    unblocked.awaitUninterruptibly();
            }
            finally
            {
                lock.unlock();
            }
        }
    }
}
