package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.UpgradeService;

public abstract class AbstractPaxosCallback<T> implements IAsyncCallback<T>
{
    protected final CountDownLatch latch;
    protected final int targets;
    private final ConsistencyLevel consistency;
    private volatile boolean gotUpgradeError = false;

    public AbstractPaxosCallback(int targets, ConsistencyLevel consistency)
    {
        this.targets = targets;
        this.consistency = consistency;
        latch = new CountDownLatch(targets);
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }

    public int getResponseCount()
    {
        return (int) (targets - latch.getCount());
    }

    @Override
    public void response(MessageIn<T> msg)
    {
        if (msg.parameters.containsKey(UpgradeService.PAXOS_UPGRADE_ERROR))
        {
            while (latch.getCount() > 0)
                latch.countDown();
            gotUpgradeError = true;
        }
        handleResponse(msg);
    }

    public abstract void handleResponse(MessageIn<T> msg);

    public void await() throws WriteTimeoutException, PaxosState.UpgradedException
    {
        try
        {
            if (!latch.await(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
                throw new WriteTimeoutException(WriteType.CAS, consistency, getResponseCount(), targets);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        if (gotUpgradeError)
        {
            throw new PaxosState.UpgradedException();
        }
    }
}
