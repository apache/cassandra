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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.SimpleCondition;

public abstract class AbstractWriteResponseHandler implements IAsyncCallback
{
    private static Predicate<InetAddress> isAlive = new Predicate<InetAddress>()
    {
        public boolean apply(InetAddress endpoint)
        {
            return FailureDetector.instance.isAlive(endpoint);
        }
    };

    private final SimpleCondition condition = new SimpleCondition();
    protected final Table table;
    protected final long startTime;
    protected final Collection<InetAddress> naturalEndpoints;
    protected final ConsistencyLevel consistencyLevel;
    protected final Runnable callback;
    protected final Collection<InetAddress> pendingEndpoints;
    private final WriteType writeType;

    /**
     * @param pendingEndpoints
     * @param callback A callback to be called when the write is successful.
     */
    protected AbstractWriteResponseHandler(Table table,
                                           Collection<InetAddress> naturalEndpoints,
                                           Collection<InetAddress> pendingEndpoints,
                                           ConsistencyLevel consistencyLevel,
                                           Runnable callback,
                                           WriteType writeType)
    {
        this.table = table;
        this.pendingEndpoints = pendingEndpoints;
        this.startTime = System.currentTimeMillis();
        this.consistencyLevel = consistencyLevel;
        this.naturalEndpoints = naturalEndpoints;
        this.callback = callback;
        this.writeType = writeType;
    }

    public void get() throws WriteTimeoutException
    {
        long timeout = DatabaseDescriptor.getWriteRpcTimeout() - (System.currentTimeMillis() - startTime);

        boolean success;
        try
        {
            success = condition.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }

        if (!success)
            throw new WriteTimeoutException(writeType, consistencyLevel, ackCount(), totalBlockFor());
    }

    protected int totalBlockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return consistencyLevel.blockFor(table) + pendingEndpoints.size();
    }

    protected abstract int ackCount();

    /** null message means "response from local write" */
    public abstract void response(MessageIn msg);

    public void assureSufficientLiveNodes() throws UnavailableException
    {
        consistencyLevel.assureSufficientLiveNodes(table, Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), isAlive));
    }

    protected void signal()
    {
        condition.signal();
        if (callback != null)
            callback.run();
    }
}
