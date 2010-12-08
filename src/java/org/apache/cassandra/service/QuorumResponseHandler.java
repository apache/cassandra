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

package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumResponseHandler<T> implements IAsyncCallback
{
    protected static final Logger logger = LoggerFactory.getLogger( QuorumResponseHandler.class );
    protected final SimpleCondition condition = new SimpleCondition();
    protected final IResponseResolver<T> resolver;
    private final long startTime;
    protected final int blockfor;
    
    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public QuorumResponseHandler(IResponseResolver<T> resolver, ConsistencyLevel consistencyLevel, String table)
    {
        this.blockfor = determineBlockFor(consistencyLevel, table);
        this.resolver = resolver;
        this.startTime = System.currentTimeMillis();

        logger.debug("QuorumResponseHandler blocking for {} responses", blockfor);
    }
    
    public T get() throws TimeoutException, DigestMismatchException, IOException
    {
        try
        {
            long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
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
            {
                StringBuilder sb = new StringBuilder("");
                for (Message message : resolver.getMessages())
                {
                    sb.append(message.getFrom());
                }
                throw new TimeoutException("Operation timed out - received only " + resolver.getMessageCount() + " responses from " + sb.toString() + " .");
            }
        }
        finally
        {
            for (Message response : resolver.getMessages())
            {
                MessagingService.removeRegisteredCallback(response.getMessageId());
            }
        }

        return resolver.resolve();
    }
    
    public void response(Message message)
    {
        resolver.preprocess(message);
        if (resolver.getMessageCount() < blockfor)
            return;
        if (resolver.isDataPresent())
            condition.signal();
    }
    
    public int determineBlockFor(ConsistencyLevel consistencyLevel, String table)
    {
        switch (consistencyLevel)
        {
            case ONE:
            case ANY:
                return 1;
            case QUORUM:
                return (Table.open(table).getReplicationStrategy().getReplicationFactor() / 2) + 1;
            case ALL:
                return Table.open(table).getReplicationStrategy().getReplicationFactor();
            default:
                throw new UnsupportedOperationException("invalid consistency level: " + consistencyLevel);
        }
    }

    public void assureSufficientLiveNodes(Collection<InetAddress> endpoints) throws UnavailableException
    {
        if (endpoints.size() < blockfor)
            throw new UnavailableException();
    }
}
