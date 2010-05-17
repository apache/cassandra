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

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.SimpleCondition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuorumResponseHandler<T> implements IAsyncCallback
{
    protected static final Logger logger = LoggerFactory.getLogger( QuorumResponseHandler.class );
    protected final SimpleCondition condition = new SimpleCondition();
    protected final Collection<Message> responses = new LinkedBlockingQueue<Message>();;
    protected IResponseResolver<T> responseResolver;
    private final long startTime;
    protected int blockfor;
    
    /**
     * Constructor when response count has to be calculated and blocked for.
     */
    public QuorumResponseHandler(IResponseResolver<T> responseResolver, ConsistencyLevel consistencyLevel, String table)
    {
        this.blockfor = determineBlockFor(consistencyLevel, table);
        this.responseResolver = responseResolver;
        this.startTime = System.currentTimeMillis();
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
                for (Message message : responses)
                {
                    sb.append(message.getFrom());
                }
                throw new TimeoutException("Operation timed out - received only " + responses.size() + " responses from " + sb.toString() + " .");
            }
        }
        finally
        {
            for (Message response : responses)
            {
                MessagingService.removeRegisteredCallback(response.getMessageId());
            }
        }

        return responseResolver.resolve(responses);
    }
    
    public void response(Message message)
    {
        responses.add(message);
        if (responses.size() < blockfor) {
            return;
        }
        if (responseResolver.isDataPresent(responses))
        {
            condition.signal();
        }
    }
    
    public int determineBlockFor(ConsistencyLevel consistencyLevel, String table)
    {
        switch (consistencyLevel)
        {
            case ONE:
            case ANY:
                return 1;
            case QUORUM:
                return (DatabaseDescriptor.getQuorum(table)/ 2) + 1;
            case ALL:
                return DatabaseDescriptor.getReplicationFactor(table);
            default:
                throw new UnsupportedOperationException("invalid consistency level: " + table.toString());
        }
    }
}
