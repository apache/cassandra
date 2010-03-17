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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.SimpleCondition;

import org.apache.log4j.Logger;

public class QuorumResponseHandler<T> implements IAsyncCallback
{
    protected static final Logger logger = Logger.getLogger( QuorumResponseHandler.class );
    protected final SimpleCondition condition = new SimpleCondition();
    protected final Collection<Message> responses;
    private IResponseResolver<T> responseResolver;
    private final long startTime;

    public QuorumResponseHandler(int responseCount, IResponseResolver<T> responseResolver)
    {
        responses = new LinkedBlockingQueue<Message>();
        this.responseResolver = responseResolver;
        startTime = System.currentTimeMillis();
    }
    
    public T get() throws TimeoutException, DigestMismatchException, IOException
    {
        try
        {
            long timeout = System.currentTimeMillis() - startTime + DatabaseDescriptor.getRpcTimeout();
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
        if (responseResolver.isDataPresent(responses))
        {
            condition.signal();
        }
    }
}
