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

import java.util.*;
import java.net.InetAddress;

import org.apache.cassandra.locator.ILatencyPublisher;
import org.apache.cassandra.locator.ILatencySubscriber;

import org.apache.log4j.Logger;

public class ResponseVerbHandler implements IVerbHandler, ILatencyPublisher
{
    private static final Logger logger_ = Logger.getLogger( ResponseVerbHandler.class );
    private List<ILatencySubscriber>  subscribers = new ArrayList<ILatencySubscriber>();
    
    public void doVerb(Message message)
    {     
        String messageId = message.getMessageId();        
        IAsyncCallback cb = MessagingService.getRegisteredCallback(messageId);
        double age = 0;
        if (cb != null)
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Processing response on a callback from " + message.getMessageId() + "@" + message.getFrom());
            age = System.currentTimeMillis() - MessagingService.getRegisteredCallbackAge(messageId);
            cb.response(message);
        }
        else
        {
            IAsyncResult ar = MessagingService.getAsyncResult(messageId);
            if (ar != null)
            {
                if (logger_.isDebugEnabled())
                    logger_.debug("Processing response on an async result from " + message.getMessageId() + "@" + message.getFrom());
                age = System.currentTimeMillis() - MessagingService.getAsyncResultAge(messageId);
                ar.result(message);
            }
        }
        notifySubscribers(message.getFrom(), age);
    }

    private void notifySubscribers(InetAddress host, double latency)
    {
        for (ILatencySubscriber subscriber : subscribers)
        {
            subscriber.receiveTiming(host, latency);
        }
    }

    public void register(ILatencySubscriber subscriber)
    {
        subscribers.add(subscriber);
    }
}
