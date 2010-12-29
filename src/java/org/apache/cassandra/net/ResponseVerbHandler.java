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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.ILatencyPublisher;
import org.apache.cassandra.locator.ILatencySubscriber;

public class ResponseVerbHandler implements IVerbHandler, ILatencyPublisher
{
    private static final Logger logger_ = LoggerFactory.getLogger( ResponseVerbHandler.class );
    private List<ILatencySubscriber>  subscribers = new ArrayList<ILatencySubscriber>();


    public void doVerb(Message message)
    {     
        String messageId = message.getMessageId();
        MessagingService.responseReceivedFrom(messageId, message.getFrom());
        double age = System.currentTimeMillis() - MessagingService.getRegisteredCallbackAge(messageId);
        IMessageCallback cb = MessagingService.getRegisteredCallback(messageId);
        if (cb == null)
            return;

        // if cb is not null, then age will be valid
        for (ILatencySubscriber subscriber : subscribers)
            subscriber.receiveTiming(message.getFrom(), age);

        if (cb instanceof IAsyncCallback)
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Processing response on a callback from " + message.getMessageId() + "@" + message.getFrom());
            ((IAsyncCallback) cb).response(message);
        }
        else
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Processing response on an async result from " + message.getMessageId() + "@" + message.getFrom());
            ((IAsyncResult) cb).result(message);
        }
    }

    public void register(ILatencySubscriber subscriber)
    {
        subscribers.add(subscriber);
    }
}
