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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.Pair;

public class ResponseVerbHandler implements IVerbHandler
{
    private static final Logger logger_ = LoggerFactory.getLogger( ResponseVerbHandler.class );

    public void doVerb(Message message, String id)
    {     
        double age = System.currentTimeMillis() - MessagingService.instance().getRegisteredCallbackAge(id);
        Pair<InetAddress, IMessageCallback> pair = MessagingService.instance().removeRegisteredCallback(id);
        if (pair == null)
        {
            logger_.debug("Callback already removed for {}", id);
            return;
        }

        IMessageCallback cb = pair.right;
        MessagingService.instance().maybeAddLatency(cb, message.getFrom(), age);

        if (cb instanceof IAsyncCallback)
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Processing response on a callback from " + id + "@" + message.getFrom());
            ((IAsyncCallback) cb).response(message);
        }
        else
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Processing response on an async result from " + id + "@" + message.getFrom());
            ((IAsyncResult) cb).result(message);
        }
    }
}
