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

import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class ResponseVerbHandler implements IVerbHandler
{
    private static final Logger logger_ = Logger.getLogger( ResponseVerbHandler.class );
    
    public void doVerb(Message message)
    {     
        String messageId = message.getMessageId();        
        IAsyncCallback cb = MessagingService.getRegisteredCallback(messageId);
        if ( cb != null )
        {
            logger_.info("Processing response on a callback from " + message.getFrom());
            cb.response(message);
        }
        else
        {            
            AsyncResult ar = (AsyncResult)MessagingService.getAsyncResult(messageId);
            if ( ar != null )
            {
                logger_.info("Processing response on an async result from " + message.getFrom());
                ar.result(message.getMessageBody());
            }
        }
    }
}
