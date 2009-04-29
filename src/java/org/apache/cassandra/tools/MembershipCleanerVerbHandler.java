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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class MembershipCleanerVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(MembershipCleanerVerbHandler.class);

    public void doVerb(Message message)
    {
        byte[] body = (byte[])message.getMessageBody()[0];
        
        try
        {
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            /* Deserialize to get the token for this endpoint. */
            MembershipCleaner.MembershipCleanerMessage mcMessage = MembershipCleaner.MembershipCleanerMessage.serializer().deserialize(bufIn);
            
            String target = mcMessage.getTarget();
            logger_.info("Removing the node [" + target + "] from membership");
            EndPoint targetEndPoint = new EndPoint(target, DatabaseDescriptor.getControlPort());
            /* Remove the token related information for this endpoint */
            StorageService.instance().removeTokenState(targetEndPoint);
            
            /* Get the headers for this message */
            Map<String, byte[]> headers = message.getHeaders();
            headers.remove( StorageService.getLocalStorageEndPoint().getHost() );
            logger_.debug("Number of nodes in the header " + headers.size());
            Set<String> nodes = headers.keySet();
            
            for ( String node : nodes )
            {            
                logger_.debug("Processing node " + node);
                byte[] bytes = headers.remove(node);
                /* Send a message to this node to alter its membership state. */
                EndPoint targetNode = new EndPoint(node, DatabaseDescriptor.getStoragePort());                
                
                logger_.debug("Sending a membership clean message to " + targetNode);
                MessagingService.getMessagingInstance().sendOneWay(message, targetNode);
                break;
            }                        
        }
        catch( IOException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }
    }

}
