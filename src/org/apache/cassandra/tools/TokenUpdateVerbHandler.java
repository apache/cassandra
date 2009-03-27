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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
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
import org.apache.cassandra.tools.TokenUpdater.TokenInfoMessage;
import org.apache.cassandra.utils.LogUtil;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TokenUpdateVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(TokenUpdateVerbHandler.class);

    public void doVerb(Message message)
    {
    	byte[] body = (byte[])message.getMessageBody()[0];
        
        try
        {
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);
            /* Deserialize to get the token for this endpoint. */
            TokenUpdater.TokenInfoMessage tiMessage = TokenUpdater.TokenInfoMessage.serializer().deserialize(bufIn);
            
            BigInteger token = tiMessage.getToken();
            logger_.info("Updating the token to [" + token + "]");
            StorageService.instance().updateToken(token);
            
            /* Get the headers for this message */
            Map<String, byte[]> headers = message.getHeaders();
            headers.remove( StorageService.getLocalStorageEndPoint().getHost() );
            logger_.debug("Number of nodes in the header " + headers.size());
            Set<String> nodes = headers.keySet();
            
            for ( String node : nodes )
            {            
                logger_.debug("Processing node " + node);
                byte[] bytes = headers.remove(node);
                /* Send a message to this node to update its token to the one retreived. */
                EndPoint target = new EndPoint(node, DatabaseDescriptor.getStoragePort());
                token = new BigInteger(bytes);
                
                /* Reset the new TokenInfoMessage */
                tiMessage = new TokenUpdater.TokenInfoMessage(target, token );
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos);
                TokenInfoMessage.serializer().serialize(tiMessage, dos);
                message.setMessageBody(new Object[]{bos.toByteArray()});
                
                logger_.debug("Sending a token update message to " + target + " to update it to " + token);
                MessagingService.getMessagingInstance().sendOneWay(message, target);
                break;
            }                        
        }
    	catch( IOException ex )
    	{
    		logger_.debug(LogUtil.throwableToString(ex));
    	}
    }

}
