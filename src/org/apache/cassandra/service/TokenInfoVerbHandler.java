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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TokenInfoVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( TokenInfoVerbHandler.class );
    
    public void doVerb(Message message)
    {
        EndPoint from = message.getFrom();
        logger_.info("Received a token download request from " + from);
        /* Get the token metadata map and send it over. */        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        try
        {
            TokenMetadata.serializer().serialize( StorageService.instance().getTokenMetadata(), dos );
            Message response = message.getReply(StorageService.getLocalStorageEndPoint(), new Object[]{bos.toByteArray()});
            logger_.info("Sending the token download response to " + from);
            MessagingService.getMessagingInstance().sendOneWay(response, from);
        }
        catch ( IOException ex )
        {
            logger_.info( LogUtil.throwableToString(ex) );
        }
    }
}
