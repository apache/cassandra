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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class LocationInfoVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger( LocationInfoVerbHandler.class );
    
    public void doVerb(Message message)
    {            
        EndPoint from = message.getFrom();
        logger_.info("Received a location download request from " + from);
        
        Object[] body = message.getMessageBody();
        byte[] bytes = (byte[])body[0];
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bytes, bytes.length);
        try
        {
            Range range = Range.serializer().deserialize(bufIn);
            /* Get the replicas for the given range */
            EndPoint[] replicas = StorageService.instance().getNStorageEndPoint(range.right());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            
            for ( EndPoint replica : replicas )
            {       
                replica.setPort(DatabaseDescriptor.getControlPort());
                if ( FailureDetector.instance().isAlive(replica) )
                {
                    replica.setPort(DatabaseDescriptor.getStoragePort());
                    CompactEndPointSerializationHelper.serialize(replica, dos);
                    break;
                }
            }
            
            Message response = message.getReply(StorageService.getLocalStorageEndPoint(), new Object[]{bos.toByteArray()});
            logger_.info("Sending the token download response to " + from);
            MessagingService.getMessagingInstance().sendOneWay(response, from);
        }
        catch (IOException ex)
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }        
    }
}
