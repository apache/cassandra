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

package org.apache.cassandra.db;

import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class LoadVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(LoadVerbHandler.class);    
    
    public void doVerb(Message message)
    { 
        try
        {
	        byte[] body = message.getMessageBody();
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(body, body.length);
	        RowMutationMessage rmMsg = RowMutationMessage.serializer().deserialize(buffer);

            EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(rmMsg.getRowMutation().key());

			Message messageInternal = new Message(StorageService.getLocalStorageEndPoint(), 
	                StorageService.mutationStage_,
					StorageService.mutationVerbHandler_, 
	                body
	        );
            
            StringBuilder sb = new StringBuilder();
			for(EndPoint endPoint : endpoints)
			{                
                sb.append(endPoint);
				MessagingService.getMessagingInstance().sendOneWay(messageInternal, endPoint);
			}
            if (logger_.isDebugEnabled())
                logger_.debug("Sent data to " + sb.toString());
        }        
        catch ( Exception e )
        {
            if (logger_.isDebugEnabled())
                logger_.debug(LogUtil.throwableToString(e));            
        }        
    }

}
