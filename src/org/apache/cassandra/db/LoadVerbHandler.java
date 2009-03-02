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
	        Object[] body = message.getMessageBody();
	        RowMutationMessage rmMsg = (RowMutationMessage)body[0];
	        RowMutation rm = rmMsg.getRowMutation();
	
			EndPoint[] endpoints = StorageService.instance().getNStorageEndPoint(rm.key());
	
			Message messageInternal = new Message(StorageService.getLocalStorageEndPoint(), 
	                StorageService.mutationStage_,
					StorageService.mutationVerbHandler_, 
	                new Object[]{ rmMsg }
	        );
            
            StringBuilder sb = new StringBuilder();
			for(EndPoint endPoint : endpoints)
			{                
                sb.append(endPoint);
				MessagingService.getMessagingInstance().sendOneWay(messageInternal, endPoint);
			}
            logger_.debug("Sent data to " + sb.toString());            
        }        
        catch ( Exception e )
        {
            logger_.debug(LogUtil.throwableToString(e));            
        }        
    }

}
