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

import org.apache.cassandra.db.RowMutationVerbHandler.RowMutationContext;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;



/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class BinaryVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(BinaryVerbHandler.class);    
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<RowMutationContext> tls_ = new InheritableThreadLocal<RowMutationContext>();
    
    public void doVerb(Message message)
    { 
        byte[] bytes = (byte[])message.getMessageBody()[0];
        /* Obtain a Row Mutation Context from TLS */
        RowMutationContext rowMutationCtx = tls_.get();
        if ( rowMutationCtx == null )
        {
            rowMutationCtx = new RowMutationContext();
            tls_.set(rowMutationCtx);
        }                
        rowMutationCtx.buffer_.reset(bytes, bytes.length);
        
	    try
	    {
            RowMutationMessage rmMsg = RowMutationMessage.serializer().deserialize(rowMutationCtx.buffer_);
            RowMutation rm = rmMsg.getRowMutation();            	                
            rowMutationCtx.row_.key(rm.key());
            rm.load(rowMutationCtx.row_);
	
	    }        
	    catch ( Exception e )
	    {
	        logger_.debug(LogUtil.throwableToString(e));            
	    }        
    }

}
