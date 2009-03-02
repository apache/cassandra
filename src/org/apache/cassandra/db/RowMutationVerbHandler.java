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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.*;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.concurrent.*;
import org.apache.cassandra.net.*;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class RowMutationVerbHandler implements IVerbHandler
{
    protected static class RowMutationContext
    {
        protected Row row_ = new Row();
        protected DataInputBuffer buffer_ = new DataInputBuffer();
    }
    
    private static Logger logger_ = Logger.getLogger(RowMutationVerbHandler.class);     
    /* We use this so that we can reuse the same row mutation context for the mutation. */
    private static ThreadLocal<RowMutationContext> tls_ = new InheritableThreadLocal<RowMutationContext>();
    
    public void doVerb(Message message)
    {
        /* For DEBUG only. Printing queue length */                         
        logger_.info( "ROW MUTATION STAGE: " + StageManager.getStageTaskCount(StorageService.mutationStage_) );
        /* END DEBUG */
            
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
            /* Check if there were any hints in this message */
            byte[] hintedBytes = message.getHeader(RowMutationMessage.hint_);            
            if ( hintedBytes != null && hintedBytes.length > 0 )
            {
            	EndPoint hint = EndPoint.fromBytes(hintedBytes);
                /* add necessary hints to this mutation */
                try
                {
                	RowMutation hintedMutation = new RowMutation(rm.table(), HintedHandOffManager.key_);
                	hintedMutation.addHints(rm.key() + ":" + hint.getHost());
                	hintedMutation.apply();
                }
                catch ( ColumnFamilyNotDefinedException ex )
                {
                    logger_.debug(LogUtil.throwableToString(ex));
                }
            }
            
            long start = System.currentTimeMillis(); 
            
            rowMutationCtx.row_.key(rm.key());
            rm.apply(rowMutationCtx.row_);
            
            long end = System.currentTimeMillis();                       
            logger_.info("ROW MUTATION APPLY: " + (end - start) + " ms.");
            
            /*WriteResponseMessage writeResponseMessage = new WriteResponseMessage(rm.table(), rm.key(), true);
            Message response = message.getReply( StorageService.getLocalStorageEndPoint(), new Object[]{writeResponseMessage} );
            logger_.debug("Sending teh response to " +  message.getFrom() + " for key :" + rm.key());
            MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());  */                    
        }         
        catch( ColumnFamilyNotDefinedException ex )
        {
            logger_.debug(LogUtil.throwableToString(ex));
        }        
        catch ( IOException e )
        {
            logger_.debug(LogUtil.throwableToString(e));            
        }        
    }
}
