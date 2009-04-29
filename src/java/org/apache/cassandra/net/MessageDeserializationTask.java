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

import java.io.IOException;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.net.io.FastSerializer;
import org.apache.cassandra.net.io.ISerializer;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class MessageDeserializationTask implements Runnable
{
    private static Logger logger_ = Logger.getLogger(MessageDeserializationTask.class); 
    private static ISerializer serializer_ = new FastSerializer();
    private int serializerType_;
    private byte[] bytes_ = new byte[0];    
    
    MessageDeserializationTask(int serializerType, byte[] bytes)
    {
        serializerType_ = serializerType;
        bytes_ = bytes;        
    }
    
    public void run()
    {
    	/* For DEBUG only. Printing queue length */   
    	DebuggableThreadPoolExecutor es = (DebuggableThreadPoolExecutor)MessagingService.getDeserilizationExecutor();
        logger_.debug( "Message Deserialization Task: " + (es.getTaskCount() - es.getCompletedTaskCount()) );
        /* END DEBUG */
        try
        {                        
            Message message = (Message)serializer_.deserialize(bytes_);                                                           
            
            if ( message != null )
            {
                message = SinkManager.processServerMessageSink(message);
                MessagingService.receive(message);                                                                                                    
            }
        }
        catch ( IOException ex )
        {            
            logger_.warn(LogUtil.throwableToString(ex));              
        }
    }

}
