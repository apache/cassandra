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
import java.net.SocketException;

import org.apache.cassandra.concurrent.Context;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.ThreadLocalContext;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class MessageSerializationTask implements Runnable
{
    private static Logger logger_ = Logger.getLogger(MessageSerializationTask.class);
    private Message message_;
    private EndPoint to_;    
    
    public MessageSerializationTask(Message message, EndPoint to)
    {
        message_ = message;
        to_ = to;        
    }
    
    public Message getMessage()
    {
        return message_;
    }

    public void run()
    {        
    	/* For DEBUG only. Printing queue length */   
    	DebuggableThreadPoolExecutor es = (DebuggableThreadPoolExecutor)MessagingService.getWriteExecutor();
        logger_.debug( "Message Serialization Task: " + (es.getTaskCount() - es.getCompletedTaskCount()) );
        /* END DEBUG */
        
        /* Adding the message to be serialized in the TLS. For accessing in the afterExecute() */
        Context ctx = new Context();
        ctx.put(this.getClass().getName(), message_);
        ThreadLocalContext.put(ctx);
        
        TcpConnection connection = null;
        try
        {
            Message message = SinkManager.processClientMessageSink(message_);
            if(null == message) 
                return;
            connection = MessagingService.getConnection(message_.getFrom(), to_);
            connection.write(message);            
        }            
        catch ( SocketException se )
        {            
            // Shutting down the entire pool. May be too conservative an approach.
            MessagingService.getConnectionPool(message_.getFrom(), to_).shutdown();
            logger_.warn(LogUtil.throwableToString(se));
        }
        catch ( IOException e )
        {
            logConnectAndIOException(e, connection);
        }
        catch (Throwable th)
        {
            logger_.warn(LogUtil.throwableToString(th));
        }
        finally
        {
            if ( connection != null )
            {
                connection.close();
            }            
        }
    }
    
    private void logConnectAndIOException(IOException ex, TcpConnection connection)
    {                    
        if ( connection != null )
        {
            connection.errorClose();
        }
        logger_.warn(LogUtil.throwableToString(ex));
    }
}

