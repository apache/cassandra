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

import java.io.*;
import java.net.SocketException;

import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

class FileStreamTask implements Runnable
{
    private static Logger logger_ = Logger.getLogger( FileStreamTask.class );
    
    private String file_;
    private long startPosition_;
    private long total_;
    private EndPoint from_;
    private EndPoint to_;
    
    FileStreamTask(String file, long startPosition, long total, EndPoint from, EndPoint to)
    {
        file_ = file;
        startPosition_ = startPosition;
        total_ = total;
        from_ = from;
        to_ = to;
    }
    
    public void run()
    {
        TcpConnection connection = null;
        try
        {                        
            connection = new TcpConnection(from_, to_);
            File file = new File(file_);             
            connection.stream(file, startPosition_, total_);
            MessagingService.setStreamingMode(false);
            logger_.debug("Done streaming " + file);
        }            
        catch ( SocketException se )
        {                        
            logger_.info(LogUtil.throwableToString(se));
        }
        catch ( IOException e )
        {
            logConnectAndIOException(e, connection);
        }
        catch (Throwable th)
        {
            logger_.warn(LogUtil.throwableToString(th));
        }        
    }
    
    private void logConnectAndIOException(IOException ex, TcpConnection connection)
    {                    
        if ( connection != null )
        {
            connection.errorClose();
        }
        logger_.info(LogUtil.throwableToString(ex));
    }
}
