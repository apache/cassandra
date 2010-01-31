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

package org.apache.cassandra.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.net.InetAddress;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.streaming.IStreamComplete;

import org.apache.log4j.Logger;

public class StreamInManager
{
    private static final Logger logger = Logger.getLogger(StreamInManager.class);

    /* Maintain a stream context per host that is the source of the stream */
    public static final Map<InetAddress, List<InitiatedFile>> ctxBag_ = new Hashtable<InetAddress, List<InitiatedFile>>();
    /* Maintain in this map the status of the streams that need to be sent back to the source */
    public static final Map<InetAddress, List<CompletedFileStatus>> streamStatusBag_ = new Hashtable<InetAddress, List<CompletedFileStatus>>();
    /* Maintains a callback handler per endpoint to notify the app that a stream from a given endpoint has been handled */
    public static final Map<InetAddress, IStreamComplete> streamNotificationHandlers_ = new HashMap<InetAddress, IStreamComplete>();
    
    public synchronized static InitiatedFile getStreamContext(InetAddress key)
    {        
        List<InitiatedFile> context = ctxBag_.get(key);
        if ( context == null )
            throw new IllegalStateException("Streaming context has not been set for " + key);
        InitiatedFile initiatedFile = context.remove(0);
        if ( context.isEmpty() )
            ctxBag_.remove(key);
        return initiatedFile;
    }
    
    public synchronized static CompletedFileStatus getStreamStatus(InetAddress key)
    {
        List<CompletedFileStatus> status = streamStatusBag_.get(key);
        if ( status == null )
            throw new IllegalStateException("Streaming status has not been set for " + key);
        CompletedFileStatus streamStatus = status.remove(0);
        if ( status.isEmpty() )
            streamStatusBag_.remove(key);
        return streamStatus;
    }
    
    /*
     * This method helps determine if the StreamCompletionHandler needs
     * to be invoked for the data being streamed from a source. 
    */
    public synchronized static boolean isDone(InetAddress key)
    {
        return (ctxBag_.get(key) == null);
    }
    
    public synchronized static IStreamComplete getStreamCompletionHandler(InetAddress key)
    {
        return streamNotificationHandlers_.get(key);
    }
    
    public synchronized static void removeStreamCompletionHandler(InetAddress key)
    {
        streamNotificationHandlers_.remove(key);
    }
    
    public synchronized static void registerStreamCompletionHandler(InetAddress key, IStreamComplete streamComplete)
    {
        streamNotificationHandlers_.put(key, streamComplete);
    }
    
    public synchronized static void addStreamContext(InetAddress key, InitiatedFile initiatedFile, CompletedFileStatus streamStatus)
    {
        /* Record the stream context */
        List<InitiatedFile> context = ctxBag_.get(key);
        if ( context == null )
        {
            context = new ArrayList<InitiatedFile>();
            ctxBag_.put(key, context);
        }
        context.add(initiatedFile);
        
        /* Record the stream status for this stream context */
        List<CompletedFileStatus> status = streamStatusBag_.get(key);
        if ( status == null )
        {
            status = new ArrayList<CompletedFileStatus>();
            streamStatusBag_.put(key, status);
        }
        status.add( streamStatus );
    }        
}
