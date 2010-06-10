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

import java.util.*;
import java.net.InetAddress;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.cassandra.streaming.FileStatusHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamInManager
{
    private static final Logger logger = LoggerFactory.getLogger(StreamInManager.class);

    /* Maintain a stream context per host that is the source of the stream */
    public static final Map<InetAddress, List<PendingFile>> ctxBag_ = new Hashtable<InetAddress, List<PendingFile>>();
    /* Maintain in this map the status of the streams that need to be sent back to the source */
    public static final Map<InetAddress, List<FileStatus>> streamStatusBag_ = new Hashtable<InetAddress, List<FileStatus>>();
    /* Maintains a callback handler per endpoint to notify the app that a stream from a given endpoint has been handled */
    public static final Map<InetAddress, FileStatusHandler> streamNotificationHandlers_ = new HashMap<InetAddress, FileStatusHandler>();

    public static final Multimap<InetAddress, PendingFile> activeStreams = Multimaps.synchronizedMultimap(HashMultimap.<InetAddress, PendingFile>create());

    /**
     * gets the next file to be received given a host key.
     * @param key
     * @return next file to receive.
     * @throws IndexOutOfBoundsException if you are unfortunate enough to call this on an empty context. 
     */
    public synchronized static PendingFile getNextIncomingFile(InetAddress key)
    {        
        List<PendingFile> context = ctxBag_.get(key);
        if ( context == null )
            throw new IllegalStateException("Streaming context has not been set for " + key);
        // will thrown an IndexOutOfBoundsException if nothing is there.
        PendingFile pendingFile = context.remove(0);
        if ( context.isEmpty() )
            ctxBag_.remove(key);
        return pendingFile;
    }
    
    public synchronized static FileStatus getStreamStatus(InetAddress key)
    {
        List<FileStatus> status = streamStatusBag_.get(key);
        if ( status == null )
            throw new IllegalStateException("Streaming status has not been set for " + key);
        FileStatus streamStatus = status.remove(0);
        if ( status.isEmpty() )
            streamStatusBag_.remove(key);
        return streamStatus;
    }

    /** query method to determine which hosts are streaming to this node. */
    public static Set<InetAddress> getSources()
    {
        HashSet<InetAddress> set = new HashSet<InetAddress>();
        set.addAll(ctxBag_.keySet());
        set.addAll(activeStreams.keySet());
        return set;
    }

    /** query the status of incoming files. */
    public static List<PendingFile> getIncomingFiles(InetAddress host)
    {
        // avoid returning null.
        List<PendingFile> list = new ArrayList<PendingFile>();
        if (ctxBag_.containsKey(host))
            list.addAll(ctxBag_.get(host));
        list.addAll(activeStreams.get(host));
        return list;
    }

    /*
     * This method helps determine if the FileStatusHandler needs
     * to be invoked for the data being streamed from a source. 
    */
    public synchronized static boolean isDone(InetAddress key)
    {
        return (ctxBag_.get(key) == null);
    }
    
    public synchronized static FileStatusHandler getFileStatusHandler(InetAddress key)
    {
        return streamNotificationHandlers_.get(key);
    }
    
    public synchronized static void removeFileStatusHandler(InetAddress key)
    {
        streamNotificationHandlers_.remove(key);
    }
    
    public synchronized static void registerFileStatusHandler(InetAddress key, FileStatusHandler streamComplete)
    {
        streamNotificationHandlers_.put(key, streamComplete);
    }
    
    public synchronized static void addStreamContext(InetAddress key, PendingFile pendingFile, FileStatus streamStatus)
    {
        /* Record the stream context */
        List<PendingFile> context = ctxBag_.get(key);
        if ( context == null )
        {
            context = new ArrayList<PendingFile>();
            ctxBag_.put(key, context);
        }
        context.add(pendingFile);
        
        /* Record the stream status for this stream context */
        List<FileStatus> status = streamStatusBag_.get(key);
        if ( status == null )
        {
            status = new ArrayList<FileStatus>();
            streamStatusBag_.put(key, status);
        }
        status.add( streamStatus );
    }
}
