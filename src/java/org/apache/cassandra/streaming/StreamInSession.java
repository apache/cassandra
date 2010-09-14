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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** each context gets its own StreamInSession. So there may be >1 StreamInManager per host */
public class StreamInSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamInSession.class);

    private static ConcurrentMap<StreamContext, StreamInSession> streamManagers = new ConcurrentHashMap<StreamContext, StreamInSession>(0);
    public static final Multimap<StreamContext, PendingFile> activeStreams = Multimaps.synchronizedMultimap(HashMultimap.<StreamContext, PendingFile>create());
    public static final Multimap<InetAddress, StreamContext> sourceHosts = Multimaps.synchronizedMultimap(HashMultimap.<InetAddress, StreamContext>create());
    
    private final List<PendingFile> pendingFiles = new ArrayList<PendingFile>();
    private final StreamContext context;

    private StreamInSession(StreamContext context)
    {
        this.context = context;
    }

    public synchronized static StreamInSession get(StreamContext context)
    {
        StreamInSession session = streamManagers.get(context);
        if (session == null)
        {
            StreamInSession possibleNew = new StreamInSession(context);
            if ((session = streamManagers.putIfAbsent(context, possibleNew)) == null)
            {
                session = possibleNew;
                sourceHosts.put(context.host, context);
            }
        }
        return session;
    }

    public void addFilesToRequest(List<PendingFile> pendingFiles)
    {
        for(PendingFile file : pendingFiles)
        {
            if(logger.isDebugEnabled())
                logger.debug("Adding file {} to Stream Request queue", file.getFilename());
            this.pendingFiles.add(file);
        }
    }

    /**
     * Complete the transfer process of the existing file and then request
     * the next file in the list
     */
    public void finishAndRequestNext(PendingFile lastFile)
    {
        pendingFiles.remove(lastFile);
        if (pendingFiles.size() > 0)
            StreamIn.requestFile(context, pendingFiles.get(0));
        else
        {
            if (StorageService.instance.isBootstrapMode())
                StorageService.instance.removeBootstrapSource(context.host, lastFile.desc.ksname);
            remove();
        }
    }
    
    public void remove()
    {
        if (streamManagers.containsKey(context))
            streamManagers.remove(context);
        sourceHosts.remove(context.host, context);
    }

    /** query method to determine which hosts are streaming to this node. */
    public static Set<StreamContext> getSources()
    {
        HashSet<StreamContext> set = new HashSet<StreamContext>();
        set.addAll(streamManagers.keySet());
        set.addAll(activeStreams.keySet());
        return set;
    }

    /** query the status of incoming files. */
    public static List<PendingFile> getIncomingFiles(InetAddress host)
    {
        // avoid returning null.
        List<PendingFile> list = new ArrayList<PendingFile>();
        for (StreamContext context : sourceHosts.get(host))
        {
            if (streamManagers.containsKey(context))
                list.addAll(streamManagers.get(context).pendingFiles);
            list.addAll(activeStreams.get(context));
        }
        return list;
    }
}
