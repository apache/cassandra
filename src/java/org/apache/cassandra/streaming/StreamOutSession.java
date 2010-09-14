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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.SimpleCondition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * This class manages the streaming of multiple files one after the other.
*/
public class StreamOutSession
{   
    private static Logger logger = LoggerFactory.getLogger( StreamOutSession.class );
        
    // one host may have multiple stream contexts (think of them as sessions). each context gets its own manager.
    private static ConcurrentMap<StreamContext, StreamOutSession> streamManagers = new ConcurrentHashMap<StreamContext, StreamOutSession>();
    public static final Multimap<InetAddress, StreamContext> destHosts = Multimaps.synchronizedMultimap(HashMultimap.<InetAddress, StreamContext>create());

    public static StreamOutSession get(StreamContext context)
    {
        StreamOutSession session = streamManagers.get(context);
        if (session == null)
        {
            StreamOutSession possibleNew = new StreamOutSession(context);
            if ((session = streamManagers.putIfAbsent(context, possibleNew)) == null)
            {
                session = possibleNew;
                destHosts.put(context.host, context);
            }
        }
        return session;
    }
    
    public static void remove(StreamContext context)
    {
        if (streamManagers.containsKey(context) && streamManagers.get(context).files.size() == 0)
        {
            streamManagers.remove(context);
            destHosts.remove(context.host, context);
        }
    }

    public static Set<InetAddress> getDestinations()
    {
        // the results of streamManagers.keySet() isn't serializable, so create a new set.
        Set<InetAddress> hosts = new HashSet<InetAddress>();
        hosts.addAll(destHosts.keySet());
        return hosts;
    }
    
    /** 
     * this method exists so that we don't have to call StreamOutManager.get() which has a nasty side-effect of 
     * indicating that we are streaming to a particular host.
     **/     
    public static List<PendingFile> getPendingFiles(StreamContext context)
    {
        List<PendingFile> list = new ArrayList<PendingFile>();
        StreamOutSession session = streamManagers.get(context);
        if (session != null)
            list.addAll(session.getFiles());
        return list;
    }

    public static List<PendingFile> getOutgoingFiles(InetAddress host)
    {
        List<PendingFile> list = new ArrayList<PendingFile>();
        for(StreamContext context : destHosts.get(host))
        {
            list.addAll(getPendingFiles(context));
        }
        return list;
    }

    // we need sequential and random access to the files. hence, the map and the list.
    private final List<PendingFile> files = new ArrayList<PendingFile>();
    private final Map<String, PendingFile> fileMap = new HashMap<String, PendingFile>();
    
    private final StreamContext context;
    private final SimpleCondition condition = new SimpleCondition();
    
    private StreamOutSession(StreamContext context)
    {
        this.context = context;
    }
    
    public void addFilesToStream(List<PendingFile> pendingFiles)
    {
        // reset the condition in case this SOM is getting reused before it can be removed.
        condition.reset();
        for (PendingFile pendingFile : pendingFiles)
        {
            if (logger.isDebugEnabled())
                logger.debug("Adding file {} to be streamed.", pendingFile.getFilename());
            files.add(pendingFile);
            fileMap.put(pendingFile.getFilename(), pendingFile);
        }
    }
    
    public void retry(String file)
    {
        PendingFile pf = fileMap.get(file);
        if (pf != null)
            streamFile(pf);
    }

    private void streamFile(PendingFile pf)
    {
        if (logger.isDebugEnabled())
            logger.debug("Streaming {} ...", pf);
        MessagingService.instance.stream(new StreamHeader(context.sessionId, pf, true), context.host);
    }

    public void finishAndStartNext(String pfname) throws IOException
    {
        PendingFile pf = fileMap.remove(pfname);
        files.remove(pf);

        if (files.size() > 0)
            streamFile(files.get(0));
        else
        {
            if (logger.isDebugEnabled())
                logger.debug("Signalling that streaming is done for {} session {}", context.host, context.sessionId);
            remove(context);
            condition.signalAll();
        }
    }

    public void removePending(PendingFile pf)
    {
        files.remove(pf);
        fileMap.remove(pf.getFilename());
        if (files.size() == 0)
            remove(context);
    }

    public void waitForStreamCompletion()
    {
        try
        {
            condition.await();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    List<PendingFile> getFiles()
    {
        return Collections.unmodifiableList(files);
    }
}
