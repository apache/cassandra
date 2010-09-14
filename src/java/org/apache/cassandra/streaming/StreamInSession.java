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

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** each context gets its own StreamInSession. So there may be >1 Session per host */
public class StreamInSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamInSession.class);

    private static ConcurrentMap<StreamContext, StreamInSession> sessions = new NonBlockingHashMap<StreamContext, StreamInSession>();
    private final Set<PendingFile> activeStreams = new HashSet<PendingFile>();

    private final List<PendingFile> pendingFiles = new ArrayList<PendingFile>();
    private final StreamContext context;

    private StreamInSession(StreamContext context)
    {
        this.context = context;
    }

    public static StreamInSession create(InetAddress host)
    {
        StreamContext context = new StreamContext(host);
        StreamInSession session = new StreamInSession(context);
        sessions.put(context, session);
        return session;
    }

    public static StreamInSession get(InetAddress host, long sessionId)
    {
        StreamContext context = new StreamContext(host, sessionId);

        StreamInSession session = sessions.get(context);
        if (session == null)
        {
            StreamInSession possibleNew = new StreamInSession(context);
            if ((session = sessions.putIfAbsent(context, possibleNew)) == null)
            {
                session = possibleNew;
            }
        }
        return session;
    }

    // FIXME hack for "initiated" streams.  replace w/ integration w/ pendingfiles
    public void addActiveStream(PendingFile file)
    {
        activeStreams.add(file);
    }

    public void removeActiveStream(PendingFile file)
    {
        activeStreams.remove(file);
    }

    public void addFilesToRequest(List<PendingFile> files)
    {
        for(PendingFile file : files)
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
            requestFile(pendingFiles.get(0));
        else
        {
            if (StorageService.instance.isBootstrapMode())
                StorageService.instance.removeBootstrapSource(getHost(), lastFile.desc.ksname);
            remove();
        }
    }
    
    public void remove()
    {
        sessions.remove(context);
    }

    public void requestFile(PendingFile file)
    {
        if (logger.isDebugEnabled())
            logger.debug("Requesting file {} from source {}", file.getFilename(), getHost());
        Message message = new StreamRequestMessage(FBUtilities.getLocalAddress(), file, getSessionId()).makeMessage();
        MessagingService.instance.sendOneWay(message, getHost());
    }

    public long getSessionId()
    {
        return context.sessionId;
    }

    public InetAddress getHost()
    {
        return context.host;
    }

    /** query method to determine which hosts are streaming to this node. */
    public static Set<InetAddress> getSources()
    {
        HashSet<InetAddress> set = new HashSet<InetAddress>();
        for (StreamInSession session : sessions.values())
        {
            set.add(session.getHost());
        }
        return set;
    }

    /** query the status of incoming files. */
    public static List<PendingFile> getIncomingFiles(InetAddress host)
    {
        List<PendingFile> list = new ArrayList<PendingFile>();
        for (Map.Entry<StreamContext, StreamInSession> entry : sessions.entrySet())
        {
            if (entry.getKey().host.equals(host))
            {
                StreamInSession session = entry.getValue();
                list.addAll(session.pendingFiles);
                list.addAll(session.activeStreams);
            }
        }
        return list;
    }
}
