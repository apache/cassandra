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
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SimpleCondition;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This class manages the streaming of multiple files one after the other.
*/
public class StreamOutSession
{   
    private static final Logger logger = LoggerFactory.getLogger( StreamOutSession.class );
        
    // one host may have multiple stream sessions.
    private static final ConcurrentMap<Pair<InetAddress, Long>, StreamOutSession> streams = new NonBlockingHashMap<Pair<InetAddress, Long>, StreamOutSession>();

    public static StreamOutSession create(InetAddress host)
    {
        return create(host, System.nanoTime());
    }

    public static StreamOutSession create(InetAddress host, long sessionId)
    {
        Pair<InetAddress, Long> context = new Pair<InetAddress, Long>(host, sessionId);
        StreamOutSession session = new StreamOutSession(context);
        streams.put(context, session);
        return session;
    }

    public static StreamOutSession get(InetAddress host, long sessionId)
    {
        return streams.get(new Pair<InetAddress, Long>(host, sessionId));
    }

    public void close()
    {
        streams.remove(context);
    }

    // we need sequential and random access to the files. hence, the map and the list.
    private final List<PendingFile> files = new ArrayList<PendingFile>();
    private final Map<String, PendingFile> fileMap = new HashMap<String, PendingFile>();
    
    private final Pair<InetAddress, Long> context;
    private final SimpleCondition condition = new SimpleCondition();
    
    private StreamOutSession(Pair<InetAddress, Long> context)
    {
        this.context = context;
    }

    public InetAddress getHost()
    {
        return context.left;
    }

    public long getSessionId()
    {
        return context.right;
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
        MessagingService.instance.stream(new StreamHeader(getSessionId(), pf, true), getHost());
    }

    public void finishAndStartNext(String pfname) throws IOException
    {
        PendingFile pf = fileMap.remove(pfname);
        files.remove(pf);

        if (files.isEmpty())
        {
            if (logger.isDebugEnabled())
                logger.debug("Signalling that streaming is done for {} session {}", getHost(), getSessionId());
            close();
            condition.signalAll();
        }
        else
        {
            streamFile(files.get(0));
        }
    }

    public void removePending(PendingFile pf)
    {
        files.remove(pf);
        fileMap.remove(pf.getFilename());
        if (files.isEmpty())
            close();
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

    public static Set<InetAddress> getDestinations()
    {
        Set<InetAddress> hosts = new HashSet<InetAddress>();
        for (StreamOutSession session : streams.values())
        {
            hosts.add(session.getHost());
        }
        return hosts;
    }

    public static List<PendingFile> getOutgoingFiles(InetAddress host)
    {
        List<PendingFile> list = new ArrayList<PendingFile>();
        for (Map.Entry<Pair<InetAddress, Long>, StreamOutSession> entry : streams.entrySet())
        {
            if (entry.getKey().left.equals(host))
                list.addAll(entry.getValue().getFiles());
        }
        return list;
    }
}
