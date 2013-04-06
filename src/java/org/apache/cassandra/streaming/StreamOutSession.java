/*
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
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This class manages the streaming of multiple files one after the other.
 */
public class StreamOutSession extends AbstractStreamSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamOutSession.class);

    // one host may have multiple stream sessions.
    private static final ConcurrentMap<UUID, StreamOutSession> streams = new NonBlockingHashMap<UUID, StreamOutSession>();

    public static StreamOutSession create(String table, InetAddress host, IStreamCallback callback)
    {
        return create(table, host, UUIDGen.getTimeUUID(), callback);
    }

    public static StreamOutSession create(String table, InetAddress host, UUID sessionId)
    {
        return create(table, host, sessionId, null);
    }

    public static StreamOutSession create(String table, InetAddress host, UUID sessionId, IStreamCallback callback)
    {
        StreamOutSession session = new StreamOutSession(table, host, sessionId, callback);
        streams.put(sessionId, session);
        return session;
    }

    public static StreamOutSession get(UUID sessionId)
    {
        return streams.get(sessionId);
    }

    private final Map<String, PendingFile> files = new NonBlockingHashMap<String, PendingFile>();

    private volatile String currentFile;

    private StreamOutSession(String table, InetAddress host, UUID sessionId, IStreamCallback callback)
    {
        super(table, host, sessionId, callback);
    }

    public void addFilesToStream(List<PendingFile> pendingFiles)
    {
        for (PendingFile pendingFile : pendingFiles)
        {
            if (logger.isDebugEnabled())
                logger.debug("Adding file {} to be streamed.", pendingFile.getFilename());
            files.put(pendingFile.getFilename(), pendingFile);
        }
    }

    public void retry()
    {
        streamFile(files.get(currentFile));
    }

    private void streamFile(PendingFile pf)
    {
        if (logger.isDebugEnabled())
            logger.debug("Streaming {} ...", pf);
        currentFile = pf.getFilename();
        MessagingService.instance().stream(new StreamHeader(table, getSessionId(), pf), getHost());
    }

    public void startNext()
    {
        assert files.containsKey(currentFile);
        files.get(currentFile).sstable.releaseReference();
        files.remove(currentFile);
        Iterator<PendingFile> iter = files.values().iterator();
        if (iter.hasNext())
            streamFile(iter.next());
    }

    protected void closeInternal(boolean success)
    {
        // Release reference on last file (or any uncompleted ones)
        for (PendingFile file : files.values())
            file.sstable.releaseReference();
        streams.remove(sessionId);
    }

    /** convenience method for use when testing */
    void await() throws InterruptedException
    {
        while (streams.containsKey(sessionId))
            Thread.sleep(10);
    }

    public Collection<PendingFile> getFiles()
    {
        return files.values();
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
        for (Map.Entry<UUID, StreamOutSession> entry : streams.entrySet())
        {
            StreamOutSession session = entry.getValue();
            if (session.getHost().equals(host))
                list.addAll(session.getFiles());
        }
        return list;
    }

    public void validateCurrentFile(String file)
    {
        if (!file.equals(currentFile))
            throw new IllegalStateException(String.format("target reports current file is %s but is %s", file, currentFile));
    }

    public void begin()
    {
        PendingFile first = files.isEmpty() ? null : files.values().iterator().next();
        currentFile = first == null ? null : first.getFilename();
        StreamHeader header = new StreamHeader(table, getSessionId(), first, files.values());
        logger.info("Streaming to {}", getHost());
        logger.debug("Files are {}", StringUtils.join(files.values(), ","));
        MessagingService.instance().stream(header, getHost());
    }
}
