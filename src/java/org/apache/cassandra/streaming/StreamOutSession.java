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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This class manages the streaming of multiple files one after the other.
*/
public class StreamOutSession implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger( StreamOutSession.class );

    // one host may have multiple stream sessions.
    private static final ConcurrentMap<Pair<InetAddress, Long>, StreamOutSession> streams = new NonBlockingHashMap<Pair<InetAddress, Long>, StreamOutSession>();

    public static StreamOutSession create(String table, InetAddress host, Runnable callback)
    {
        return create(table, host, System.nanoTime(), callback);
    }

    public static StreamOutSession create(String table, InetAddress host, long sessionId)
    {
        return create(table, host, sessionId, null);
    }

    public static StreamOutSession create(String table, InetAddress host, long sessionId, Runnable callback)
    {
        Pair<InetAddress, Long> context = new Pair<InetAddress, Long>(host, sessionId);
        StreamOutSession session = new StreamOutSession(table, context, callback);
        streams.put(context, session);
        return session;
    }

    public static StreamOutSession get(InetAddress host, long sessionId)
    {
        return streams.get(new Pair<InetAddress, Long>(host, sessionId));
    }

    private final Map<String, PendingFile> files = new NonBlockingHashMap<String, PendingFile>();

    public final String table;
    private final Pair<InetAddress, Long> context;
    private final Runnable callback;
    private volatile String currentFile;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private StreamOutSession(String table, Pair<InetAddress, Long> context, Runnable callback)
    {
        this.table = table;
        this.context = context;
        this.callback = callback;
        Gossiper.instance.register(this);
        FailureDetector.instance.registerFailureDetectionEventListener(this);
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

    public void startNext() throws IOException
    {
        assert files.containsKey(currentFile);
        files.get(currentFile).sstable.releaseReference();
        files.remove(currentFile);
        Iterator<PendingFile> iter = files.values().iterator();
        if (iter.hasNext())
            streamFile(iter.next());
    }

    public void close()
    {
        close(true);
    }

    private void close(boolean success)
    {
        // Though unlikely, it is possible for close to be called multiple
        // time, if the endpoint die at the exact wrong time for instance.
        if (!isClosed.compareAndSet(false, true))
        {
            logger.debug("StreamOutSession {} already closed", getSessionId());
            return;
        }

        Gossiper.instance.unregister(this);
        FailureDetector.instance.unregisterFailureDetectionEventListener(this);

        // Release reference on last file (or any uncompleted ones)
        for (PendingFile file : files.values())
            file.sstable.releaseReference();
        streams.remove(context);
        // Instead of just not calling the callback on failure, we could have
        // allow to register a specific callback for failures, but we leave
        // that to a future ticket (likely CASSANDRA-3112)
        if (callback != null && success)
            callback.run();
    }

    /** convenience method for use when testing */
    void await() throws InterruptedException
    {
        while (streams.containsKey(context))
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
        for (Map.Entry<Pair<InetAddress, Long>, StreamOutSession> entry : streams.entrySet())
        {
            if (entry.getKey().left.equals(host))
                list.addAll(entry.getValue().getFiles());
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

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        if (!endpoint.equals(getHost()))
            return;

        // We want a higher confidence in the failure detection than usual because failing a streaming wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        logger.error("StreamOutSession {} failed because {} died or was restarted/removed", endpoint);
        close(false);
    }
}
