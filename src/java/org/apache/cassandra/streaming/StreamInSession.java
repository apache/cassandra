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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundTcpConnection;
import org.apache.cassandra.utils.UUIDGen;

/** each context gets its own StreamInSession. So there may be >1 Session per host */
public class StreamInSession extends AbstractStreamSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamInSession.class);

    private static final ConcurrentMap<UUID, StreamInSession> sessions = new NonBlockingHashMap<UUID, StreamInSession>();

    private final Set<PendingFile> files = new NonBlockingHashSet<PendingFile>();
    private final List<SSTableReader> readers = new ArrayList<SSTableReader>();
    private PendingFile current;
    private Socket socket;
    private volatile int retries;

    private StreamInSession(InetAddress host, UUID sessionId, IStreamCallback callback)
    {
        super(null, host, sessionId, callback);
    }

    public static StreamInSession create(InetAddress host, IStreamCallback callback)
    {
        StreamInSession session = new StreamInSession(host, UUIDGen.getTimeUUID(), callback);
        sessions.put(session.getSessionId(), session);
        return session;
    }

    public static StreamInSession get(InetAddress host, UUID sessionId)
    {
        StreamInSession session = sessions.get(sessionId);
        if (session == null)
        {
            StreamInSession possibleNew = new StreamInSession(host, sessionId, null);
            if ((session = sessions.putIfAbsent(sessionId, possibleNew)) == null)
                session = possibleNew;
        }
        return session;
    }

    public static boolean hasSession(UUID sessionId)
    {
        return sessions.get(sessionId) != null;
    }

    public void setCurrentFile(PendingFile file)
    {
        this.current = file;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public void setSocket(Socket socket)
    {
        this.socket = socket;
    }

    public void addFiles(Collection<PendingFile> files)
    {
        for (PendingFile file : files)
        {
            if(logger.isDebugEnabled())
                logger.debug("Adding file {} to Stream Request queue", file.getFilename());
            this.files.add(file);
        }
    }

    public void finished(PendingFile remoteFile, SSTableReader reader) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Finished {} (from {}). Sending ack to {}", new Object[] {remoteFile, getHost(), this});

        assert reader != null;
        readers.add(reader);
        files.remove(remoteFile);
        if (remoteFile.equals(current))
            current = null;
        StreamReply reply = new StreamReply(remoteFile.getFilename(), getSessionId(), StreamReply.Status.FILE_FINISHED);
        // send a StreamStatus message telling the source node it can delete this file
        sendMessage(reply.createMessage());
        logger.debug("ack {} sent for {}", reply, remoteFile);
    }

    public void retry(PendingFile remoteFile)
    {
        retries++;
        if (retries > DatabaseDescriptor.getMaxStreamingRetries())
        {
            logger.error(String.format("Failed streaming session %s from %s while receiving %s", getSessionId(), getHost().toString(), current),
                         new IllegalStateException("Too many retries for " + remoteFile));
            close(false);
            return;
        }
        StreamReply reply = new StreamReply(remoteFile.getFilename(), getSessionId(), StreamReply.Status.FILE_RETRY);
        logger.info("Streaming of file {} for {} failed: requesting a retry.", remoteFile, this);
        try
        {
            sendMessage(reply.createMessage());
        }
        catch (IOException e)
        {
            logger.error("Sending retry message failed, closing session.", e);
            close(false);
        }
    }

    public void sendMessage(MessageOut<StreamReply> message) throws IOException
    {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        OutboundTcpConnection.write(message,
                                    String.valueOf(getSessionId()),
                                    System.currentTimeMillis(),
                                    out,
                                    MessagingService.instance().getVersion(getHost()));
        out.flush();
    }

    public void closeIfFinished() throws IOException
    {
        if (files.isEmpty())
        {
            HashMap <ColumnFamilyStore, List<SSTableReader>> cfstores = new HashMap<ColumnFamilyStore, List<SSTableReader>>();
            try
            {
                for (SSTableReader sstable : readers)
                {
                    assert sstable.getTableName().equals(table);

                    // Acquire the reference (for secondary index building) before submitting the index build,
                    // so it can't get compacted out of existence in between
                    if (!sstable.acquireReference())
                        throw new AssertionError("We shouldn't fail acquiring a reference on a sstable that has just been transferred");

                    ColumnFamilyStore cfs = Table.open(sstable.getTableName()).getColumnFamilyStore(sstable.getColumnFamilyName());
                    if (!cfstores.containsKey(cfs))
                        cfstores.put(cfs, new ArrayList<SSTableReader>());
                    cfstores.get(cfs).add(sstable);
                }

                // add sstables and build secondary indexes
                for (Map.Entry<ColumnFamilyStore, List<SSTableReader>> entry : cfstores.entrySet())
                {
                    if (entry.getKey() != null)
                    {
                        entry.getKey().addSSTables(entry.getValue());
                        entry.getKey().indexManager.maybeBuildSecondaryIndexes(entry.getValue(), entry.getKey().indexManager.allIndexesNames());
                    }
                }
            }
            finally
            {
                for (List<SSTableReader> referenced : cfstores.values())
                    SSTableReader.releaseReferences(referenced);
            }

            // send reply to source that we're done
            StreamReply reply = new StreamReply("", getSessionId(), StreamReply.Status.SESSION_FINISHED);
            logger.info("Finished streaming session {} from {}", getSessionId(), getHost());
            try
            {
                if (socket != null)
                    OutboundTcpConnection.write(reply.createMessage(),
                                                sessionId.toString(),
                                                System.currentTimeMillis(),
                                                new DataOutputStream(socket.getOutputStream()),
                                                MessagingService.instance().getVersion(getHost()));
                else
                    logger.debug("No socket to reply to {} with!", getHost());
            }
            finally
            {
                if (socket != null)
                    socket.close();
            }

            close(true);
        }
    }

    protected void closeInternal(boolean success)
    {
        sessions.remove(sessionId);
        if (!success && FailureDetector.instance.isAlive(getHost()))
        {
            StreamReply reply = new StreamReply("", getSessionId(), StreamReply.Status.SESSION_FAILURE);
            MessagingService.instance().sendOneWay(reply.createMessage(), getHost());
        }
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
    public static Set<PendingFile> getIncomingFiles(InetAddress host)
    {
        Set<PendingFile> set = new HashSet<PendingFile>();
        for (Map.Entry<UUID, StreamInSession> entry : sessions.entrySet())
        {
            StreamInSession session = entry.getValue();
            if (session.getHost().equals(host))
            {
                if (session.current != null)
                    set.add(session.current);
                set.addAll(session.files);
            }
        }
        return set;
    }
}
