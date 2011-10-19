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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** each context gets its own StreamInSession. So there may be >1 Session per host */
public class StreamInSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamInSession.class);

    private static ConcurrentMap<Pair<InetAddress, Long>, StreamInSession> sessions = new NonBlockingHashMap<Pair<InetAddress, Long>, StreamInSession>();

    private final Set<PendingFile> files = new NonBlockingHashSet<PendingFile>();
    private final Pair<InetAddress, Long> context;
    private final Runnable callback;
    private String table;
    private final List<SSTableReader> readers = new ArrayList<SSTableReader>();
    private PendingFile current;

    private StreamInSession(Pair<InetAddress, Long> context, Runnable callback)
    {
        this.context = context;
        this.callback = callback;
    }

    public static StreamInSession create(InetAddress host, Runnable callback)
    {
        Pair<InetAddress, Long> context = new Pair<InetAddress, Long>(host, System.nanoTime());
        StreamInSession session = new StreamInSession(context, callback);
        sessions.put(context, session);
        return session;
    }

    public static StreamInSession get(InetAddress host, long sessionId)
    {
        Pair<InetAddress, Long> context = new Pair<InetAddress, Long>(host, sessionId);
        StreamInSession session = sessions.get(context);
        if (session == null)
        {
            StreamInSession possibleNew = new StreamInSession(context, null);
            if ((session = sessions.putIfAbsent(context, possibleNew)) == null)
            {
                session = possibleNew;
            }
        }
        return session;
    }

    public void setCurrentFile(PendingFile file)
    {
        this.current = file;
    }

    public void setTable(String table)
    {
        this.table = table;
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
            logger.debug("Finished {}. Sending ack to {}", remoteFile, this);

        assert reader != null;
        readers.add(reader);
        files.remove(remoteFile);
        if (remoteFile.equals(current))
            current = null;
        StreamReply reply = new StreamReply(remoteFile.getFilename(), getSessionId(), StreamReply.Status.FILE_FINISHED);
        // send a StreamStatus message telling the source node it can delete this file
        MessagingService.instance().sendOneWay(reply.getMessage(Gossiper.instance.getVersion(getHost())), getHost());
    }

    public void retry(PendingFile remoteFile) throws IOException
    {
        StreamReply reply = new StreamReply(remoteFile.getFilename(), getSessionId(), StreamReply.Status.FILE_RETRY);
        logger.info("Streaming of file {} from {} failed: requesting a retry.", remoteFile, this);
        MessagingService.instance().sendOneWay(reply.getMessage(Gossiper.instance.getVersion(getHost())), getHost());
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
                        throw new AssertionError("We shouldn't fail acquiring a reference on a sstable that has just been transfered");

                    ColumnFamilyStore cfs = Table.open(sstable.getTableName()).getColumnFamilyStore(sstable.getColumnFamilyName());
                    cfs.addSSTable(sstable);
                    if (!cfstores.containsKey(cfs))
                        cfstores.put(cfs, new ArrayList<SSTableReader>());
                    cfstores.get(cfs).add(sstable);
                }

                // build secondary indexes
                for (Map.Entry<ColumnFamilyStore, List<SSTableReader>> entry : cfstores.entrySet())
                {
                    if (entry.getKey() != null)
                        entry.getKey().indexManager.maybeBuildSecondaryIndexes(entry.getValue(), entry.getKey().indexManager.getIndexedColumns());
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
            MessagingService.instance().sendOneWay(reply.getMessage(Gossiper.instance.getVersion(getHost())), getHost());

            if (callback != null)
                callback.run();
            sessions.remove(context);
        }
    }

    public long getSessionId()
    {
        return context.right;
    }

    public InetAddress getHost()
    {
        return context.left;
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
        for (Map.Entry<Pair<InetAddress, Long>, StreamInSession> entry : sessions.entrySet())
        {
            if (entry.getKey().left.equals(host))
            {
                StreamInSession session = entry.getValue();
                if (session.current != null)
                    set.add(session.current);
                set.addAll(session.files);
            }
        }
        return set;
    }
}
