/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.streaming;

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * This class handles streaming data from one node to another.
 *
 * The source node [the Out side] is always in charge of the streaming session.  Streams may
 * be initiated either directly by the source via the methods in this class,
 * or on demand from the target (via StreamRequest).
 *
 * Files to stream are grouped into sessions, which can have callbacks associated
 * with them so that (for instance) we can mark a new node a full member of the
 * cluster after all the data it needs has been streamed.
 *
 * The source begins a session by sending
 * a Message with the stream bit flag in the Header turned on.  Part of that Message
 * will include a StreamHeader that includes the files that will be streamed as part
 * of that session, as well as the first file-to-be-streamed. (Combining session list
 * and first file like this is inconvenient, but not as inconvenient as the old
 * three-part send-file-list, wait-for-ack, start-first-file dance.)
 *
 * This is done over a separate TCP connection to avoid blocking ordinary intra-node
 * traffic during the stream.  So there is no Handler for the main stream of data --
 * when a connection sets the Stream bit, IncomingTcpConnection knows what to expect
 * without any further Messages.
 *
 * After each file, the target node [the In side] will send a StreamReply indicating success
 * (FILE_FINISHED) or failure (FILE_RETRY).
 *
 * When all files have been successfully transferred and integrated the target will
 * send an additional SESSION_FINISHED reply and the session is complete.
 *
 * For Stream requests (for bootstrap), one subtlety is that we always have to
 * create at least one stream reply, even if the list of files is empty, otherwise the
 * target has no way to know that it can stop waiting for an answer.
 *
 */
public class StreamOut
{
    private static Logger logger = LoggerFactory.getLogger(StreamOut.class);

    /**
     * Split out files for all tables on disk locally for each range and then stream them to the target endpoint.
    */
    public static void transferRanges(InetAddress target, String tableName, Collection<Range> ranges, Runnable callback, OperationType type)
    {
        assert ranges.size() > 0;
        
        // this is so that this target shows up as a destination while anticompaction is happening.
        StreamOutSession session = StreamOutSession.create(tableName, target, callback);

        logger.info("Beginning transfer to {}", target);
        logger.debug("Ranges are {}", StringUtils.join(ranges, ","));

        try
        {
            Table table = flushSSTable(tableName);
            // send the matching portion of every sstable in the keyspace
            transferSSTables(session, table.getAllSSTables(), ranges, type);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * (1) dump all the memtables to disk.
     * (2) determine the minimal file sections we need to send for the given ranges
     * (3) transfer the data.
     */
    private static Table flushSSTable(String tableName) throws IOException
    {
        Table table = Table.open(tableName);
        logger.info("Flushing memtables for {}...", tableName);
        for (Future<?> f : table.flush())
        {
            try
            {
                f.get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
        return table;
    }

    /**
     * Split out files for all tables on disk locally for each range and then stream them to the target endpoint.
    */
    public static void transferRangesForRequest(StreamOutSession session, Collection<Range> ranges, OperationType type)
    {
        assert ranges.size() > 0;

        logger.info("Beginning transfer to {}", session.getHost());
        logger.debug("Ranges are {}", StringUtils.join(ranges, ","));

        try
        {
            Table table = flushSSTable(session.table);
            // send the matching portion of every sstable in the keyspace
            List<PendingFile> pending = createPendingFiles(table.getAllSSTables(), ranges, type);
            session.addFilesToStream(pending);
            session.begin();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Transfers matching portions of a group of sstables from a single table to the target endpoint.
     */
    public static void transferSSTables(StreamOutSession session, Collection<SSTableReader> sstables, Collection<Range> ranges, OperationType type) throws IOException
    {
        List<PendingFile> pending = createPendingFiles(sstables, ranges, type);

        // Even if the list of pending files is empty, we need to initiate the transfer otherwise
        // the remote end will hang in cases where this was a requested transfer.
        session.addFilesToStream(pending);
        session.begin();
    }

    // called prior to sending anything.
    private static List<PendingFile> createPendingFiles(Collection<SSTableReader> sstables, Collection<Range> ranges, OperationType type)
    {
        List<PendingFile> pending = new ArrayList<PendingFile>();
        for (SSTableReader sstable : sstables)
        {
            Descriptor desc = sstable.descriptor;
            List<Pair<Long,Long>> sections = sstable.getPositionsForRanges(ranges);
            if (sections.isEmpty())
                continue;
            pending.add(new PendingFile(sstable, desc, SSTable.COMPONENT_DATA, sections, type));
        }
        logger.info("Stream context metadata {}, {} sstables.", pending, sstables.size());
        return pending;
    }
}
