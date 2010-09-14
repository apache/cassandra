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

import java.net.InetAddress;
import java.util.*;
import java.io.IOException;
import java.io.IOError;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

/**
 * This class handles streaming data from one node to another.
 *
 * For StreamingRepair and Unbootstrap
 *  1. The ranges are transferred on a single file basis.
 *  2. Each transfer has the header information for the sstable being transferred.
 *  3. List of the pending files are maintained, as this is the source node.
 *
 * For StreamRequests
 *  1. The ranges are compiled and the first file transferred.
 *  2. The header contains the first file info + all the remaining pending files info.
 *  3. List of the pending files are not maintained, that will be maintained by the destination node
 *
 */
public class StreamOut
{
    private static Logger logger = LoggerFactory.getLogger(StreamOut.class);

    static String TABLE_NAME = "STREAMING-TABLE-NAME";

    /**
     * Split out files for all tables on disk locally for each range and then stream them to the target endpoint.
    */
    public static void transferRanges(InetAddress target, String tableName, Collection<Range> ranges, Runnable callback)
    {
        assert ranges.size() > 0;
        
        StreamContext context = new StreamContext(target);
        // this is so that this target shows up as a destination while anticompaction is happening.
        StreamOutSession.get(context);

        logger.info("Beginning transfer process to {} for ranges {}", context, StringUtils.join(ranges, ", "));

        try
        {
            Table table = flushSSTable(tableName);
            // send the matching portion of every sstable in the keyspace
            transferSSTables(context, tableName, table.getAllSSTables(), ranges);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            StreamOutSession.remove(context);
        }
        if (callback != null)
            callback.run();
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
        for (Future f : table.flush())
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
    public static void transferRangesForRequest(StreamContext context, String tableName, Collection<Range> ranges, Runnable callback)
    {
        assert ranges.size() > 0;

        logger.info("Beginning transfer process to {} for ranges {}", context, StringUtils.join(ranges, ", "));

        try
        {
            Table table = flushSSTable(tableName);
            // send the matching portion of every sstable in the keyspace
            transferSSTablesForRequest(context, tableName, table.getAllSSTables(), ranges);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        if (callback != null)
            callback.run();
    }

    /**
     * Transfers matching portions of a group of sstables from a single table to the target endpoint.
     */
    public static void transferSSTables(StreamContext context, String table, Collection<SSTableReader> sstables, Collection<Range> ranges) throws IOException
    {
        List<PendingFile> pending = createPendingFiles(sstables, ranges);

        if (pending.size() > 0)
        {
            StreamHeader header = new StreamHeader(context.sessionId, pending.get(0), true);
            StreamOutSession.get(context).addFilesToStream(pending);

            logger.info("Streaming file {} to {}", header.getStreamFile(), context.host);
            MessagingService.instance.stream(header, context.host);

            logger.info("Waiting for transfer to {} to complete", context);
            StreamOutSession.get(context).waitForStreamCompletion();
            logger.info("Done with transfer to {}", context);
        }
    }

    /**
     * Transfers the first file for matching portions of a group of sstables and appends a list of other files
     * to the header for the requesting destination to take control of the rest of the transfers
     */
    private static void transferSSTablesForRequest(StreamContext context, String table, Collection<SSTableReader> sstables, Collection<Range> ranges) throws IOException
    {
        List<PendingFile> pending = createPendingFiles(sstables, ranges);
        if (pending.size() > 0)
        {
            StreamHeader header = new StreamHeader(context.sessionId, pending.get(0), pending, false);
            // In case this happens to be a re-request due to some error condition on the destination side
            if (StreamOutSession.getPendingFiles(context).size() == 0)
                StreamOutSession.get(context).addFilesToStream(pending);

            logger.info("Streaming file {} to {}", header.getStreamFile(), context.host);
            MessagingService.instance.stream(header, context.host);
            StreamOutSession.get(context).removePending(header.getStreamFile());
        }
        else
        {
            FileStatus status = new FileStatus("", context.sessionId);
            status.setAction(FileStatus.Action.EMPTY);
            Message message = status.makeStreamStatusMessage();
            message.setHeader(StreamOut.TABLE_NAME, table.getBytes());
            MessagingService.instance.sendOneWay(message, context.host);
        }
    }

    // called prior to sending anything.
    private static List<PendingFile> createPendingFiles(Collection<SSTableReader> sstables, Collection<Range> ranges)
    {
        List<PendingFile> pending = new ArrayList<PendingFile>();
        for (SSTableReader sstable : sstables)
        {
            Descriptor desc = sstable.getDescriptor();
            List<Pair<Long,Long>> sections = sstable.getPositionsForRanges(ranges);
            if (sections.isEmpty())
                continue;
            pending.add(new PendingFile(desc, SSTable.COMPONENT_DATA, sections));
        }
        logger.info("Stream context metadata {}, {} sstables.", pending, sstables.size());
        return pending;
    }
}
