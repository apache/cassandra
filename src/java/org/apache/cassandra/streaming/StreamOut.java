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
import java.io.File;
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
 * For bootstrap,
 *  1. BOOTSTRAP_TOKEN asks the most-loaded node what Token to use to split its Range in two.
 *  2. STREAM_REQUEST tells source nodes to send us the necessary Ranges
 *  3. source nodes send STREAM_INITIATE to us to say "get ready to receive data" [if there is data to send]
 *  4. when we have everything set up to receive the data, we send STREAM_INITIATE_DONE back to the source nodes and they start streaming
 *  5. when streaming is complete, we send STREAM_FINISHED to the source so it can clean up on its end
 *
 * For unbootstrap, the leaving node starts with step 3 (1 and 2 are skipped entirely).  This is why
 * STREAM_INITIATE is a separate verb, rather than just a reply to STREAM_REQUEST; the REQUEST is optional.
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
        
        // this is so that this target shows up as a destination while anticompaction is happening.
        StreamOutManager.pendingDestinations.add(target);

        logger.info("Beginning transfer process to " + target + " for ranges " + StringUtils.join(ranges, ", "));

        /*
         * (1) dump all the memtables to disk.
         * (2) determine the minimal file sections we need to send for the given ranges
         * (3) transfer the data.
        */
        try
        {
            Table table = Table.open(tableName);
            logger.info("Flushing memtables for " + tableName + "...");
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
            // send the matching portion of every sstable in the keyspace
            transferSSTables(target, tableName, table.getAllSSTables(), ranges);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            StreamOutManager.remove(target);
        }
        if (callback != null)
            callback.run();
    }

    /**
     * Transfers matching portions of a group of sstables from a single table to the target endpoint.
     */
    public static void transferSSTables(InetAddress target, String table, Collection<SSTableReader> sstables, Collection<Range> ranges) throws IOException
    {
        List<PendingFile> pending = new ArrayList<PendingFile>();
        int i = 0;
        for (SSTableReader sstable : sstables)
        {
            Descriptor desc = sstable.getDescriptor();
            List<Pair<Long,Long>> sections = sstable.getPositionsForRanges(ranges);
            if (sections.isEmpty())
                continue;
            pending.add(new PendingFile(desc, SSTable.COMPONENT_DATA, sections));
        }
        logger.info("Stream context metadata " + pending + " " + sstables.size() + " sstables.");

        PendingFile[] pendingFiles = pending.toArray(new PendingFile[pending.size()]);
        StreamOutManager.get(target).addFilesToStream(pendingFiles);
        StreamInitiateMessage biMessage = new StreamInitiateMessage(pendingFiles);
        Message message = StreamInitiateMessage.makeStreamInitiateMessage(biMessage);
        message.setHeader(StreamOut.TABLE_NAME, table.getBytes());
        logger.info("Sending a stream initiate message to " + target + " ...");
        MessagingService.instance.sendOneWay(message, target);

        if (pendingFiles.length > 0)
        {
            logger.info("Waiting for transfer to " + target + " to complete");
            StreamOutManager.get(target).waitForStreamCompletion();
            // todo: it would be good if there were a dafe way to remove the StreamManager for target.
            // (StreamManager will delete the streamed file on completion.)
            logger.info("Done with transfer to " + target);
        }
    }
}
