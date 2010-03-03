package org.apache.cassandra.streaming;
/*
 * 
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
 * 
 */


import java.net.InetAddress;
import java.util.*;
import java.io.IOException;
import java.io.File;
import java.io.IOError;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.streaming.StreamInitiateMessage;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamOutManager;

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
    private static Logger logger = Logger.getLogger(StreamOut.class);

    static String TABLE_NAME = "STREAMING-TABLE-NAME";

    /**
     * Split out files for all tables on disk locally for each range and then stream them to the target endpoint.
    */
    public static void transferRanges(InetAddress target, String tableName, Collection<Range> ranges, Runnable callback)
    {
        assert ranges.size() > 0;

        if (logger.isDebugEnabled())
            logger.debug("Beginning transfer process to " + target + " for ranges " + StringUtils.join(ranges, ", "));

        /*
         * (1) dump all the memtables to disk.
         * (2) anticompaction -- split out the keys in the range specified
         * (3) transfer the data.
        */
        try
        {
            Table table = Table.open(tableName);
            if (logger.isDebugEnabled())
                logger.debug("Flushing memtables ...");
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
            if (logger.isDebugEnabled())
                logger.debug("Performing anticompaction ...");
            /* Get the list of files that need to be streamed */
            transferSSTables(target, table.forceAntiCompaction(ranges, target), tableName); // SSTR GC deletes the file when done
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        if (callback != null)
            callback.run();
    }

    /**
     * Transfers a group of sstables from a single table to the target endpoint
     * and then marks them as ready for local deletion.
     */
    public static void transferSSTables(InetAddress target, List<SSTableReader> sstables, String table) throws IOException
    {
        PendingFile[] pendingFiles = new PendingFile[SSTable.FILES_ON_DISK * sstables.size()];
        int i = 0;
        for (SSTableReader sstable : sstables)
        {
            for (String component : sstable.getAllComponents())
            {
                SSTable.Descriptor desc = sstable.getDescriptor();
                long filelen = new File(desc.filenameFor(component)).length();
                pendingFiles[i++] = new PendingFile(desc, component, filelen);
            }
        }
        if (logger.isDebugEnabled())
          logger.debug("Stream context metadata " + StringUtils.join(pendingFiles, ", " + " " + sstables.size() + " sstables."));

        StreamOutManager.get(target).addFilesToStream(pendingFiles);
        StreamInitiateMessage biMessage = new StreamInitiateMessage(pendingFiles);
        Message message = StreamInitiateMessage.makeStreamInitiateMessage(biMessage);
        message.setHeader(StreamOut.TABLE_NAME, table.getBytes());
        if (logger.isDebugEnabled())
          logger.debug("Sending a stream initiate message to " + target + " ...");
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
