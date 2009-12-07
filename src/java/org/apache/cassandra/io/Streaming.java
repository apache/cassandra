package org.apache.cassandra.io;
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

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.StreamInitiateMessage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.net.io.StreamContextManager;
import org.apache.cassandra.net.io.IStreamComplete;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.service.StreamManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LogUtil;

public class Streaming
{
    private static Logger logger = Logger.getLogger(Streaming.class);
    public static final long RING_DELAY = 30 * 1000; // delay after which we assume ring has stablized

    /**
     * split out files on disk locally for each range and then stream them to the target endpoint
    */
    public static void transferRanges(InetAddress target, Collection<Range> ranges, Runnable callback)
    {
        assert ranges.size() > 0;

        if (logger.isDebugEnabled())
            logger.debug("Beginning transfer process to " + target + " for ranges " + StringUtils.join(ranges, ", "));

        /*
         * (1) dump all the memtables to disk.
         * (2) anticompaction -- split out the keys in the range specified
         * (3) transfer the data.
        */
        List<String> tables = DatabaseDescriptor.getTables();
        for (String tName : tables)
        {
            try
            {
                Table table = Table.open(tName);
                if (logger.isDebugEnabled())
                  logger.debug("Flushing memtables ...");
                table.flush();
                if (logger.isDebugEnabled())
                  logger.debug("Performing anticompaction ...");
                /* Get the list of files that need to be streamed */
                transferOneTable(target, table.forceAntiCompaction(ranges, target), tName); // SSTR GC deletes the file when done
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        if (callback != null)
            callback.run();
    }

    private static void transferOneTable(InetAddress target, List<SSTableReader> sstables, String table) throws IOException
    {
        StreamContextManager.StreamContext[] streamContexts = new StreamContextManager.StreamContext[SSTable.FILES_ON_DISK * sstables.size()];
        int i = 0;
        for (SSTableReader sstable : sstables)
        {
            for (String filename : sstable.getAllFilenames())
            {
                File file = new File(filename);
                streamContexts[i++] = new StreamContextManager.StreamContext(file.getAbsolutePath(), file.length(), table);
            }
        }
        if (logger.isDebugEnabled())
          logger.debug("Stream context metadata " + StringUtils.join(streamContexts, ", "));

        StreamManager.instance(target).addFilesToStream(streamContexts);
        StreamInitiateMessage biMessage = new StreamInitiateMessage(streamContexts);
        Message message = StreamInitiateMessage.makeStreamInitiateMessage(biMessage);
        if (logger.isDebugEnabled())
          logger.debug("Sending a stream initiate message to " + target + " ...");
        MessagingService.instance().sendOneWay(message, target);

        if (streamContexts.length > 0)
        {
            logger.info("Waiting for transfer to " + target + " to complete");
            StreamManager.instance(target).waitForStreamCompletion();
            for (SSTableReader sstable : sstables)
            {
                sstable.markCompacted();
            }
            logger.info("Done with transfer to " + target);
        }
    }

    public static class StreamInitiateVerbHandler implements IVerbHandler
    {
        /*
         * Here we handle the StreamInitiateMessage. Here we get the
         * array of StreamContexts. We get file names for the column
         * families associated with the files and replace them with the
         * file names as obtained from the column family store on the
         * receiving end.
        */
        public void doVerb(Message message)
        {
            byte[] body = message.getMessageBody();
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);

            try
            {
                StreamInitiateMessage biMsg = StreamInitiateMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamContext[] streamContexts = biMsg.getStreamContext();

                if (streamContexts.length == 0 && StorageService.instance().isBootstrapMode())
                {
                    if (logger.isDebugEnabled())
                        logger.debug("no data needed from " + message.getFrom());
                    StorageService.instance().removeBootstrapSource(message.getFrom());
                    return;
                }

                Map<String, String> fileNames = getNewNames(streamContexts);
                /*
                 * For each of stream context's in the incoming message
                 * generate the new file names and store the new file names
                 * in the StreamContextManager.
                */
                for (StreamContextManager.StreamContext streamContext : streamContexts )
                {
                    StreamContextManager.StreamStatus streamStatus = new StreamContextManager.StreamStatus(streamContext.getTargetFile(), streamContext.getExpectedBytes() );
                    String file = getNewFileNameFromOldContextAndNames(fileNames, streamContext);

                    if (logger.isDebugEnabled())
                      logger.debug("Received Data from  : " + message.getFrom() + " " + streamContext.getTargetFile() + " " + file);
                    streamContext.setTargetFile(file);
                    addStreamContext(message.getFrom(), streamContext, streamStatus);
                }

                StreamContextManager.registerStreamCompletionHandler(message.getFrom(), new StreamCompletionHandler());
                if (logger.isDebugEnabled())
                  logger.debug("Sending a stream initiate done message ...");
                Message doneMessage = new Message(FBUtilities.getLocalAddress(), "", StorageService.streamInitiateDoneVerbHandler_, new byte[0] );
                MessagingService.instance().sendOneWay(doneMessage, message.getFrom());
            }
            catch (IOException ex)
            {
                throw new IOError(ex);
            }
        }

        public String getNewFileNameFromOldContextAndNames(Map<String, String> fileNames,
                StreamContextManager.StreamContext streamContext)
        {
            File sourceFile = new File( streamContext.getTargetFile() );
            String[] piece = FBUtilities.strip(sourceFile.getName(), "-");
            String cfName = piece[0];
            String ssTableNum = piece[1];
            String typeOfFile = piece[2];

            String newFileNameExpanded = fileNames.get( streamContext.getTable() + "-" + cfName + "-" + ssTableNum );
            //Drop type (Data.db) from new FileName
            String newFileName = newFileNameExpanded.replace("Data.db", typeOfFile);
            return DatabaseDescriptor.getDataFileLocationForTable(streamContext.getTable()) + File.separator + newFileName;
        }

        public Map<String, String> getNewNames(StreamContextManager.StreamContext[] streamContexts) throws IOException
        {
            /*
             * Mapping for each file with unique CF-i ---> new file name. For eg.
             * for a file with name <CF>-<i>-Data.db there is a corresponding
             * <CF>-<i>-Index.db. We maintain a mapping from <CF>-<i> to a newly
             * generated file name.
            */
            Map<String, String> fileNames = new HashMap<String, String>();
            /* Get the distinct entries from StreamContexts i.e have one entry per Data/Index/Filter file set */
            Set<String> distinctEntries = new HashSet<String>();
            for ( StreamContextManager.StreamContext streamContext : streamContexts )
            {
                String[] pieces = FBUtilities.strip(new File(streamContext.getTargetFile()).getName(), "-");
                distinctEntries.add(streamContext.getTable() + "-" + pieces[0] + "-" + pieces[1] );
            }

            /* Generate unique file names per entry */
            for ( String distinctEntry : distinctEntries )
            {
                String tableName;
                String[] pieces = FBUtilities.strip(distinctEntry, "-");
                tableName = pieces[0];
                Table table = Table.open( tableName );

                ColumnFamilyStore cfStore = table.getColumnFamilyStore(pieces[1]);
                if (logger.isDebugEnabled())
                  logger.debug("Generating file name for " + distinctEntry + " ...");
                fileNames.put(distinctEntry, cfStore.getTempSSTableFileName());
            }

            return fileNames;
        }

        private void addStreamContext(InetAddress host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus)
        {
            if (logger.isDebugEnabled())
              logger.debug("Adding stream context " + streamContext + " for " + host + " ...");
            StreamContextManager.addStreamContext(host, streamContext, streamStatus);
        }
    }

    public static class StreamInitiateDoneVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message)
        {
            if (logger.isDebugEnabled())
              logger.debug("Received a stream initiate done message ...");
            StreamManager.instance(message.getFrom()).start();
        }
    }

    /**
     * This is the callback handler that is invoked when we have
     * completely received a single file from a remote host.
     *
     * TODO if we move this into CFS we could make addSSTables private, improving encapsulation.
    */
    private static class StreamCompletionHandler implements IStreamComplete
    {
        public void onStreamCompletion(InetAddress host, StreamContextManager.StreamContext streamContext, StreamContextManager.StreamStatus streamStatus) throws IOException
        {
            /* Parse the stream context and the file to the list of SSTables in the associated Column Family Store. */
            if (streamContext.getTargetFile().contains("-Data.db"))
            {
                String tableName = streamContext.getTable();
                File file = new File( streamContext.getTargetFile() );
                String fileName = file.getName();
                String [] temp = fileName.split("-");

                //Open the file to see if all parts are now here
                try
                {
                    SSTableReader sstable = SSTableWriter.renameAndOpen(streamContext.getTargetFile());
                    //TODO add a sanity check that this sstable has all its parts and is ok
                    Table.open(tableName).getColumnFamilyStore(temp[0]).addSSTable(sstable);
                    logger.info("Streaming added " + sstable.getFilename());
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Not able to add streamed file " + streamContext.getTargetFile(), e);
                }
            }

            if (logger.isDebugEnabled())
              logger.debug("Sending a streaming finished message with " + streamStatus + " to " + host);
            /* Send a StreamStatusMessage object which may require the source node to re-stream certain files. */
            StreamContextManager.StreamStatusMessage streamStatusMessage = new StreamContextManager.StreamStatusMessage(streamStatus);
            Message message = StreamContextManager.StreamStatusMessage.makeStreamStatusMessage(streamStatusMessage);
            MessagingService.instance().sendOneWay(message, host);

            /* If we're done with everything for this host, remove from bootstrap sources */
            if (StreamContextManager.isDone(host) && StorageService.instance().isBootstrapMode())
            {
                StorageService.instance().removeBootstrapSource(host);
            }
        }
    }

    public static class StreamFinishedVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message)
        {
            byte[] body = message.getMessageBody();
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);

            try
            {
                StreamContextManager.StreamStatusMessage streamStatusMessage = StreamContextManager.StreamStatusMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamStatus streamStatus = streamStatusMessage.getStreamStatus();

                switch (streamStatus.getAction())
                {
                    case DELETE:
                        StreamManager.instance(message.getFrom()).finish(streamStatus.getFile());
                        break;

                    case STREAM:
                        if (logger.isDebugEnabled())
                            logger.debug("Need to re-stream file " + streamStatus.getFile());
                        StreamManager.instance(message.getFrom()).repeat();
                        break;

                    default:
                        break;
                }
            }
            catch (IOException ex)
            {
                throw new IOError(ex);
            }
        }
    }
}
