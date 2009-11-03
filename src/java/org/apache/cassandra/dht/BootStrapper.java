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

package org.apache.cassandra.dht;

 import java.util.*;
 import java.util.concurrent.locks.Condition;
 import java.io.IOException;
 import java.io.UnsupportedEncodingException;
 import java.io.File;
 import java.net.InetAddress;

 import org.apache.log4j.Logger;

 import org.apache.commons.lang.ArrayUtils;

 import org.apache.cassandra.locator.TokenMetadata;
 import org.apache.cassandra.locator.AbstractReplicationStrategy;
 import org.apache.cassandra.net.*;
 import org.apache.cassandra.net.io.StreamContextManager;
 import org.apache.cassandra.net.io.IStreamComplete;
 import org.apache.cassandra.service.StorageService;
 import org.apache.cassandra.service.StorageLoadBalancer;
 import org.apache.cassandra.service.StreamManager;
 import org.apache.cassandra.utils.LogUtil;
 import org.apache.cassandra.utils.SimpleCondition;
 import org.apache.cassandra.utils.FBUtilities;
 import org.apache.cassandra.config.DatabaseDescriptor;
 import org.apache.cassandra.gms.FailureDetector;
 import org.apache.cassandra.gms.IFailureDetector;
 import org.apache.cassandra.io.DataInputBuffer;
 import org.apache.cassandra.io.SSTableReader;
 import org.apache.cassandra.io.SSTableWriter;
 import org.apache.cassandra.db.ColumnFamilyStore;
 import org.apache.cassandra.db.Table;
 import com.google.common.collect.Multimap;
 import com.google.common.collect.HashMultimap;
 import com.google.common.collect.ArrayListMultimap;


 /**
  * This class handles the bootstrapping responsibilities for the local endpoint.
  *
  *  - bootstrapTokenVerb asks the most-loaded node what Token to use to split its Range in two.
  *  - bootstrapMetadataVerb tells source nodes to send us the necessary Ranges
  *  - source nodes send bootStrapInitiateVerb to us to say "get ready to receive data" [if there is data to send]
  *  - when we have everything set up to receive the data, we send bootStrapInitiateDoneVerb back to the source nodes and they start streaming
  *  - when streaming is complete, we send bootStrapTerminateVerb to the source so it can clean up on its end
  */
public class BootStrapper
{
    public static final long INITIAL_DELAY = 30 * 1000; //ms

    private static final Logger logger = Logger.getLogger(BootStrapper.class);

    /* endpoints that need to be bootstrapped */
    protected final InetAddress address;
    /* tokens of the nodes being bootstrapped. */
    protected final Token token;
    protected final TokenMetadata tokenMetadata;
    private final AbstractReplicationStrategy replicationStrategy;

    public BootStrapper(AbstractReplicationStrategy rs, InetAddress address, Token token, TokenMetadata tmd)
    {
        assert address != null;
        assert token != null;

        replicationStrategy = rs;
        this.address = address;
        this.token = token;
        tokenMetadata = tmd;
    }
    
    public void startBootstrap() throws IOException
    {
        new Thread(new Runnable()
        {
            public void run()
            {
                Multimap<Range, InetAddress> rangesWithSourceTarget = getRangesWithSources();
                if (logger.isDebugEnabled())
                        logger.debug("Beginning bootstrap process for " + address + " ...");
                /* Send messages to respective folks to stream data over to me */
                for (Map.Entry<InetAddress, Collection<Range>> entry : getWorkMap(rangesWithSourceTarget).asMap().entrySet())
                {
                    InetAddress source = entry.getKey();
                    BootstrapMetadata bsMetadata = new BootstrapMetadata(address, entry.getValue());
                    Message message = BootstrapMetadataMessage.makeBootstrapMetadataMessage(new BootstrapMetadataMessage(bsMetadata));
                    if (logger.isDebugEnabled())
                        logger.debug("Sending the BootstrapMetadataMessage to " + source);
                    MessagingService.instance().sendOneWay(message, source);
                    StorageService.instance().addBootstrapSource(source);
                }
            }
        }).start();
    }

    public static void guessTokenIfNotSpecified() throws IOException
    {
        StorageService ss = StorageService.instance();
        StorageLoadBalancer slb = StorageLoadBalancer.instance();

        slb.waitForLoadInfo();

        // if initialtoken was specified, use that.  otherwise, pick a token to assume half the load of the most-loaded node.
        if (DatabaseDescriptor.getInitialToken() == null)
        {
            double maxLoad = 0;
            InetAddress maxEndpoint = null;
            for (Map.Entry<InetAddress,Double> entry : slb.getLoadInfo().entrySet())
            {
                if (maxEndpoint == null || entry.getValue() > maxLoad)
                {
                    maxEndpoint = entry.getKey();
                    maxLoad = entry.getValue();
                }
            }
            if (maxEndpoint == null)
            {
                throw new RuntimeException("No bootstrap sources found");
            }

            if (!maxEndpoint.equals(FBUtilities.getLocalAddress()))
            {
                Token<?> t = getBootstrapTokenFrom(maxEndpoint);
                logger.info("New token will be " + t + " to assume load from " + maxEndpoint);
                ss.setToken(t);
            }
        }
    }

    Multimap<Range, InetAddress> getRangesWithSources()
    {
        TokenMetadata temp = tokenMetadata.cloneMe();
        assert temp.sortedTokens().size() > 0;
        temp.update(token, address);
        Collection<Range> myRanges = replicationStrategy.getAddressRanges(temp).get(address);

        Multimap<Range, InetAddress> myRangeAddresses = HashMultimap.create();
        Multimap<Range, InetAddress> rangeAddresses = replicationStrategy.getRangeAddresses(tokenMetadata);
        for (Range range : rangeAddresses.keySet())
        {
            for (Range myRange : myRanges)
            {
                if (range.contains(myRange.right()))
                {
                    myRangeAddresses.putAll(myRange, rangeAddresses.get(range));
                    break;
                }
            }
        }
        return myRangeAddresses;
    }

    private static Token<?> getBootstrapTokenFrom(InetAddress maxEndpoint)
    {
        Message message = new Message(FBUtilities.getLocalAddress(), "", StorageService.bootstrapTokenVerbHandler_, ArrayUtils.EMPTY_BYTE_ARRAY);
        BootstrapTokenCallback btc = new BootstrapTokenCallback();
        MessagingService.instance().sendRR(message, maxEndpoint, btc);
        return btc.getToken();
    }

    static Multimap<InetAddress, Range> getWorkMap(Multimap<Range, InetAddress> rangesWithSourceTarget)
    {
        return getWorkMap(rangesWithSourceTarget, FailureDetector.instance());
    }

    static Multimap<InetAddress, Range> getWorkMap(Multimap<Range, InetAddress> rangesWithSourceTarget, IFailureDetector failureDetector)
    {
        /*
         * Map whose key is the source node and the value is a map whose key is the
         * target and value is the list of ranges to be sent to it.
        */
        Multimap<InetAddress, Range> sources = ArrayListMultimap.create();

        // TODO look for contiguous ranges and map them to the same source
        for (Range range : rangesWithSourceTarget.keySet())
        {
            for (InetAddress source : rangesWithSourceTarget.get(range))
            {
                if (failureDetector.isAlive(source))
                {
                    sources.put(source, range);
                    break;
                }
            }
        }
        return sources;
    }

    public static class BootstrapTokenVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message)
        {
            StorageService ss = StorageService.instance();
            List<String> tokens = ss.getSplits(2);
            assert tokens.size() == 3 : tokens.size();
            Message response;
            try
            {
                response = message.getReply(FBUtilities.getLocalAddress(), tokens.get(1).getBytes("UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new AssertionError();
            }
            MessagingService.instance().sendOneWay(response, message.getFrom());
        }
    }

    public static class BootstrapInitiateDoneVerbHandler implements IVerbHandler
    {
        public void doVerb(Message message)
        {
            if (logger.isDebugEnabled())
              logger.debug("Received a bootstrap initiate done message ...");
            /* Let the Stream Manager do his thing. */
            StreamManager.instance(message.getFrom()).start();
        }
    }

    private static class BootstrapTokenCallback implements IAsyncCallback
    {
        private volatile Token<?> token;
        private final Condition condition = new SimpleCondition();

        public Token<?> getToken()
        {
            try
            {
                condition.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return token;
        }

        public void response(Message msg)
        {
            try
            {
                token = StorageService.getPartitioner().getTokenFactory().fromString(new String(msg.getMessageBody(), "UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new AssertionError();
            }
            condition.signalAll();
        }
    }

    public static class BootStrapInitiateVerbHandler implements IVerbHandler
    {
        /*
         * Here we handle the BootstrapInitiateMessage. Here we get the
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
                BootstrapInitiateMessage biMsg = BootstrapInitiateMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamContext[] streamContexts = biMsg.getStreamContext();

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

                    //String file = DatabaseDescriptor.getDataFileLocationForTable(streamContext.getTable()) + File.separator + newFileName + "-Data.db";
                    if (logger.isDebugEnabled())
                      logger.debug("Received Data from  : " + message.getFrom() + " " + streamContext.getTargetFile() + " " + file);
                    streamContext.setTargetFile(file);
                    addStreamContext(message.getFrom(), streamContext, streamStatus);
                }

                StreamContextManager.registerStreamCompletionHandler(message.getFrom(), new BootstrapCompletionHandler());
                /* Send a bootstrap initiation done message to execute on default stage. */
                if (logger.isDebugEnabled())
                  logger.debug("Sending a bootstrap initiate done message ...");
                Message doneMessage = new Message(FBUtilities.getLocalAddress(), "", StorageService.bootStrapInitiateDoneVerbHandler_, new byte[0] );
                MessagingService.instance().sendOneWay(doneMessage, message.getFrom());
            }
            catch ( IOException ex )
            {
                logger.info(LogUtil.throwableToString(ex));
            }
        }

        String getNewFileNameFromOldContextAndNames(Map<String, String> fileNames,
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

        Map<String, String> getNewNames(StreamContextManager.StreamContext[] streamContexts) throws IOException
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

    /**
     * This is the callback handler that is invoked when we have
     * completely been bootstrapped for a single file by a remote host.
     *
     * TODO if we move this into CFS we could make addSSTables private, improving encapsulation.
    */
    private static class BootstrapCompletionHandler implements IStreamComplete
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
                    logger.info("Bootstrap added " + sstable.getFilename());
                }
                catch (IOException e)
                {
                    logger.error("Not able to bootstrap with file " + streamContext.getTargetFile(), e);
                }
            }

            if (logger.isDebugEnabled())
              logger.debug("Sending a bootstrap terminate message with " + streamStatus + " to " + host);
            /* Send a StreamStatusMessage object which may require the source node to re-stream certain files. */
            StreamContextManager.StreamStatusMessage streamStatusMessage = new StreamContextManager.StreamStatusMessage(streamStatus);
            Message message = StreamContextManager.StreamStatusMessage.makeStreamStatusMessage(streamStatusMessage);
            MessagingService.instance().sendOneWay(message, host);
            /* If we're done with everything for this host, remove from bootstrap sources */
            if (StreamContextManager.isDone(host))
                StorageService.instance().removeBootstrapSource(host);
        }
    }

    public static class BootstrapTerminateVerbHandler implements IVerbHandler
    {
        private static Logger logger_ = Logger.getLogger( BootstrapTerminateVerbHandler.class );

        public void doVerb(Message message)
        {
            byte[] body = message.getMessageBody();
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(body, body.length);

            try
            {
                StreamContextManager.StreamStatusMessage streamStatusMessage = StreamContextManager.StreamStatusMessage.serializer().deserialize(bufIn);
                StreamContextManager.StreamStatus streamStatus = streamStatusMessage.getStreamStatus();

                switch( streamStatus.getAction() )
                {
                    case DELETE:
                        StreamManager.instance(message.getFrom()).finish(streamStatus.getFile());
                        break;

                    case STREAM:
                        if (logger_.isDebugEnabled())
                          logger_.debug("Need to re-stream file " + streamStatus.getFile());
                        StreamManager.instance(message.getFrom()).repeat();
                        break;

                    default:
                        break;
                }
            }
            catch ( IOException ex )
            {
                logger_.info(LogUtil.throwableToString(ex));
            }
        }
    }
}
