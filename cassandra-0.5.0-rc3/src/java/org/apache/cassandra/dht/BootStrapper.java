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
 import java.net.InetAddress;

 import org.apache.log4j.Logger;

 import org.apache.commons.lang.ArrayUtils;
 import org.apache.commons.lang.StringUtils;

 import org.apache.cassandra.locator.TokenMetadata;
 import org.apache.cassandra.locator.AbstractReplicationStrategy;
 import org.apache.cassandra.net.*;
 import org.apache.cassandra.service.StorageService;
 import org.apache.cassandra.utils.SimpleCondition;
 import org.apache.cassandra.utils.FBUtilities;
 import org.apache.cassandra.config.DatabaseDescriptor;
 import org.apache.cassandra.gms.FailureDetector;
 import org.apache.cassandra.gms.IFailureDetector;
 import org.apache.cassandra.io.Streaming;
 import com.google.common.collect.Multimap;
 import com.google.common.collect.ArrayListMultimap;


 /**
  * This class handles the bootstrapping responsibilities for the local endpoint.
  *
  *  - bootstrapTokenVerb asks the most-loaded node what Token to use to split its Range in two.
  *  - streamRequestVerb tells source nodes to send us the necessary Ranges
  *  - source nodes send streamInitiateVerb to us to say "get ready to receive data" [if there is data to send]
  *  - when we have everything set up to receive the data, we send streamInitiateDoneVerb back to the source nodes and they start streaming
  *  - when streaming is complete, we send streamFinishedVerb to the source so it can clean up on its end
  */
public class BootStrapper
{
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
                    logger.debug("Beginning bootstrap process");
                /* Send messages to respective folks to stream data over to me */
                for (Map.Entry<InetAddress, Collection<Range>> entry : getWorkMap(rangesWithSourceTarget).asMap().entrySet())
                {
                    InetAddress source = entry.getKey();
                    StorageService.instance().addBootstrapSource(source);
                    if (logger.isDebugEnabled())
                        logger.debug("Requesting from " + source + " ranges " + StringUtils.join(entry.getValue(), ", "));
                    Streaming.requestRanges(source, entry.getValue());
                }
            }
        }).start();
    }

    /**
     * if initialtoken was specified, use that.
     * otherwise, pick a token to assume half the load of the most-loaded node.
     */
    public static Token getBootstrapToken(final TokenMetadata metadata, final Map<InetAddress, Double> load) throws IOException
    {
        if (DatabaseDescriptor.getInitialToken() != null)
        {
            logger.debug("token manually specified as " + DatabaseDescriptor.getInitialToken());
            return StorageService.getPartitioner().getTokenFactory().fromString(DatabaseDescriptor.getInitialToken());
        }

        return getBalancedToken(metadata, load);
    }

    public static Token getBalancedToken(TokenMetadata metadata, Map<InetAddress, Double> load)
    {
        InetAddress maxEndpoint = getBootstrapSource(metadata, load);
        Token<?> t = getBootstrapTokenFrom(maxEndpoint);
        logger.info("New token will be " + t + " to assume load from " + maxEndpoint);
        return t;
    }

    static InetAddress getBootstrapSource(final TokenMetadata metadata, final Map<InetAddress, Double> load)
    {
        // sort first by number of nodes already bootstrapping into a source node's range, then by load.
        List<InetAddress> endpoints = new ArrayList<InetAddress>(load.size());
        for (InetAddress endpoint : load.keySet())
        {
            if (!metadata.isMember(endpoint))
                continue;
            endpoints.add(endpoint);
        }

        if (endpoints.isEmpty())
            throw new RuntimeException("No other nodes seen!  Unable to bootstrap");
        Collections.sort(endpoints, new Comparator<InetAddress>()
        {
            public int compare(InetAddress ia1, InetAddress ia2)
            {
                int n1 = metadata.pendingRangeChanges(ia1);
                int n2 = metadata.pendingRangeChanges(ia2);
                if (n1 != n2)
                    return -(n1 - n2); // more targets = _less_ priority!

                double load1 = load.get(ia1);
                double load2 = load.get(ia2);
                if (load1 == load2)
                    return 0;
                return load1 < load2 ? -1 : 1;
            }
        });

        InetAddress maxEndpoint = endpoints.get(endpoints.size() - 1);
        assert !maxEndpoint.equals(FBUtilities.getLocalAddress());
        return maxEndpoint;
    }

    /** get potential sources for each range, ordered by proximity (as determined by EndPointSnitch) */
    Multimap<Range, InetAddress> getRangesWithSources()
    {
        assert tokenMetadata.sortedTokens().size() > 0;
        Collection<Range> myRanges = replicationStrategy.getPendingAddressRanges(tokenMetadata, token, address);

        Multimap<Range, InetAddress> myRangeAddresses = ArrayListMultimap.create();
        Multimap<Range, InetAddress> rangeAddresses = replicationStrategy.getRangeAddresses(tokenMetadata);
        for (Range range : rangeAddresses.keySet())
        {
            for (Range myRange : myRanges)
            {
                if (range.contains(myRange))
                {
                    List<InetAddress> preferred = DatabaseDescriptor.getEndPointSnitch().getSortedListByProximity(address, rangeAddresses.get(range));
                    myRangeAddresses.putAll(myRange, preferred);
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
}
