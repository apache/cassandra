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
package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SimpleCondition;

public class BootStrapper
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddress address;
    /* token of the node being bootstrapped. */
    protected final Token<?> token;
    protected final TokenMetadata tokenMetadata;
    private static final long BOOTSTRAP_TIMEOUT = 30000; // default bootstrap timeout of 30s

    public BootStrapper(InetAddress address, Token token, TokenMetadata tmd)
    {
        assert address != null;
        assert token != null;

        this.address = address;
        this.token = token;
        tokenMetadata = tmd;
    }

    public void bootstrap() throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(tokenMetadata, address, OperationType.BOOTSTRAP);
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));

        for (String table : Schema.instance.getNonSystemTables())
        {
            AbstractReplicationStrategy strategy = Table.open(table).getReplicationStrategy();
            streamer.addRanges(table, strategy.getPendingAddressRanges(tokenMetadata, token, address));
        }

        streamer.fetch();
        StorageService.instance.finishBootstrapping();
    }

    /**
     * if initialtoken was specified, use that.
     * otherwise, pick a token to assume half the load of the most-loaded node.
     */
    public static Token getBootstrapToken(final TokenMetadata metadata, final Map<InetAddress, Double> load) throws IOException, ConfigurationException
    {
        if (DatabaseDescriptor.getInitialToken() != null)
        {
            logger.debug("token manually specified as " + DatabaseDescriptor.getInitialToken());
            Token token = StorageService.getPartitioner().getTokenFactory().fromString(DatabaseDescriptor.getInitialToken());
            if (metadata.getEndpoint(token) != null)
                throw new ConfigurationException("Bootstraping to existing token " + token + " is not allowed (decommission/removetoken the old node first).");
            return token;
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
            if (!metadata.isMember(endpoint) || !FailureDetector.instance.isAlive(endpoint))
                continue;
            endpoints.add(endpoint);
        }

        if (endpoints.isEmpty())
            throw new RuntimeException("No other nodes seen!  Unable to bootstrap."
                                       + "If you intended to start a single-node cluster, you should make sure "
                                       + "your broadcast_address (or listen_address) is listed as a seed.  "
                                       + "Otherwise, you need to determine why the seed being contacted "
                                       + "has no knowledge of the rest of the cluster.  Usually, this can be solved "
                                       + "by giving all nodes the same seed list.");
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
        assert !maxEndpoint.equals(FBUtilities.getBroadcastAddress());
        if (metadata.pendingRangeChanges(maxEndpoint) > 0)
            throw new RuntimeException("Every node is a bootstrap source! Please specify an initial token manually or wait for an existing bootstrap operation to finish.");

        return maxEndpoint;
    }

    static Token<?> getBootstrapTokenFrom(InetAddress maxEndpoint)
    {
        MessageOut message = new MessageOut(MessagingService.Verb.BOOTSTRAP_TOKEN);
        int retries = 5;
        long timeout = Math.max(DatabaseDescriptor.getRpcTimeout(), BOOTSTRAP_TIMEOUT);

        while (retries > 0)
        {
            BootstrapTokenCallback btc = new BootstrapTokenCallback();
            MessagingService.instance().sendRR(message, maxEndpoint, btc, timeout);
            Token token = btc.getToken(timeout);
            if (token != null)
                return token;

            retries--;
        }
        throw new RuntimeException("Bootstrap failed, could not obtain token from: " + maxEndpoint);
    }

    public static class BootstrapTokenVerbHandler implements IVerbHandler
    {
        public void doVerb(MessageIn message, String id)
        {
            StorageService ss = StorageService.instance;
            String tokenString = StorageService.getPartitioner().getTokenFactory().toString(ss.getBootstrapToken());
            MessageOut<String> response = new MessageOut<String>(MessagingService.Verb.INTERNAL_RESPONSE, tokenString, StringSerializer.instance);
            MessagingService.instance().sendReply(response, id, message.from);
        }
    }

    private static class BootstrapTokenCallback implements IAsyncCallback<String>
    {
        private volatile Token<?> token;
        private final Condition condition = new SimpleCondition();

        public Token<?> getToken(long timeout)
        {
            boolean success;
            try
            {
                success = condition.await(timeout, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }

            return success ? token : null;
        }

        public void response(MessageIn<String> msg)
        {
            token = StorageService.getPartitioner().getTokenFactory().fromString(msg.payload);
            condition.signalAll();
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutput out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInput in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.NATIVE.sizeof(s);
        }
    }
}
