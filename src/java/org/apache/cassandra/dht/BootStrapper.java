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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifierSupport;
import org.apache.cassandra.utils.progress.ProgressEventType;

public class BootStrapper extends ProgressEventNotifierSupport
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddressAndPort address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final ClusterMetadata metadata;

    public BootStrapper(InetAddressAndPort address, Collection<Token> tokens, ClusterMetadata metadata)
    {
        assert address != null;
        assert tokens != null && !tokens.isEmpty();

        this.address = address;
        this.tokens = tokens;
        this.metadata = metadata;
    }

    public Future<StreamState> bootstrap(StreamStateStore stateStore, boolean useStrictConsistency, InetAddressAndPort replacing)
    {
        logger.trace("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(metadata,
                                                   tokens,
                                                   StreamOperation.BOOTSTRAP,
                                                   useStrictConsistency,
                                                   DatabaseDescriptor.getEndpointSnitch(),
                                                   stateStore,
                                                   true,
                                                   DatabaseDescriptor.getStreamingConnectionsPerHost());

        if (replacing != null)
            streamer.addSourceFilter(new RangeStreamer.ExcludedSourcesFilter(Collections.singleton(replacing)));

        final Collection<String> nonLocalStrategyKeyspaces = Schema.instance.getNonLocalStrategyKeyspaces().names();
        if (nonLocalStrategyKeyspaces.isEmpty())
            logger.debug("Schema does not contain any non-local keyspaces to stream on bootstrap");
        for (String keyspaceName : nonLocalStrategyKeyspaces)
        {
            KeyspaceMetadata ksm = metadata.schema.getKeyspaces().get(keyspaceName).get();
            if (ksm.params.replication.isMeta())
                continue;
            streamer.addRanges(keyspaceName, metadata.placements.get(ksm.params.replication).writes.byEndpoint().get(FBUtilities.getBroadcastAddressAndPort()));
        }

        StreamResultFuture bootstrapStreamResult = streamer.fetchAsync();
        bootstrapStreamResult.addEventListener(new StreamEventHandler()
        {
            private final AtomicInteger receivedFiles = new AtomicInteger();
            private final AtomicInteger totalFilesToReceive = new AtomicInteger();

            @Override
            public void handleStreamEvent(StreamEvent event)
            {
                switch (event.eventType)
                {
                    case STREAM_PREPARED:
                        StreamEvent.SessionPreparedEvent prepared = (StreamEvent.SessionPreparedEvent) event;
                        int currentTotal = totalFilesToReceive.addAndGet((int) prepared.session.getTotalFilesToReceive());
                        ProgressEvent prepareProgress = new ProgressEvent(ProgressEventType.PROGRESS, receivedFiles.get(), currentTotal, "prepare with " + prepared.session.peer + " complete");
                        fireProgressEvent("bootstrap", prepareProgress);
                        break;

                    case FILE_PROGRESS:
                        StreamEvent.ProgressEvent progress = (StreamEvent.ProgressEvent) event;
                        if (progress.progress.isCompleted())
                        {
                            int received = receivedFiles.incrementAndGet();
                            ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.PROGRESS, received, totalFilesToReceive.get(), "received file " + progress.progress.fileName);
                            fireProgressEvent("bootstrap", currentProgress);
                        }
                        break;

                    case STREAM_COMPLETE:
                        StreamEvent.SessionCompleteEvent completeEvent = (StreamEvent.SessionCompleteEvent) event;
                        ProgressEvent completeProgress = new ProgressEvent(ProgressEventType.PROGRESS, receivedFiles.get(), totalFilesToReceive.get(), "session with " + completeEvent.peer + " complete");
                        fireProgressEvent("bootstrap", completeProgress);
                        break;
                }
            }

            @Override
            public void onSuccess(StreamState streamState)
            {
                ProgressEventType type;
                String message;

                if (streamState.hasFailedSession())
                {
                    type = ProgressEventType.ERROR;
                    message = "Some bootstrap stream failed";
                }
                else
                {
                    type = ProgressEventType.SUCCESS;
                    message = "Bootstrap streaming success";
                }
                ProgressEvent currentProgress = new ProgressEvent(type, receivedFiles.get(), totalFilesToReceive.get(), message);
                fireProgressEvent("bootstrap", currentProgress);
            }

            @Override
            public void onFailure(Throwable throwable)
            {
                ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.ERROR, receivedFiles.get(), totalFilesToReceive.get(), throwable.getMessage());
                fireProgressEvent("bootstrap", currentProgress);
            }
        });
        return bootstrapStreamResult;
    }

    /**
     * if initialtoken was specified, use that (split on comma).
     * otherwise, if allocationKeyspace is specified use the token allocation algorithm to generate suitable tokens
     * else choose num_tokens tokens at random
     */
    public static Collection<Token> getBootstrapTokens(final ClusterMetadata metadata, InetAddressAndPort address) throws ConfigurationException
    {
        String allocationKeyspace = DatabaseDescriptor.getAllocateTokensForKeyspace();
        Integer allocationLocalRf = DatabaseDescriptor.getAllocateTokensForLocalRf();
        Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
        if (initialTokens.size() > 0 && allocationKeyspace != null)
            logger.warn("manually specified tokens override automatic allocation");

        // if user specified tokens, use those
        if (initialTokens.size() > 0)
        {
            Collection<Token> tokens = getSpecifiedTokens(metadata, initialTokens);
            BootstrapDiagnostics.useSpecifiedTokens(address, allocationKeyspace, tokens, DatabaseDescriptor.getNumTokens());
            return tokens;
        }

        int numTokens = DatabaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (allocationKeyspace != null)
            return allocateTokens(metadata, address, allocationKeyspace, numTokens);

        if (allocationLocalRf != null)
            return allocateTokens(metadata, address, allocationLocalRf, numTokens);

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode.  You should probably add more vnodes and/or use the automatic token allocation mechanism.");

        Collection<Token> tokens = getRandomTokens(metadata, numTokens);
        BootstrapDiagnostics.useRandomTokens(address, metadata, numTokens, tokens);
        return tokens;
    }

    private static Collection<Token> getSpecifiedTokens(final TokenMetadata metadata,
                                                        Collection<String> initialTokens)
    {
        logger.info("tokens manually specified as {}",  initialTokens);
        List<Token> tokens = new ArrayList<>(initialTokens.size());
        for (String tokenString : initialTokens)
        {
            Token token = metadata.partitioner.getTokenFactory().fromString(tokenString);
            if (metadata.getEndpoint(token) != null)
                throw new ConfigurationException("Bootstrapping to existing token " + tokenString + " is not allowed (decommission/removenode the old node first).");
            tokens.add(token);
        }
        return tokens;
    }

    static Collection<Token> allocateTokens(final TokenMetadata metadata,
                                            InetAddressAndPort address,
                                            String allocationKeyspace,
                                            int numTokens)
    {
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        Keyspace ks = Keyspace.open(allocationKeyspace);
        if (ks == null)
            throw new ConfigurationException("Problem opening token allocation keyspace " + allocationKeyspace);
        AbstractReplicationStrategy rs = ks.getReplicationStrategy();

        Collection<Token> tokens = TokenAllocation.allocateTokens(metadata, rs, address, numTokens);
        BootstrapDiagnostics.tokensAllocated(address, metadata, allocationKeyspace, numTokens, tokens);
        return tokens;
    }


    static Collection<Token> allocateTokens(final TokenMetadata metadata,
                                            InetAddressAndPort address,
                                            int rf,
                                            int numTokens)
    {
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        Collection<Token> tokens = TokenAllocation.allocateTokens(metadata, rf, address, numTokens);
        BootstrapDiagnostics.tokensAllocated(address, metadata, rf, numTokens, tokens);
        return tokens;
    }

    public static Collection<Token> getRandomTokens(TokenMetadata metadata, int numTokens)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = metadata.partitioner.getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }

        logger.info("Generated random tokens. tokens are {}", tokens);
        return tokens;
    }
}
