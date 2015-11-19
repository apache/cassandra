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

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.tokenallocator.TokenAllocation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifierSupport;
import org.apache.cassandra.utils.progress.ProgressEventType;

public class BootStrapper extends ProgressEventNotifierSupport
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);

    /* endpoint that needs to be bootstrapped */
    protected final InetAddress address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final TokenMetadata tokenMetadata;

    public BootStrapper(InetAddress address, Collection<Token> tokens, TokenMetadata tmd)
    {
        assert address != null;
        assert tokens != null && !tokens.isEmpty();

        this.address = address;
        this.tokens = tokens;
        this.tokenMetadata = tmd;
    }

    public ListenableFuture<StreamState> bootstrap(StreamStateStore stateStore, boolean useStrictConsistency)
    {
        logger.trace("Beginning bootstrap process");

        RangeStreamer streamer = new RangeStreamer(tokenMetadata,
                                                   tokens,
                                                   address,
                                                   "Bootstrap",
                                                   useStrictConsistency,
                                                   DatabaseDescriptor.getEndpointSnitch(),
                                                   stateStore,
                                                   true);
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));

        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            AbstractReplicationStrategy strategy = Keyspace.open(keyspaceName).getReplicationStrategy();
            streamer.addRanges(keyspaceName, strategy.getPendingAddressRanges(tokenMetadata, tokens, address));
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
    public static Collection<Token> getBootstrapTokens(final TokenMetadata metadata, InetAddress address) throws ConfigurationException
    {
        String allocationKeyspace = DatabaseDescriptor.getAllocateTokensForKeyspace();
        Collection<String> initialTokens = DatabaseDescriptor.getInitialTokens();
        if (initialTokens.size() > 0 && allocationKeyspace != null)
            logger.warn("manually specified tokens override automatic allocation");

        // if user specified tokens, use those
        if (initialTokens.size() > 0)
            return getSpecifiedTokens(metadata, initialTokens);

        int numTokens = DatabaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (allocationKeyspace != null)
            return allocateTokens(metadata, address, allocationKeyspace, numTokens);

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode.  You should probably add more vnodes and/or use the automatic token allocation mechanism.");

        return getRandomTokens(metadata, numTokens);
    }

    private static Collection<Token> getSpecifiedTokens(final TokenMetadata metadata,
                                                        Collection<String> initialTokens)
    {
        logger.trace("tokens manually specified as {}",  initialTokens);
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
                                            InetAddress address,
                                            String allocationKeyspace,
                                            int numTokens)
    {
        Keyspace ks = Keyspace.open(allocationKeyspace);
        if (ks == null)
            throw new ConfigurationException("Problem opening token allocation keyspace " + allocationKeyspace);
        AbstractReplicationStrategy rs = ks.getReplicationStrategy();

        return TokenAllocation.allocateTokens(metadata, rs, address, numTokens);
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
        return tokens;
    }

    public static class StringSerializer implements IVersionedSerializer<String>
    {
        public static final StringSerializer instance = new StringSerializer();

        public void serialize(String s, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(s);
        }

        public String deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readUTF();
        }

        public long serializedSize(String s, int version)
        {
            return TypeSizes.sizeof(s);
        }
    }
}
