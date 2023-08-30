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
package org.apache.cassandra.streaming;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.TimeUUID;

import static com.google.common.collect.Iterables.all;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.streaming.StreamingChannel.Factory.Global.streamingFactory;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan
{
    private static final String[] EMPTY_COLUMN_FAMILIES = new String[0];
    private final TimeUUID planId = nextTimeUUID();
    private final StreamOperation streamOperation;
    private final List<StreamEventHandler> handlers = new ArrayList<>();
    private final StreamCoordinator coordinator;

    private boolean flushBeforeTransfer = true;

    /**
     * Start building stream plan.
     *
     * @param streamOperation Stream streamOperation that describes this StreamPlan
     */
    public StreamPlan(StreamOperation streamOperation)
    {
        this(streamOperation, 1, false, NO_PENDING_REPAIR, PreviewKind.NONE);
    }

    public StreamPlan(StreamOperation streamOperation, boolean connectSequentially)
    {
        this(streamOperation, 1, connectSequentially, NO_PENDING_REPAIR, PreviewKind.NONE);
    }

    public StreamPlan(StreamOperation streamOperation, int connectionsPerHost,
                      boolean connectSequentially, TimeUUID pendingRepair, PreviewKind previewKind)
    {
        this.streamOperation = streamOperation;
        this.coordinator = new StreamCoordinator(streamOperation, connectionsPerHost, streamingFactory(),
                                                 false, connectSequentially, pendingRepair, previewKind);
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * Here, we have to encode both _local_ range transientness (encoded in Replica itself, in RangesAtEndpoint)
     * and _remote_ (source) range transientmess, which is encoded by splitting ranges into full and transient.
     *
     * At the other end the distinction between full and transient is ignored it just used the transient status
     * of the Replica objects we send to determine what to send. The real reason we have this split down to
     * StreamRequest is that on completion StreamRequest is used to write to the system table tracking
     * what has already been streamed. At that point since we only have the local Replica instances so we don't
     * know what we got from the remote. We preserve that here by splitting based on the remotes transient
     * status.
     * 
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param fullRanges ranges to fetch that from provides the full version of
     * @param transientRanges ranges to fetch that from provides only transient data of
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddressAndPort from, String keyspace, RangesAtEndpoint fullRanges, RangesAtEndpoint transientRanges)
    {
        return requestRanges(from, keyspace, fullRanges, transientRanges, EMPTY_COLUMN_FAMILIES);
    }

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param fullRanges ranges to fetch that from provides the full data for
     * @param transientRanges ranges to fetch that from provides only transient data for
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddressAndPort from, String keyspace, RangesAtEndpoint fullRanges, RangesAtEndpoint transientRanges, String... columnFamilies)
    {
        //It should either be a dummy address for repair or if it's a bootstrap/move/rebuild it should be this node
        assert all(fullRanges, Replica::isSelf) || RangesAtEndpoint.isDummyList(fullRanges) : fullRanges.toString();
        assert all(transientRanges, Replica::isSelf) || RangesAtEndpoint.isDummyList(transientRanges) : transientRanges.toString();

        StreamSession session = coordinator.getOrCreateOutboundSession(from);
        session.addStreamRequest(keyspace, fullRanges, transientRanges, Arrays.asList(columnFamilies));
        return this;
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param replicas ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddressAndPort to, String keyspace, RangesAtEndpoint replicas, String... columnFamilies)
    {
        StreamSession session = coordinator.getOrCreateOutboundSession(to);
        session.addTransferRanges(keyspace, replicas, Arrays.asList(columnFamilies), flushBeforeTransfer);
        return this;
    }

    /**
     * Add transfer task to send given streams
     *
     * @param to endpoint address of receiver
     * @param streams streams to send
     * @return this object for chaining
     */
    public StreamPlan transferStreams(InetAddressAndPort to, Collection<OutgoingStream> streams)
    {
        coordinator.transferStreams(to, streams);
        return this;
    }

    public StreamPlan listeners(StreamEventHandler handler, StreamEventHandler... handlers)
    {
        this.handlers.add(handler);
        if (handlers != null)
            Collections.addAll(this.handlers, handlers);
        return this;
    }

    public TimeUUID planId()
    {
        return planId;
    }

    public StreamOperation streamOperation()
    {
        return streamOperation;
    }

    @VisibleForTesting
    public List<StreamEventHandler> handlers()
    {
        return handlers;
    }

    /**
     * Set custom StreamConnectionFactory to be used for establishing connection
     *
     * @param factory StreamConnectionFactory to use
     * @return self
     */
    public StreamPlan connectionFactory(StreamingChannel.Factory factory)
    {
        this.coordinator.setConnectionFactory(factory);
        return this;
    }

    /**
     * @return true if this plan has no plan to execute
     */
    public boolean isEmpty()
    {
        return !coordinator.hasActiveSessions();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    public StreamResultFuture execute()
    {
        return StreamResultFuture.createInitiator(planId, streamOperation, handlers, coordinator);
    }

    /**
     * Set flushBeforeTransfer option.
     * When it's true, will flush before streaming ranges. (Default: true)
     *
     * @param flushBeforeTransfer set to true when the node should flush before transfer
     * @return this object for chaining
     */
    public StreamPlan flushBeforeTransfer(boolean flushBeforeTransfer)
    {
        this.flushBeforeTransfer = flushBeforeTransfer;
        return this;
    }

    public TimeUUID getPendingRepair()
    {
        return coordinator.getPendingRepair();
    }

    public boolean getFlushBeforeTransfer()
    {
        return flushBeforeTransfer;
    }

    @VisibleForTesting
    public StreamCoordinator getCoordinator()
    {
        return coordinator;
    }
}
