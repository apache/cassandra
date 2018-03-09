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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan
{
    private static final String[] EMPTY_COLUMN_FAMILIES = new String[0];
    private final UUID planId = UUIDGen.getTimeUUID();
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
        this(streamOperation, 1, false, false, NO_PENDING_REPAIR, PreviewKind.NONE);
    }

    public StreamPlan(StreamOperation streamOperation, boolean keepSSTableLevels, boolean connectSequentially)
    {
        this(streamOperation, 1, keepSSTableLevels, connectSequentially, NO_PENDING_REPAIR, PreviewKind.NONE);
    }

    public StreamPlan(StreamOperation streamOperation, int connectionsPerHost, boolean keepSSTableLevels,
                      boolean connectSequentially, UUID pendingRepair, PreviewKind previewKind)
    {
        this.streamOperation = streamOperation;
        this.coordinator = new StreamCoordinator(connectionsPerHost, keepSSTableLevels, new DefaultConnectionFactory(),
                                                 connectSequentially, pendingRepair, previewKind);
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddressAndPort from, InetAddressAndPort connecting, String keyspace, Collection<Range<Token>> ranges)
    {
        return requestRanges(from, connecting, keyspace, ranges, EMPTY_COLUMN_FAMILIES);
    }

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param connecting Actual connecting address for the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddressAndPort from, InetAddressAndPort connecting, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = coordinator.getOrCreateNextSession(from, connecting);
        session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies));
        return this;
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @see #transferRanges(InetAddressAndPort, InetAddressAndPort, String, java.util.Collection, String...)
     */
    public StreamPlan transferRanges(InetAddressAndPort to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        return transferRanges(to, to, keyspace, ranges, columnFamilies);
    }

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddressAndPort to, InetAddressAndPort connecting, String keyspace, Collection<Range<Token>> ranges)
    {
        return transferRanges(to, connecting, keyspace, ranges, EMPTY_COLUMN_FAMILIES);
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param connecting Actual connecting address of the endpoint
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddressAndPort to, InetAddressAndPort connecting, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = coordinator.getOrCreateNextSession(to, connecting);
        session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), flushBeforeTransfer);
        return this;
    }

    /**
     * Add transfer task to send given SSTable files.
     *
     * @param to endpoint address of receiver
     * @param sstableDetails sstables with file positions and estimated key count.
     *                       this collection will be modified to remove those files that are successfully handed off
     * @return this object for chaining
     */
    public StreamPlan transferFiles(InetAddressAndPort to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        coordinator.transferFiles(to, sstableDetails);
        return this;

    }

    public StreamPlan listeners(StreamEventHandler handler, StreamEventHandler... handlers)
    {
        this.handlers.add(handler);
        if (handlers != null)
            Collections.addAll(this.handlers, handlers);
        return this;
    }

    /**
     * Set custom StreamConnectionFactory to be used for establishing connection
     *
     * @param factory StreamConnectionFactory to use
     * @return self
     */
    public StreamPlan connectionFactory(StreamConnectionFactory factory)
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
        return StreamResultFuture.init(planId, streamOperation, handlers, coordinator);
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

    public UUID getPendingRepair()
    {
        return coordinator.getPendingRepair();
    }

    public boolean getFlushBeforeTransfer()
    {
        return flushBeforeTransfer;
    }
}
