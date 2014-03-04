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

import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.UUIDGen;

/**
 * {@link StreamPlan} is a helper class that builds StreamOperation of given configuration.
 *
 * This is the class you want to use for building streaming plan and starting streaming.
 */
public class StreamPlan
{
    private final UUID planId = UUIDGen.getTimeUUID();
    private final String description;
    private final List<StreamEventHandler> handlers = new ArrayList<>();

    // sessions per InetAddress of the other end.
    private final Map<InetAddress, StreamSession> sessions = new HashMap<>();
    private final long repairedAt;

    private boolean flushBeforeTransfer = true;

    /**
     * Start building stream plan.
     *
     * @param description Stream type that describes this StreamPlan
     */
    public StreamPlan(String description)
    {
        this(description, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public StreamPlan(String description, long repairedAt)
    {
        this.description = description;
        this.repairedAt = repairedAt;
    }


    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges)
    {
        return requestRanges(from, keyspace, ranges, new String[0]);
    }

    /**
     * Request data in {@code columnFamilies} under {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(from);
        session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies), repairedAt);
        return this;
    }

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges)
    {
        return transferRanges(to, keyspace, ranges, new String[0]);
    }

    /**
     * Add transfer task to send data of specific {@code columnFamilies} under {@code keyspace} and {@code ranges}.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     * @param columnFamilies specific column families
     * @return this object for chaining
     */
    public StreamPlan transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), flushBeforeTransfer, repairedAt);
        return this;
    }

    /**
     * Add transfer task to send given SSTable files.
     *
     * @param to endpoint address of receiver
     * @param sstableDetails sstables with file positions and estimated key count
     * @return this object for chaining
     */
    public StreamPlan transferFiles(InetAddress to, Collection<StreamSession.SSTableStreamingSections> sstableDetails)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferFiles(sstableDetails);
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
     * @return true if this plan has no plan to execute
     */
    public boolean isEmpty()
    {
        return sessions.isEmpty();
    }

    /**
     * Execute this {@link StreamPlan} asynchronously.
     *
     * @return Future {@link StreamState} that you can use to listen on progress of streaming.
     */
    public StreamResultFuture execute()
    {
        return StreamResultFuture.init(planId, description, sessions.values(), handlers);
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

    private StreamSession getOrCreateSession(InetAddress peer)
    {
        StreamSession session = sessions.get(peer);
        if (session == null)
        {
            session = new StreamSession(peer);
            sessions.put(peer, session);
        }
        return session;
    }

}
