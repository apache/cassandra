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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;

/**
 * Base class for log streaming tasks that track mutation log transfers and receives.
 */
public abstract class LogStreamTask
{
    protected final StreamSession session;
    protected final InetAddressAndPort peer;
    protected boolean completed = false;

    private final Map<String, Set<Range<Token>>> keyspaceRanges = new HashMap<>();

    public LogStreamTask(StreamSession session, InetAddressAndPort peer)
    {
        this.session = session;
        this.peer = peer;
    }

    public synchronized void addKeyspaceRanges(String keyspace, RangesAtEndpoint ranges)
    {
        addKeyspaceRanges(keyspace, ranges.ranges());
    }

    public synchronized void addKeyspaceRanges(String keyspace, Collection<Range<Token>> ranges)
    {
        keyspaceRanges.computeIfAbsent(keyspace, k -> new HashSet<>()).addAll(ranges);
    }

    public abstract void abort();

    public boolean isCompleted()
    {
        return completed;
    }

    public LogStreamManifest getManifest()
    {
        return LogStreamManifest.create(keyspaceRanges);
    }

    protected boolean markCompleted()
    {
        if (completed)
            return false;
        completed = true;
        return true;
    }

    public InetAddressAndPort getPeer()
    {
        return peer;
    }
}