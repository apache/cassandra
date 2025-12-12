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
package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Statistics about synchronizing two replica
 */
public class SyncStat
{
    public final SyncNodePair nodes;
    public final Collection<Range<Token>> differences;
    public final List<SessionSummary> summaries;
    public final TimeUUID planId;

    @Nullable
    public final ShortMutationId transferId;

    public SyncStat(SyncNodePair nodes, Collection<Range<Token>> differences)
    {
        this(nodes, differences, null);
    }

    private SyncStat(SyncNodePair nodes,  Collection<Range<Token>> differences, List<SessionSummary> summaries)
    {
        this.nodes = nodes;
        this.summaries = summaries;
        this.differences = differences;
        this.planId = null;
        this.transferId = null;
    }

    private SyncStat(SyncNodePair nodes,  Collection<Range<Token>> differences, List<SessionSummary> summaries, TimeUUID planId, ShortMutationId transferId)
    {
        this.nodes = nodes;
        this.summaries = summaries;
        this.differences = differences;
        this.planId = planId;
        this.transferId = transferId;
    }

    public SyncStat withSummaries(List<SessionSummary> summaries, TimeUUID planId, ShortMutationId transferId)
    {
        return new SyncStat(nodes, differences, summaries, planId, transferId);
    }

    @Override
    public String toString()
    {
        return "SyncStat{" +
               "nodes=" + nodes +
               ", differences=" + differences +
               ", summaries=" + summaries +
               ", planId=" + planId +
               ", transfer ID=" + transferId +
               '}';
    }
}
