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

package org.apache.cassandra.locator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;

import java.util.Map;

public class EndpointsByReplica extends ReplicaMultimap<Replica, EndpointsForRange>
{
    public EndpointsByReplica(Map<Replica, EndpointsForRange> map)
    {
        super(map);
    }

    public EndpointsForRange get(Replica range)
    {
        Preconditions.checkNotNull(range);
        return map.getOrDefault(range, EndpointsForRange.empty(range.range()));
    }

    public static class Builder extends ReplicaMultimap.Builder<Replica, EndpointsForRange.Builder>
    {
        @Override
        protected EndpointsForRange.Builder newBuilder(Replica replica)
        {
            return new EndpointsForRange.Builder(replica.range());
        }

        // TODO: consider all ignoreDuplicates cases
        public void putAll(Replica range, EndpointsForRange replicas, Conflict ignoreConflicts)
        {
            map.computeIfAbsent(range, r -> newBuilder(r)).addAll(replicas, ignoreConflicts);
        }

        public EndpointsByReplica build()
        {
            return new EndpointsByReplica(
                    ImmutableMap.copyOf(
                            Maps.transformValues(this.map, EndpointsForRange.Builder::build)));
        }
    }

}
