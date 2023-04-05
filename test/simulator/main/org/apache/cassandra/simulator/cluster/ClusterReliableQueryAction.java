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

package org.apache.cassandra.simulator.cluster;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.simulator.systems.SimulatedQuery;

import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;

class ClusterReliableQueryAction extends SimulatedQuery
{
    ClusterReliableQueryAction(String id, ClusterActions actions, int on, String query, long timestamp, ConsistencyLevel consistencyLevel, Object... params)
    {
        super(id, RELIABLE_NO_TIMEOUTS, RELIABLE_NO_TIMEOUTS, actions, actions.cluster.get(on), query, timestamp, consistencyLevel, params);
    }

    public static ClusterReliableQueryAction schemaChange(String id, ClusterActions actions, int on, String query)
    {
        // this isn't used on 4.0+ nodes, but no harm in supplying it anyway
        return new ClusterReliableQueryAction(id, actions, on, query, actions.time.nextGlobalMonotonicMicros(), ConsistencyLevel.ALL);
    }
}
