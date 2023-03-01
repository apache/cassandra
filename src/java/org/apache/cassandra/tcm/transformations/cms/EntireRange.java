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

package org.apache.cassandra.tcm.transformations.cms;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.utils.FBUtilities;

public class EntireRange
{
    public static final Range<Token> entireRange = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                               DatabaseDescriptor.getPartitioner().getMinimumToken());
    public static Replica replica(InetAddressAndPort addr)
    {
        return new Replica(addr, entireRange, true);
    }

    public static final EndpointsForRange localReplicas = EndpointsForRange.of(replica(FBUtilities.getBroadcastAddressAndPort()));
    public static final DataPlacement placement = new DataPlacement(PlacementForRange.builder().withReplicaGroup(localReplicas).build(),
                                                                    PlacementForRange.builder().withReplicaGroup(localReplicas).build());
}