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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Collections2;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class SystemReplicas
{
    private static final Map<InetAddressAndPort, Replica> systemReplicas = new ConcurrentHashMap<>();
    public static final Range<Token> FULL_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                              DatabaseDescriptor.getPartitioner().getMinimumToken());

    private static Replica createSystemReplica(InetAddressAndPort endpoint)
    {
        return new Replica(endpoint, FULL_RANGE, true);
    }

    /**
     * There are a few places where a system function borrows write path functionality, but doesn't otherwise
     * fit into normal replication strategies (ie: hints and batchlog). So here we provide a replica instance
     */
    public static Replica getSystemReplica(InetAddressAndPort endpoint)
    {
        return systemReplicas.computeIfAbsent(endpoint, SystemReplicas::createSystemReplica);
    }

    public static EndpointsForRange getSystemReplicas(Collection<InetAddressAndPort> endpoints)
    {
        if (endpoints.isEmpty())
            return EndpointsForRange.empty(FULL_RANGE);

        return EndpointsForRange.copyOf(Collections2.transform(endpoints, SystemReplicas::getSystemReplica));
    }
}
