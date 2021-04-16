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

package org.apache.cassandra.service.reads.repair;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.DigestResolver.DigestResolverDebugResult;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;

final class ReadRepairEvent extends DiagnosticEvent
{

    private final ReadRepairEventType type;
    private final Keyspace keyspace;
    private final String tableName;
    private final String cqlCommand;
    private final ConsistencyLevel consistency;
    private final SpeculativeRetryPolicy.Kind speculativeRetry;
    @VisibleForTesting
    final Collection<InetAddressAndPort> destinations;
    @VisibleForTesting
    final Collection<InetAddressAndPort> allEndpoints;
    @Nullable
    private final DigestResolverDebugResult[] digestsByEndpoint;

    enum ReadRepairEventType
    {
        START_REPAIR,
        SPECULATED_READ
    }

    ReadRepairEvent(ReadRepairEventType type, AbstractReadRepair readRepair, Collection<InetAddressAndPort> destinations,
                    Collection<InetAddressAndPort> allEndpoints, DigestResolver digestResolver)
    {
        this.keyspace = readRepair.cfs.keyspace;
        this.tableName = readRepair.cfs.getTableName();
        this.cqlCommand = readRepair.command.toCQLString();
        this.consistency = readRepair.replicaPlan().consistencyLevel();
        this.speculativeRetry = readRepair.cfs.metadata().params.speculativeRetry.kind();
        this.destinations = destinations;
        this.allEndpoints = allEndpoints;
        this.digestsByEndpoint = digestResolver != null ? digestResolver.getDigestsByEndpoint() : null;
        this.type = type;
    }

    public ReadRepairEventType getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();

        ret.put("keyspace", keyspace.getName());
        ret.put("table", tableName);
        ret.put("command", cqlCommand);
        ret.put("consistency", consistency.name());
        ret.put("speculativeRetry", speculativeRetry.name());

        Set<String> eps = destinations.stream().map(Object::toString).collect(Collectors.toSet());
        ret.put("endpointDestinations", new HashSet<>(eps));

        if (digestsByEndpoint != null)
        {
            HashMap<String, Serializable> digestsMap = new HashMap<>();
            for (DigestResolverDebugResult digestsByEndpoint : digestsByEndpoint)
            {
                HashMap<String, Serializable> digests = new HashMap<>();
                digests.put("digestHex", digestsByEndpoint.digestHex);
                digests.put("isDigestResponse", digestsByEndpoint.isDigestResponse);
                digestsMap.put(digestsByEndpoint.from.toString(), digests);
            }
            ret.put("digestsByEndpoint", digestsMap);
        }
        if (allEndpoints != null)
        {
            eps = allEndpoints.stream().map(Object::toString).collect(Collectors.toSet());
            ret.put("allEndpoints", new HashSet<>(eps));
        }
        return ret;
    }
}
