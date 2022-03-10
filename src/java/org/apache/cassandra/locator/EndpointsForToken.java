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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import java.util.Arrays;
import java.util.Collection;

/**
 * A ReplicaCollection where all Replica are required to cover a range that fully contains the token() defined in the builder().
 * Endpoints are guaranteed to be unique; on construction, this is enforced unless optionally silenced (in which case
 * only the first occurrence makes the cut).
 */
public class EndpointsForToken extends Endpoints<EndpointsForToken>
{
    private final Token token;

    EndpointsForToken(Token token, ReplicaList list, ReplicaMap<InetAddressAndPort> byEndpoint)
    {
        super(list, byEndpoint);
        this.token = token;
        assert token != null;
    }

    public Token token()
    {
        return token;
    }

    @Override
    public Builder newBuilder(int initialCapacity)
    {
        return new Builder(token, initialCapacity);
    }

    @Override
    public EndpointsForToken snapshot()
    {
        return this;
    }

    @Override
    protected EndpointsForToken snapshot(ReplicaList newList)
    {
        if (newList.isEmpty()) return empty(token);
        ReplicaMap<InetAddressAndPort> byEndpoint = null;
        if (this.byEndpoint != null && list.isSubList(newList))
            byEndpoint = this.byEndpoint.forSubList(newList);
        return new EndpointsForToken(token, newList, byEndpoint);
    }

    public Replica lookup(InetAddressAndPort endpoint)
    {
        return byEndpoint().get(endpoint);
    }

    public static class Builder extends EndpointsForToken implements ReplicaCollection.Builder<EndpointsForToken>
    {
        boolean built;
        public Builder(Token token) { this(token, 0); }
        public Builder(Token token, int capacity) { this(token, new ReplicaList(capacity)); }
        private Builder(Token token, ReplicaList list) { super(token, list, endpointMap(list)); }

        public EndpointsForToken.Builder add(Replica replica, Conflict ignoreConflict)
        {
            if (built) throw new IllegalStateException();
            Preconditions.checkNotNull(replica);
            if (!replica.range().contains(super.token))
                throw new IllegalArgumentException("Replica " + replica + " does not contain " + super.token);

            if (!super.byEndpoint.internalPutIfAbsent(replica, list.size()))
            {
                switch (ignoreConflict)
                {
                    case DUPLICATE:
                        if (byEndpoint().get(replica.endpoint()).equals(replica))
                            break;
                    case NONE:
                        throw new IllegalArgumentException("Conflicting replica added (expected unique endpoints): "
                                + replica + "; existing: " + byEndpoint().get(replica.endpoint()));
                    case ALL:
                }
                return this;
            }

            list.add(replica);
            return this;
        }

        @Override
        public EndpointsForToken snapshot()
        {
            return snapshot(list.subList(0, list.size()));
        }

        public EndpointsForToken build()
        {
            built = true;
            return new EndpointsForToken(super.token, super.list, super.byEndpoint);
        }
    }

    public static Builder builder(Token token)
    {
        return new Builder(token);
    }
    public static Builder builder(Token token, int capacity)
    {
        return new Builder(token, capacity);
    }

    public static EndpointsForToken empty(Token token)
    {
        return new EndpointsForToken(token, EMPTY_LIST, EMPTY_MAP);
    }

    public static EndpointsForToken of(Token token, Replica replica)
    {
        // we only use ArrayList or ArrayList.SubList, to ensure callsites are bimorphic
        ReplicaList one = new ReplicaList(1);
        one.add(replica);
        // we can safely use singletonMap, as we only otherwise use LinkedHashMap
        return new EndpointsForToken(token, one, endpointMap(one));
    }

    public static EndpointsForToken of(Token token, Replica ... replicas)
    {
        return copyOf(token, Arrays.asList(replicas));
    }

    public static EndpointsForToken copyOf(Token token, Collection<Replica> replicas)
    {
        if (replicas.isEmpty()) return empty(token);
        return builder(token, replicas.size()).addAll(replicas).build();
    }

    public static EndpointsForToken natural(Keyspace keyspace, Token token)
    {
        return keyspace.getReplicationStrategy().getNaturalReplicasForToken(token);
    }

    public static EndpointsForToken natural(AbstractReplicationStrategy replicationStrategy, Token token)
    {
        return replicationStrategy.getNaturalReplicasForToken(token);
    }

    public static EndpointsForToken natural(TableMetadata table, Token token)
    {
        return natural(Keyspace.open(table.keyspace), token);
    }

    public static EndpointsForToken pending(TableMetadata table, Token token)
    {
        return pending(table.keyspace, token);
    }

    public static EndpointsForToken pending(Keyspace keyspace, Token token)
    {
        return pending(keyspace.getName(), token);
    }

    public static EndpointsForToken pending(String keyspace, Token token)
    {
        return StorageService.instance.getTokenMetadata().pendingEndpointsForToken(token, keyspace);
    }
}
