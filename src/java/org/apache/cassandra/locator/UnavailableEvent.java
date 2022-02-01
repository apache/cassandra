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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;

/**
 * Event based on thrown {@link UnavailableException} with additional context details.
 */
class UnavailableEvent extends DiagnosticEvent
{
    private final UnavailableEventType type;
    private final UnavailableException exception;
    @Nullable
    private final ReplicaPlan<?> plan;
    @Nullable
    private final Set<InetAddressAndPort> contacts;
    @Nullable
    private final Token token;
    @Nullable
    private final AbstractBounds<PartitionPosition> bounds;

    enum UnavailableEventType
    {
        FOR_PAXOS_WRITE,
        FOR_WRITE,
        FOR_READ,
        FOR_RANGE_READ,
        FOR_TRUNCATE,
        FOR_COUNTER,
    }

    UnavailableEvent(UnavailableEventType type, UnavailableException exception)
    {
        this(type, exception, null, null, (Token) null);
    }

    UnavailableEvent(UnavailableEventType type, UnavailableException exception, ReplicaPlan<?> plan, Set<InetAddressAndPort> contacts)
    {
        this(type, exception, plan, contacts, (Token) null);
    }

    UnavailableEvent(UnavailableEventType type, UnavailableException exception, ReplicaPlan<?> plan, Set<InetAddressAndPort> contacts, @Nullable Token token)
    {
        this.type = type;
        this.exception = exception;
        this.plan = plan;
        this.contacts = contacts;
        this.token = token;
        this.bounds = null;
    }

    UnavailableEvent(UnavailableEventType type, UnavailableException exception, ReplicaPlan<?> plan, Set<InetAddressAndPort> contacts, @Nullable AbstractBounds<PartitionPosition> bounds)
    {
        this.type = type;
        this.exception = exception;
        this.plan = plan;
        this.contacts = contacts;
        this.token = null;
        this.bounds = bounds;
    }

    @Override
    public UnavailableEventType getType()
    {
        return type;
    }

    @Override
    public Map<String, Serializable> toMap()
    {
        Map<String, Serializable> ret = new HashMap<>();
        ret.put("exception", reportException(exception));
        ret.put("localDC", DatabaseDescriptor.getLocalDataCenter());
        ret.put("downInstances", reportDownInstances());
        if (plan != null)
        {
            ret.put("keyspaceName", plan.keyspace.getName());
            ret.put("blockFor", plan.blockFor());
        }
        if (contacts != null)
        {
            ret.put("contacts", new ArrayList<>(contacts.stream().map(c -> c.toString(true)).sorted().collect(Collectors.toList())));
        }
        if (plan != null)
        {
            ret.put("replicationStrategy", reportReplicationStrategy(plan));
        }
        if (token != null)
        {
            ret.put("token", token);
        }
        if (bounds != null)
        {
            ret.put("bounds", bounds.toString());
        }
        return ret;
    }

    public UnavailableException getException()
    {
        return exception;
    }

    @Nullable
    public ReplicaPlan<?> getPlan()
    {
        return plan;
    }

    @Nullable
    public Set<InetAddressAndPort> getContacts()
    {
        return contacts;
    }

    @Nullable
    public Token getToken()
    {
        return token;
    }

    @Nullable
    public AbstractBounds<PartitionPosition> getBounds()
    {
        return bounds;
    }

    private HashMap<String, Serializable> reportException(UnavailableException exception)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        ret.put("message", exception.getMessage());
        ret.put("alive", exception.alive);
        ret.put("required", exception.required);
        ret.put("consistency", exception.consistency.name());
        return ret;
    }

    private static HashMap<String, Serializable> reportReplicationStrategy(ReplicaPlan<?> plan)
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        AbstractReplicationStrategy strategy = plan.replicationStrategy();
        ret.put("class", strategy.getClass().getSimpleName());
        ret.put("rf", strategy.getReplicationFactor().allReplicas);
        if (strategy.getReplicationFactor().hasTransientReplicas())
            ret.put("transientReplicas", strategy.getReplicationFactor().transientReplicas());
        ret.put("configOptions", ImmutableMap.copyOf(strategy.configOptions));
        ret.put("keyspaceName", strategy.keyspaceName);

        if (strategy instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy nts = (NetworkTopologyStrategy) strategy;
            HashMap<String, Integer> dcs = new HashMap<>(nts.getDatacenters().stream().collect(Collectors.toMap(Function.identity(), dc -> nts.getReplicationFactor(dc).fullReplicas)));
            ret.put("datacenters", dcs);
        }
        return ret;
    }

    private static ArrayList<String> reportDownInstances()
    {
        ArrayList<String> ret = new ArrayList<>();
        for (InetAddressAndPort endpoint : Gossiper.instance.getEndpoints())
        {
            if (!FailureDetector.instance.isAlive(endpoint))
                ret.add(endpoint.toString(true));
        }
        ret.sort(Comparator.naturalOrder());
        return ret;
    }

}
