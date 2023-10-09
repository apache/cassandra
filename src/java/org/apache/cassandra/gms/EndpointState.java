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
package org.apache.cassandra.gms;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.NullableSerializer;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */
public class EndpointState
{
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    static volatile boolean LOOSE_DEF_OF_EMPTY_ENABLED = CassandraRelevantProperties.LOOSE_DEF_OF_EMPTY_ENABLED.getBoolean();

    public final static IVersionedSerializer<EndpointState> serializer = new EndpointStateSerializer();
    public final static IVersionedSerializer<EndpointState> nullableSerializer = NullableSerializer.wrap(serializer);

    private volatile HeartBeatState hbState;
    private final AtomicReference<Map<ApplicationState, VersionedValue>> applicationState;

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    public EndpointState(HeartBeatState initialHbState)
    {
        this(initialHbState, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    public EndpointState(EndpointState other)
    {
        this(new HeartBeatState(other.hbState), new EnumMap<>(other.applicationState.get()));
    }

    EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states)
    {
        hbState = initialHbState;
        applicationState = new AtomicReference<Map<ApplicationState, VersionedValue>>(new EnumMap<>(states));
        updateTimestamp = nanoTime();
        isAlive = true;
    }

    @VisibleForTesting
    public HeartBeatState getHeartBeatState()
    {
        return hbState;
    }

    void setHeartBeatState(HeartBeatState newHbState)
    {
        updateTimestamp();
        hbState = newHbState;
    }

    public VersionedValue getApplicationState(ApplicationState key)
    {
        return applicationState.get().get(key);
    }

    public boolean containsApplicationState(ApplicationState key)
    {
        return applicationState.get().containsKey(key);
    }

    public Set<Map.Entry<ApplicationState, VersionedValue>> states()
    {
        return applicationState.get().entrySet();
    }

    public void addApplicationState(ApplicationState key, VersionedValue value)
    {
        addApplicationStates(Collections.singletonMap(key, value));
    }

    public void addApplicationStates(Map<ApplicationState, VersionedValue> values)
    {
        addApplicationStates(values.entrySet());
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values)
    {
        while (true)
        {
            Map<ApplicationState, VersionedValue> orig = applicationState.get();
            Map<ApplicationState, VersionedValue> copy = new EnumMap<>(orig);

            for (Map.Entry<ApplicationState, VersionedValue> value : values)
                copy.put(value.getKey(), value.getValue());

            if (applicationState.compareAndSet(orig, copy))
                return;
        }
    }

    void removeMajorVersion3LegacyApplicationStates()
    {
        while (hasLegacyFields())
        {
            Map<ApplicationState, VersionedValue> orig = applicationState.get();
            Map<ApplicationState, VersionedValue> updatedStates = filterMajorVersion3LegacyApplicationStates(orig);
            // avoid updating if no state is removed
            if (orig.size() == updatedStates.size()
                || applicationState.compareAndSet(orig, updatedStates))
                return;
        }
    }

    private boolean hasLegacyFields()
    {
        Set<ApplicationState> statesPresent = applicationState.get().keySet();
        if (statesPresent.isEmpty())
            return false;
        return (statesPresent.contains(ApplicationState.STATUS) && statesPresent.contains(ApplicationState.STATUS_WITH_PORT))
               || (statesPresent.contains(ApplicationState.INTERNAL_IP) && statesPresent.contains(ApplicationState.INTERNAL_ADDRESS_AND_PORT))
               || (statesPresent.contains(ApplicationState.RPC_ADDRESS) && statesPresent.contains(ApplicationState.NATIVE_ADDRESS_AND_PORT));
    }

    private static Map<ApplicationState, VersionedValue> filterMajorVersion3LegacyApplicationStates(Map<ApplicationState, VersionedValue> states)
    {
        return states.entrySet().stream().filter(entry -> {
                // Filter out pre-4.0 versions of data for more complete 4.0 versions
                switch (entry.getKey())
                {
                    case INTERNAL_IP:
                        return !states.containsKey(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
                    case STATUS:
                        return !states.containsKey(ApplicationState.STATUS_WITH_PORT);
                    case RPC_ADDRESS:
                        return !states.containsKey(ApplicationState.NATIVE_ADDRESS_AND_PORT);
                    default:
                        return true;
                }
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     */
    public long getUpdateTimestamp()
    {
        return updateTimestamp;
    }

    void updateTimestamp()
    {
        updateTimestamp = nanoTime();
    }

    @VisibleForTesting
    public void unsafeSetUpdateTimestamp(long value)
    {
        updateTimestamp = value;
    }

    public boolean isAlive()
    {
        return isAlive;
    }

    @VisibleForTesting
    public void markAlive()
    {
        isAlive = true;
    }

    @VisibleForTesting
    public void markDead()
    {
        isAlive = false;
    }

    public boolean isStateEmpty()
    {
        return applicationState.get().isEmpty();
    }

    /**
     * @return true if {@link HeartBeatState#isEmpty()} is true and no STATUS application state exists
     */
    public boolean isEmptyWithoutStatus()
    {
        Map<ApplicationState, VersionedValue> state = applicationState.get();
        boolean hasStatus = state.containsKey(ApplicationState.STATUS_WITH_PORT) || state.containsKey(ApplicationState.STATUS);
        return hbState.isEmpty() && !hasStatus
               // In the very specific case where hbState.isEmpty and STATUS is missing, this is known to be safe to "fake"
               // the data, as this happens when the gossip state isn't coming from the node but instead from a peer who
               // restarted and is missing the node's state.
               //
               // When hbState is not empty, then the node gossiped an empty STATUS; this happens during bootstrap and it's not
               // possible to tell if this is ok or not (we can't really tell if the node is dead or having networking issues).
               // For these cases allow an external actor to verify and inform Cassandra that it is safe - this is done by
               // updating the LOOSE_DEF_OF_EMPTY_ENABLED field.
               || (LOOSE_DEF_OF_EMPTY_ENABLED && !hasStatus);
    }

    public boolean isRpcReady()
    {
        VersionedValue rpcState = getApplicationState(ApplicationState.RPC_READY);
        return rpcState != null && Boolean.parseBoolean(rpcState.value);
    }

    public boolean isNormalState()
    {
        return getStatus().equals(VersionedValue.STATUS_NORMAL);
    }

    public String getStatus()
    {
        VersionedValue status = getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (status == null)
        {
            status = getApplicationState(ApplicationState.STATUS);
        }
        if (status == null)
        {
            return "";
        }

        String[] pieces = status.value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        return pieces[0];
    }

    @Nullable
    public UUID getSchemaVersion()
    {
        VersionedValue applicationState = getApplicationState(ApplicationState.SCHEMA);
        return applicationState != null
               ? UUID.fromString(applicationState.value)
               : null;
    }

    @Nullable
    public CassandraVersion getReleaseVersion()
    {
        VersionedValue applicationState = getApplicationState(ApplicationState.RELEASE_VERSION);
        return applicationState != null
               ? new CassandraVersion(applicationState.value)
               : null;
    }

    public String toString()
    {
        return "EndpointState: HeartBeatState = " + hbState + ", AppStateMap = " + applicationState.get();
    }

    public boolean isSupersededBy(EndpointState that)
    {
        int thisGeneration = this.getHeartBeatState().getGeneration();
        int thatGeneration = that.getHeartBeatState().getGeneration();

        if (thatGeneration > thisGeneration)
            return true;

        if (thisGeneration > thatGeneration)
            return false;

        return Gossiper.getMaxEndpointStateVersion(that) > Gossiper.getMaxEndpointStateVersion(this);
    }
}

class EndpointStateSerializer implements IVersionedSerializer<EndpointState>
{
    public void serialize(EndpointState epState, DataOutputPlus out, int version) throws IOException
    {
        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer.serialize(hbState, out, version);

        /* serialize the map of ApplicationState objects */
        Set<Map.Entry<ApplicationState, VersionedValue>> states = epState.states();
        out.writeInt(states.size());
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
        {
            VersionedValue value = state.getValue();
            out.writeInt(state.getKey().ordinal());
            VersionedValue.serializer.serialize(value, out, version);
        }
    }

    public EndpointState deserialize(DataInputPlus in, int version) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer.deserialize(in, version);

        int appStateSize = in.readInt();
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (int i = 0; i < appStateSize; ++i)
        {
            int key = in.readInt();
            VersionedValue value = VersionedValue.serializer.deserialize(in, version);
            states.put(Gossiper.STATES[key], value);
        }

        return new EndpointState(hbState, states);
    }

    public long serializedSize(EndpointState epState, int version)
    {
        long size = HeartBeatState.serializer.serializedSize(epState.getHeartBeatState(), version);
        Set<Map.Entry<ApplicationState, VersionedValue>> states = epState.states();
        size += TypeSizes.sizeof(states.size());
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
        {
            VersionedValue value = state.getValue();
            size += TypeSizes.sizeof(state.getKey().ordinal());
            size += VersionedValue.serializer.serializedSize(value, version);
        }
        return size;
    }
}
