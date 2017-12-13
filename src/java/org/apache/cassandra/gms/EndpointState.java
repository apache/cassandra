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
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */


public class EndpointState
{
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    public final static IVersionedSerializer<EndpointState> serializer = new EndpointStateSerializer();

    private volatile HeartBeatState hbState;
    private final AtomicReference<Map<ApplicationState, VersionedValue>> applicationState;

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    EndpointState(HeartBeatState initialHbState)
    {
        this(initialHbState, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states)
    {
        hbState = initialHbState;
        applicationState = new AtomicReference<Map<ApplicationState, VersionedValue>>(new EnumMap<>(states));
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    HeartBeatState getHeartBeatState()
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
        updateTimestamp = System.nanoTime();
    }

    public boolean isAlive()
    {
        return isAlive;
    }

    void markAlive()
    {
        isAlive = true;
    }

    void markDead()
    {
        isAlive = false;
    }

    public boolean isRpcReady()
    {
        VersionedValue rpcState = getApplicationState(ApplicationState.RPC_READY);
        return rpcState != null && Boolean.parseBoolean(rpcState.value);
    }

    public String getStatus()
    {
        VersionedValue status = getApplicationState(ApplicationState.STATUS);
        if (status == null)
            return "";

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
