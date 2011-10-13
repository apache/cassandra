/**
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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.IVersionedSerializer;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */


public class EndpointState
{
    protected static Logger logger = LoggerFactory.getLogger(EndpointState.class);

    private final static IVersionedSerializer<EndpointState> serializer = new EndpointStateSerializer();

    private volatile HeartBeatState hbState;
    final Map<ApplicationState, VersionedValue> applicationState = new NonBlockingHashMap<ApplicationState, VersionedValue>();
    
    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    // whether this endpoint has token associated with it or not. Initially set false for all
    // endpoints. After certain time of inactivity, gossiper will examine if this node has a
    // token or not and will set this true if token is found. If there is no token, this is a
    // fat client and will be removed automatically from gossip.
    private volatile boolean hasToken;

    public static IVersionedSerializer<EndpointState> serializer()
    {
        return serializer;
    }
    
    EndpointState(HeartBeatState initialHbState)
    { 
        hbState = initialHbState;
        updateTimestamp = System.currentTimeMillis();
        isAlive = true;
        hasToken = false;
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
        return applicationState.get(key);
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    @Deprecated
    public Map<ApplicationState, VersionedValue> getApplicationStateMap()
    {
        return applicationState;
    }
    
    void addApplicationState(ApplicationState key, VersionedValue value)
    {
        applicationState.put(key, value);
    }

    /* getters and setters */
    public long getUpdateTimestamp()
    {
        return updateTimestamp;
    }
    
    void updateTimestamp()
    {
        updateTimestamp = System.currentTimeMillis();
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

    void setHasToken(boolean value)
    {
        hasToken = value;
    }

    boolean hasToken()
    {
        return hasToken;
    }
}

class EndpointStateSerializer implements IVersionedSerializer<EndpointState>
{
    private static Logger logger = LoggerFactory.getLogger(EndpointStateSerializer.class);
    
    public void serialize(EndpointState epState, DataOutput dos, int version) throws IOException
    {
        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer().serialize(hbState, dos, version);

        /* serialize the map of ApplicationState objects */
        int size = epState.applicationState.size();
        dos.writeInt(size);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.applicationState.entrySet())
        {
            VersionedValue value = entry.getValue();
            dos.writeInt(entry.getKey().ordinal());
            VersionedValue.serializer.serialize(value, dos, version);
        }
    }

    public EndpointState deserialize(DataInput dis, int version) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer().deserialize(dis, version);
        EndpointState epState = new EndpointState(hbState);

        int appStateSize = dis.readInt();
        for ( int i = 0; i < appStateSize; ++i )
        {
            int key = dis.readInt();
            VersionedValue value = VersionedValue.serializer.deserialize(dis, version);
            epState.addApplicationState(Gossiper.STATES[key], value);
        }
        return epState;
    }

    public long serializedSize(EndpointState endpointState, int version)
    {
        throw new UnsupportedOperationException();
    }
}
