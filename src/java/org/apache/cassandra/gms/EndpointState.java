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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.ICompactSerializer;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */

public class EndpointState
{
    protected static Logger logger = LoggerFactory.getLogger(EndpointState.class);

    private final static ICompactSerializer<EndpointState> serializer_ = new EndpointStateSerializer();

    volatile HeartBeatState hbState_;
    final Map<ApplicationState, VersionedValue> applicationState_ = new NonBlockingHashMap<ApplicationState, VersionedValue>();
    
    /* fields below do not get serialized */
    volatile long updateTimestamp_;
    volatile boolean isAlive_;
    volatile boolean isAGossiper_;

    // whether this endpoint has token associated with it or not. Initially set false for all
    // endpoints. After certain time of inactivity, gossiper will examine if this node has a
    // token or not and will set this true if token is found. If there is no token, this is a
    // fat client and will be removed automatically from gossip.
    volatile boolean hasToken_;

    public static ICompactSerializer<EndpointState> serializer()
    {
        return serializer_;
    }
    
    EndpointState(HeartBeatState hbState)
    { 
        hbState_ = hbState; 
        updateTimestamp_ = System.currentTimeMillis(); 
        isAlive_ = true; 
        isAGossiper_ = false;
        hasToken_ = false;
    }
        
    HeartBeatState getHeartBeatState()
    {
        return hbState_;
    }
    
    void setHeartBeatState(HeartBeatState hbState)
    {
        updateTimestamp();
        hbState_ = hbState;
    }

    public VersionedValue getApplicationState(ApplicationState key)
    {
        return applicationState_.get(key);
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    @Deprecated
    public Map<ApplicationState, VersionedValue> getApplicationStateMap()
    {
        return applicationState_;
    }
    
    void addApplicationState(ApplicationState key, VersionedValue value)
    {
        applicationState_.put(key, value);
    }

    /* getters and setters */
    long getUpdateTimestamp()
    {
        return updateTimestamp_;
    }
    
    void updateTimestamp()
    {
        updateTimestamp_ = System.currentTimeMillis();
    }
    
    public boolean isAlive()
    {        
        return isAlive_;
    }

    void isAlive(boolean value)
    {        
        isAlive_ = value;        
    }

    
    boolean isAGossiper()
    {        
        return isAGossiper_;
    }

    void isAGossiper(boolean value)
    {                
        //isAlive_ = false;
        isAGossiper_ = value;        
    }

    public void setHasToken(boolean value)
    {
        hasToken_ = value;
    }

    public boolean hasToken()
    {
        return hasToken_;
    }
}

class EndpointStateSerializer implements ICompactSerializer<EndpointState>
{
    private static Logger logger_ = LoggerFactory.getLogger(EndpointStateSerializer.class);
    
    public void serialize(EndpointState epState, DataOutputStream dos) throws IOException
    {
        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer().serialize(hbState, dos);

        /* serialize the map of ApplicationState objects */
        int size = epState.applicationState_.size();
        dos.writeInt(size);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.applicationState_.entrySet())
        {
            VersionedValue value = entry.getValue();
            if (value != null)
            {
                dos.writeInt(entry.getKey().ordinal());
                VersionedValue.serializer.serialize(value, dos);
            }
        }
    }

    public EndpointState deserialize(DataInputStream dis) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer().deserialize(dis);
        EndpointState epState = new EndpointState(hbState);

        int appStateSize = dis.readInt();
        for ( int i = 0; i < appStateSize; ++i )
        {
            if ( dis.available() == 0 )
            {
                break;
            }

            int key = dis.readInt();
            VersionedValue value = VersionedValue.serializer.deserialize(dis);
            epState.addApplicationState(Gossiper.STATES[key], value);
        }
        return epState;
    }
}
