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
import java.util.*;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndPointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class EndPointState
{
    private static ICompactSerializer<EndPointState> serializer_;
    static
    {
        serializer_ = new EndPointStateSerializer();
    }
    
    HeartBeatState hbState_;
    Map<String, ApplicationState> applicationState_ = new Hashtable<String, ApplicationState>();
    
    /* fields below do not get serialized */
    long updateTimestamp_;
    boolean isAlive_;
    boolean isAGossiper_;

    public static ICompactSerializer<EndPointState> serializer()
    {
        return serializer_;
    }
    
    EndPointState(HeartBeatState hbState) 
    { 
        hbState_ = hbState; 
        updateTimestamp_ = System.currentTimeMillis(); 
        isAlive_ = true; 
        isAGossiper_ = false;
    }
        
    HeartBeatState getHeartBeatState()
    {
        return hbState_;
    }
    
    synchronized void setHeartBeatState(HeartBeatState hbState)
    {
        updateTimestamp();
        hbState_ = hbState;
    }
    
    public ApplicationState getApplicationState(String key)
    {
        return applicationState_.get(key);
    }
    
    public Map<String, ApplicationState> getApplicationState()
    {
        return applicationState_;
    }
    
    void addApplicationState(String key, ApplicationState appState)
    {        
        applicationState_.put(key, appState);        
    }

    /* getters and setters */
    long getUpdateTimestamp()
    {
        return updateTimestamp_;
    }
    
    synchronized void updateTimestamp()
    {
        updateTimestamp_ = System.currentTimeMillis();
    }
    
    public boolean isAlive()
    {        
        return isAlive_;
    }

    synchronized void isAlive(boolean value)
    {        
        isAlive_ = value;        
    }

    
    boolean isAGossiper()
    {        
        return isAGossiper_;
    }

    synchronized void isAGossiper(boolean value)
    {                
        //isAlive_ = false;
        isAGossiper_ = value;        
    }
}

class EndPointStateSerializer implements ICompactSerializer<EndPointState>
{
    private static Logger logger_ = Logger.getLogger(EndPointStateSerializer.class);
    
    public void serialize(EndPointState epState, DataOutputStream dos) throws IOException
    {
        /* These are for estimating whether we overshoot the MTU limit */
        int estimate = 0;

        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer().serialize(hbState, dos);

        /* serialize the map of ApplicationState objects */
        int size = epState.applicationState_.size();
        dos.writeInt(size);
        if ( size > 0 )
        {   
            Set<String> keys = epState.applicationState_.keySet();
            for( String key : keys )
            {
                if ( Gossiper.MAX_GOSSIP_PACKET_SIZE - dos.size() < estimate )
                {
                    logger_.info("@@@@ Breaking out to respect the MTU size in EndPointState serializer. Estimate is " + estimate + " @@@@");
                    break;
                }
            
                ApplicationState appState = epState.applicationState_.get(key);
                if ( appState != null )
                {
                    int pre = dos.size();
                    dos.writeUTF(key);
                    ApplicationState.serializer().serialize(appState, dos);                    
                    int post = dos.size();
                    estimate = post - pre;
                }                
            }
        }
    }

    public EndPointState deserialize(DataInputStream dis) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer().deserialize(dis);
        EndPointState epState = new EndPointState(hbState);               

        int appStateSize = dis.readInt();
        for ( int i = 0; i < appStateSize; ++i )
        {
            if ( dis.available() == 0 )
            {
                break;
            }
            
            String key = dis.readUTF();    
            ApplicationState appState = ApplicationState.serializer().deserialize(dis);            
            epState.addApplicationState(key, appState);            
        }
        return epState;
    }
}
