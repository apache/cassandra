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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;


/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster. 
 * Whenever a peice of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 *  
 * For eg. if we want to disseminate load information for node A do the following:
 * 
 *      ApplicationState loadState = new ApplicationState(<string reprensentation of load>);
 *      Gossiper.instance().addApplicationState("LOAD STATE", loadState);
 *  
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ApplicationState
{
    private static ICompactSerializer<ApplicationState> serializer_;
    static
    {
        serializer_ = new ApplicationStateSerializer();
    }
    
    int version_;
    String state_;

        
    ApplicationState(String state, int version)
    {
        state_ = state;
        version_ = version;
    }

    public static ICompactSerializer<ApplicationState> serializer()
    {
        return serializer_;
    }
    
    /**
     * Wraps the specified state into a ApplicationState instance.
     * @param state string representation of arbitrary state.
     */
    public ApplicationState(String state)
    {
        state_ = state;
        version_ = VersionGenerator.getNextVersion();
    }
        
    public String getState()
    {
        return state_;
    }
    
    int getStateVersion()
    {
        return version_;
    }
}

class ApplicationStateSerializer implements ICompactSerializer<ApplicationState>
{
    public void serialize(ApplicationState appState, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(appState.state_);
        dos.writeInt(appState.version_);
    }

    public ApplicationState deserialize(DataInputStream dis) throws IOException
    {
        String state = dis.readUTF();
        int version = dis.readInt();
        return new ApplicationState(state, version);
    }
}

