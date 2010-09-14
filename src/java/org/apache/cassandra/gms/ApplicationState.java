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
import java.util.UUID;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.StorageService;


/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster. 
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 *  
 * e.g. if we want to disseminate load information for node A do the following:
 * 
 *      ApplicationState loadState = new ApplicationState(<string representation of load>);
 *      Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 */

public class ApplicationState implements Comparable<ApplicationState>
{
    public static final ICompactSerializer<ApplicationState> serializer = new ApplicationStateSerializer();

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[] { DELIMITER });

    public final static String STATE_MOVE = "MOVE";
    public final static String STATE_BOOTSTRAPPING = "BOOT";
    public final static String STATE_NORMAL = "NORMAL";
    public final static String STATE_LEAVING = "LEAVING";
    public final static String STATE_LEFT = "LEFT";
    public final static String STATE_LOAD = "LOAD";
    public static final String STATE_MIGRATION = "MIGRATION";

    public final static String REMOVE_TOKEN = "remove";

    public final int version;
    public final String state;

    private ApplicationState(String state, int version)
    {
        this.state = state;
        this.version = version;
    }

    private ApplicationState(String state)
    {
        this.state = state;
        version = VersionGenerator.getNextVersion();
    }

    public int compareTo(ApplicationState apState)
    {
        return this.version - apState.version;
    }

    public static class ApplicationStateFactory
    {
        IPartitioner partitioner;

        public ApplicationStateFactory(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }

        public ApplicationState bootstrapping(Token token)
        {
            return new ApplicationState(ApplicationState.STATE_BOOTSTRAPPING + ApplicationState.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public ApplicationState normal(Token token)
        {
            return new ApplicationState(ApplicationState.STATE_NORMAL + ApplicationState.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public ApplicationState load(double load)
        {
            return new ApplicationState(String.valueOf(load));
        }

        public ApplicationState migration(UUID newVersion)
        {
            return new ApplicationState(newVersion.toString());
        }

        public ApplicationState leaving(Token token)
        {
            return new ApplicationState(ApplicationState.STATE_LEAVING + ApplicationState.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public ApplicationState left(Token token)
        {
            return new ApplicationState(ApplicationState.STATE_LEFT + ApplicationState.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public ApplicationState removeNonlocal(Token localToken, Token token)
        {
            return new ApplicationState(ApplicationState.STATE_NORMAL
                                        + ApplicationState.DELIMITER + partitioner.getTokenFactory().toString(localToken)
                                        + ApplicationState.DELIMITER + ApplicationState.REMOVE_TOKEN
                                        + ApplicationState.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

    }

    private static class ApplicationStateSerializer implements ICompactSerializer<ApplicationState>
    {
        public void serialize(ApplicationState appState, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(appState.state);
            dos.writeInt(appState.version);
        }

        public ApplicationState deserialize(DataInputStream dis) throws IOException
        {
            String state = dis.readUTF();
            int version = dis.readInt();
            return new ApplicationState(state, version);
        }
    }
}

