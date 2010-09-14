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

public class VersionedValue implements Comparable<VersionedValue>
{
    public static final ICompactSerializer<VersionedValue> serializer = new VersionedValueSerializer();

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[] { DELIMITER });

    // values for State.STATUS
    public final static String STATUS_BOOTSTRAPPING = "BOOT";
    public final static String STATUS_NORMAL = "NORMAL";
    public final static String STATUS_LEAVING = "LEAVING";
    public final static String STATUS_LEFT = "LEFT";

    public final static String REMOVE_TOKEN = "remove";

    public final int version;
    public final String value;

    private VersionedValue(String value, int version)
    {
        this.value = value;
        this.version = version;
    }

    private VersionedValue(String value)
    {
        this.value = value;
        version = VersionGenerator.getNextVersion();
    }

    public int compareTo(VersionedValue value)
    {
        return this.version - value.version;
    }

    public static class VersionedValueFactory
    {
        IPartitioner partitioner;

        public VersionedValueFactory(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }

        public VersionedValue bootstrapping(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_BOOTSTRAPPING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue normal(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_NORMAL + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue load(double load)
        {
            return new VersionedValue(String.valueOf(load));
        }

        public VersionedValue migration(UUID newVersion)
        {
            return new VersionedValue(newVersion.toString());
        }

        public VersionedValue leaving(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_LEAVING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue left(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_LEFT + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue removeNonlocal(Token localToken, Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_NORMAL
                                        + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(localToken)
                                        + VersionedValue.DELIMITER + VersionedValue.REMOVE_TOKEN
                                        + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

    }

    private static class VersionedValueSerializer implements ICompactSerializer<VersionedValue>
    {
        public void serialize(VersionedValue value, DataOutputStream dos) throws IOException
        {
            dos.writeUTF(value.value);
            dos.writeInt(value.version);
        }

        public VersionedValue deserialize(DataInputStream dis) throws IOException
        {
            String value = dis.readUTF();
            int version = dis.readInt();
            return new VersionedValue(value, version);
        }
    }
}

