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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;


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
    public static final IVersionedSerializer<VersionedValue> serializer = new VersionedValueSerializer();

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[] { DELIMITER });

    // values for ApplicationState.STATUS
    public final static String STATUS_BOOTSTRAPPING = "BOOT";
    public final static String STATUS_NORMAL = "NORMAL";
    public final static String STATUS_LEAVING = "LEAVING";
    public final static String STATUS_LEFT = "LEFT";
    public final static String STATUS_MOVING = "MOVING";

    public final static String REMOVING_TOKEN = "removing";
    public final static String REMOVED_TOKEN = "removed";

    public final static String HIBERNATE = "hibernate";

    // values for ApplicationState.REMOVAL_COORDINATOR
    public final static String REMOVAL_COORDINATOR = "REMOVER";
    // network proto version from MS
    public final static String NET_VERSION = "NET_VERSION";

    public final int version;
    public final String value;

    private VersionedValue(String value, int version)
    {
        assert value != null;
        this.value = value;
        this.version = version;
    }

    private VersionedValue(String value)
    {
        this(value, VersionGenerator.getNextVersion());
    }

    public int compareTo(VersionedValue value)
    {
        return this.version - value.version;
    }

    @Override
    public String toString()
    {
        return "Value(" + value + "," + version + ")";
    }

    private static String versionString(String...args)
    {
        return StringUtils.join(args, VersionedValue.DELIMITER);
    }

    public static class VersionedValueFactory
    {
        final IPartitioner partitioner;

        public VersionedValueFactory(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }

        public VersionedValue bootstrapping(Collection<Token> tokens, UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING,
                                                    hostId.toString(),
                                                    makeTokenString(tokens)));
        }

        public VersionedValue normal(Collection<Token> tokens, UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_NORMAL,
                                                    hostId.toString(),
                                                    makeTokenString(tokens)));
        }

        private String makeTokenString(Collection<Token> tokens)
        {
            List<String> tokenStrings = new ArrayList<String>();
            for (Token<?> token : tokens)
                tokenStrings.add(partitioner.getTokenFactory().toString(token));
            return StringUtils.join(tokenStrings, VersionedValue.DELIMITER);
        }

        public VersionedValue load(double load)
        {
            return new VersionedValue(String.valueOf(load));
        }

        public VersionedValue migration(UUID newVersion)
        {
            return new VersionedValue(newVersion.toString());
        }

        public VersionedValue leaving(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEAVING,
                                                    makeTokenString(tokens)));
        }

        public VersionedValue left(Collection<Token> tokens, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEFT,
                                                    Long.toString(expireTime),
                                                    makeTokenString(tokens)));
        }

        public VersionedValue moving(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_MOVING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue removingNonlocal(UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVING_TOKEN, hostId.toString()));
        }

        public VersionedValue removedNonlocal(UUID hostId, long expireTime)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVED_TOKEN, hostId.toString(), Long.toString(expireTime)));
        }

        public VersionedValue removalCoordinator(UUID hostId)
        {
            return new VersionedValue(versionString(VersionedValue.REMOVAL_COORDINATOR, hostId.toString()));
        }

        public VersionedValue hibernate(boolean value)
        {
            return new VersionedValue(VersionedValue.HIBERNATE + VersionedValue.DELIMITER + value);
        }

        public VersionedValue datacenter(String dcId)
        {
            return new VersionedValue(dcId);
        }

        public VersionedValue rack(String rackId)
        {
            return new VersionedValue(rackId);
        }

        public VersionedValue rpcaddress(InetAddress endpoint)
        {
            return new VersionedValue(endpoint.getHostAddress());
        }

        public VersionedValue releaseVersion()
        {
            return new VersionedValue(FBUtilities.getReleaseVersionString());
        }

        public VersionedValue networkVersion()
        {
            return new VersionedValue(String.valueOf(MessagingService.current_version));
        }

        public VersionedValue internalIP(String private_ip)
        {
            return new VersionedValue(private_ip);
        }

        public VersionedValue severity(double value)
        {
            return new VersionedValue(String.valueOf(value));
        }
    }

    private static class VersionedValueSerializer implements IVersionedSerializer<VersionedValue>
    {
        public void serialize(VersionedValue value, DataOutput dos, int version) throws IOException
        {
            dos.writeUTF(outValue(value, version));
            dos.writeInt(value.version);
        }

        private String outValue(VersionedValue value, int version)
        {
            String outValue = value.value;

            if (version < MessagingService.VERSION_12)
            {
                String[] pieces = value.value.split(DELIMITER_STR, -1);
                String type = pieces[0];

                if ((type.equals(STATUS_NORMAL)) || type.equals(STATUS_BOOTSTRAPPING))
                {
                    assert pieces.length >= 3;
                    outValue = versionString(pieces[0], pieces[2]);
                }

                if (type.equals(STATUS_LEFT))
                {
                    assert pieces.length >= 3;
                    outValue = versionString(pieces[0], pieces[2], pieces[1]);
                }

                if ((type.equals(REMOVAL_COORDINATOR)) || (type.equals(REMOVING_TOKEN)) || (type.equals(REMOVED_TOKEN)))
                    throw new RuntimeException(String.format("Unable to serialize %s(%s...) for nodes older than 1.2",
                                                             VersionedValue.class.getName(), type));
            }
            return outValue;
        }

        public VersionedValue deserialize(DataInput dis, int version) throws IOException
        {
            String value = dis.readUTF();
            int valVersion = dis.readInt();
            return new VersionedValue(value, valVersion);
        }

        public long serializedSize(VersionedValue value, int version)
        {
            return TypeSizes.NATIVE.sizeof(outValue(value, version)) + TypeSizes.NATIVE.sizeof(value.version);
        }
    }
}

