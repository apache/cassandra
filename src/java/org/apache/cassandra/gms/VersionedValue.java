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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static java.nio.charset.StandardCharsets.ISO_8859_1;


/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 * <p>
 * e.g. if we want to disseminate load information for node A do the following:
 * </p>
 * <pre>
 * {@code
 * ApplicationState loadState = new ApplicationState(<string representation of load>);
 * Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 * }
 * </pre>
 */

public class VersionedValue implements Comparable<VersionedValue>
{

    public static final IVersionedSerializer<VersionedValue> serializer = new VersionedValueSerializer();

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[]{ DELIMITER });

    // values for ApplicationState.STATUS
    public final static String STATUS_BOOTSTRAPPING = "BOOT";
    public final static String STATUS_BOOTSTRAPPING_REPLACE = "BOOT_REPLACE";
    public final static String STATUS_NORMAL = "NORMAL";
    public final static String STATUS_LEAVING = "LEAVING";
    public final static String STATUS_LEFT = "LEFT";
    public final static String STATUS_MOVING = "MOVING";

    public final static String REMOVING_TOKEN = "removing";
    public final static String REMOVED_TOKEN = "removed";

    public final static String HIBERNATE = "hibernate";
    public final static String SHUTDOWN = "shutdown";

    // values for ApplicationState.REMOVAL_COORDINATOR
    public final static String REMOVAL_COORDINATOR = "REMOVER";

    public static Set<String> BOOTSTRAPPING_STATUS = ImmutableSet.of(STATUS_BOOTSTRAPPING, STATUS_BOOTSTRAPPING_REPLACE);

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

    @VisibleForTesting
    public VersionedValue withVersion(int version)
    {
        return new VersionedValue(value, version);
    }

    public static VersionedValue unsafeMakeVersionedValue(String value, int version)
    {
        return new VersionedValue(value, version);
    }

    public int compareTo(VersionedValue value)
    {
        return this.version - value.version;
    }

    @Override
    public String toString()
    {
        return "Value(" + value + ',' + version + ')';
    }

    public byte[] toBytes()
    {
        return value.getBytes(ISO_8859_1);
    }

    private static String versionString(String... args)
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
        
        public VersionedValue cloneWithHigherVersion(VersionedValue value)
        {
            return new VersionedValue(value.value);
        }

        /** @deprecated See CASSANDRA-7544 */
        @Deprecated(since = "4.0")
        public VersionedValue bootReplacing(InetAddress oldNode)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING_REPLACE, oldNode.getHostAddress()));
        }

        public VersionedValue bootReplacingWithPort(InetAddressAndPort oldNode)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING_REPLACE, oldNode.getHostAddressAndPort()));
        }

        public VersionedValue bootstrapping(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_BOOTSTRAPPING,
                                                    makeTokenString(tokens)));
        }

        public VersionedValue normal(Collection<Token> tokens)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_NORMAL,
                                                    makeTokenString(tokens)));
        }

        private String makeTokenString(Collection<Token> tokens)
        {
            return partitioner.getTokenFactory().toString(Iterables.get(tokens, 0));
        }

        public VersionedValue load(double load)
        {
            return new VersionedValue(String.valueOf(load));
        }

        public VersionedValue diskUsage(String state)
        {
            return new VersionedValue(state);
        }

        public VersionedValue schema(UUID newVersion)
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
                                                    makeTokenString(tokens),
                                                    Long.toString(expireTime)));
        }

        @VisibleForTesting
        public VersionedValue left(Collection<Token> tokens, long expireTime, int generation)
        {
            return new VersionedValue(versionString(VersionedValue.STATUS_LEFT,
                                                    makeTokenString(tokens),
                                                    Long.toString(expireTime)), generation);
        }

        public VersionedValue moving(Token token)
        {
            return new VersionedValue(VersionedValue.STATUS_MOVING + VersionedValue.DELIMITER + partitioner.getTokenFactory().toString(token));
        }

        public VersionedValue hostId(UUID hostId)
        {
            return new VersionedValue(hostId.toString());
        }

        public VersionedValue tokens(Collection<Token> tokens)
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            try
            {
                TokenSerializer.serialize(partitioner, tokens, out);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            return new VersionedValue(new String(bos.toByteArray(), ISO_8859_1));
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

        public VersionedValue rpcReady(boolean value)
        {
            return new VersionedValue(String.valueOf(value));
        }

        public VersionedValue shutdown(boolean value)
        {
            return new VersionedValue(VersionedValue.SHUTDOWN + VersionedValue.DELIMITER + value);
        }

        public VersionedValue indexStatus(String status)
        {
            return new VersionedValue(status);
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

        public VersionedValue nativeaddressAndPort(InetAddressAndPort address)
        {
            return new VersionedValue(address.getHostAddressAndPort());
        }

        public VersionedValue releaseVersion()
        {
            return new VersionedValue(FBUtilities.getReleaseVersionString());
        }

        @VisibleForTesting
        public VersionedValue releaseVersion(String version)
        {
            return new VersionedValue(version);
        }

        @VisibleForTesting
        public VersionedValue networkVersion(int version)
        {
            return new VersionedValue(String.valueOf(version));
        }

        public VersionedValue networkVersion()
        {
            return new VersionedValue(String.valueOf(MessagingService.current_version));
        }

        public VersionedValue internalIP(InetAddress private_ip)
        {
            return new VersionedValue(private_ip.getHostAddress());
        }

        public VersionedValue internalAddressAndPort(InetAddressAndPort private_ip_and_port)
        {
            return new VersionedValue(private_ip_and_port.getHostAddressAndPort());
        }

        public VersionedValue severity(double value)
        {
            return new VersionedValue(String.valueOf(value));
        }

        public VersionedValue sstableVersions(Set<Version> versions)
        {
            return new VersionedValue(versions.stream()
                                              .map(Version::toFormatAndVersionString)
                                              .collect(Collectors.joining(",")));
        }
    }

    private static class VersionedValueSerializer implements IVersionedSerializer<VersionedValue>
    {
        public void serialize(VersionedValue value, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(outValue(value, version));
            out.writeInt(value.version);
        }

        private String outValue(VersionedValue value, int version)
        {
            return value.value;
        }

        public VersionedValue deserialize(DataInputPlus in, int version) throws IOException
        {
            String value = in.readUTF();
            int valVersion = in.readInt();
            return new VersionedValue(value, valVersion);
        }

        public long serializedSize(VersionedValue value, int version)
        {
            return TypeSizes.sizeof(outValue(value, version)) + TypeSizes.sizeof(value.version);
        }
    }
}

