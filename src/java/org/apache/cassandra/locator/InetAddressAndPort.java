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
package org.apache.cassandra.locator;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FastByteOperations;

/**
 * A class to replace the usage of InetAddress to identify hosts in the cluster.
 * Opting for a full replacement class so that in the future if we change the nature
 * of the identifier the refactor will be easier in that we don't have to change the type
 * just the methods.
 *
 * Because an IP might contain multiple C* instances the identification must be done
 * using the IP + port. InetSocketAddress is undesirable for a couple of reasons. It's not comparable,
 * it's toString() method doesn't correctly bracket IPv6, it doesn't handle optional default values,
 * and a couple of other minor behaviors that are slightly less troublesome like handling the
 * need to sometimes return a port and sometimes not.
 *
 */
public final class InetAddressAndPort extends InetSocketAddress implements Comparable<InetAddressAndPort>, Serializable
{
    private static final long serialVersionUID = 0;

    //Store these here to avoid requiring DatabaseDescriptor to be loaded. DatabaseDescriptor will set
    //these when it loads the config. A lot of unit tests won't end up loading DatabaseDescriptor.
    //Tools that might use this class also might not load database descriptor. Those tools are expected
    //to always override the defaults.
    static volatile int defaultPort = 7000;

    public final byte[] addressBytes;

    @VisibleForTesting
    InetAddressAndPort(InetAddress address, byte[] addressBytes, int port)
    {
        super(address, port);
        Preconditions.checkNotNull(address);
        Preconditions.checkNotNull(addressBytes);
        validatePortRange(port);
        this.addressBytes = addressBytes;
    }

    public InetAddressAndPort withPort(int port)
    {
        return new InetAddressAndPort(getAddress(), addressBytes, port);
    }

    private static void validatePortRange(int port)
    {
        if (port < 0 | port > 65535)
        {
            throw new IllegalArgumentException("Port " + port + " is not a valid port number in the range 0-65535");
        }
    }

    @Override
    public int compareTo(InetAddressAndPort o)
    {
        int retval = FastByteOperations.compareUnsigned(addressBytes, 0, addressBytes.length, o.addressBytes, 0, o.addressBytes.length);
        if (retval != 0)
        {
            return retval;
        }

        return Integer.compare(getPort(), o.getPort());
    }

    public String getHostAddressAndPort()
    {
        return getHostAddress(true);
    }

    private static final Pattern JMX_INCOMPATIBLE_CHARS = Pattern.compile("[\\[\\]:]");


    /**
     * Return a version of getHostAddressAndPort suitable for use in JMX object names without
     * requiring any escaping.  Replaces each character invalid for JMX names with an underscore.
     *
     * @return String with JMX-safe representation of the IP address and port
     */
    public String getHostAddressAndPortForJMX()
    {
        return JMX_INCOMPATIBLE_CHARS.matcher(getHostAddressAndPort()).replaceAll("_");
    }

    public String getHostAddress(boolean withPort)
    {
        return hostAddress(this, withPort);
    }

    public String getHostName(boolean withPort)
    {
        return withPort ? String.format("%s:%s", getHostName(), getPort()) : getHostName();
    }

    public static String hostAddressAndPort(InetSocketAddress address)
    {
        return hostAddress(address, true);
    }

    public static String hostAddress(InetSocketAddress address, boolean withPort)
    {
        if (withPort)
        {
            return HostAndPort.fromParts(address.getAddress().getHostAddress(), address.getPort()).toString();
        }
        else
        {
            return address.getAddress().getHostAddress();
        }
    }

    @Override
    public String toString()
    {
        return toString(this);
    }

    public String toString(boolean withPort)
    {
        return toString(this, withPort);
    }

    public static String toString(InetSocketAddress address)
    {
        return toString(address, true);
    }

    public static String toString(InetSocketAddress address, boolean withPort)
    {
        if (withPort)
        {
            return toString(address.getAddress(), address.getPort());
        }
        else
        {
            return address.getAddress().toString();
        }
    }

    /** Format an InetAddressAndPort in the same style as InetAddress.toString.
     *  The string returned is of the form: hostname / literal IP address : port
     *  (without the whitespace). Literal IPv6 addresses will be wrapped with [ ]
     *  to make the port number clear.
     *
     *  If the host name is unresolved, no reverse name service lookup
     *  is performed. The hostname part will be represented by an empty string.
     *
     * @param address InetAddress to convert String
     * @param port Port number to convert to String
     * @return String representation of the IP address and port
     */
    public static String toString(InetAddress address, int port)
    {
        String addressToString = address.toString(); // cannot use getHostName as it resolves
        int nameLength = addressToString.lastIndexOf('/'); // use last index to prevent ambiguity if host name contains /
        assert nameLength >= 0 : "InetAddress.toString format may have changed, expecting /";

        // Check if need to wrap address with [ ] for IPv6 addresses
        if (addressToString.indexOf(':', nameLength) >= 0)
        {
            StringBuilder sb = new StringBuilder(addressToString.length() + 16);
            sb.append(addressToString, 0, nameLength + 1); // append optional host and / char
            sb.append('[');
            sb.append(addressToString, nameLength + 1, addressToString.length());
            sb.append("]:");
            sb.append(port);
            return sb.toString();
        }
        else // can just append :port
        {
            StringBuilder sb = new StringBuilder(addressToString); // will have enough capacity for port
            sb.append(":");
            sb.append(port);
            return sb.toString();
        }
    }

    public static InetAddressAndPort getByName(String name) throws UnknownHostException
    {
        return getByNameOverrideDefaults(name, null);
    }


    public static List<InetAddressAndPort> getAllByName(String name) throws UnknownHostException
    {
        return getAllByNameOverrideDefaults(name, null);
    }

    /**
     *
     * @param name Hostname + optional ports string
     * @param port Port to connect on, overridden by values in hostname string, defaults to DatabaseDescriptor default if not specified anywhere.
     */
    public static List<InetAddressAndPort> getAllByNameOverrideDefaults(String name, Integer port) throws UnknownHostException
    {
        HostAndPort hap = HostAndPort.fromString(name);
        if (hap.hasPort())
        {
            port = hap.getPort();
        }
        Integer finalPort = port;

        return Stream.of(InetAddress.getAllByName(hap.getHost()))
                     .map((address) -> getByAddressOverrideDefaults(address, finalPort))
                     .collect(Collectors.toList());
    }

    /**
     *
     * @param name Hostname + optional ports string
     * @param port Port to connect on, overridden by values in hostname string, defaults to DatabaseDescriptor default if not specified anywhere.
     */
    public static InetAddressAndPort getByNameOverrideDefaults(String name, Integer port) throws UnknownHostException
    {
        HostAndPort hap = HostAndPort.fromString(name);
        if (hap.hasPort())
        {
            port = hap.getPort();
        }
        return getByAddressOverrideDefaults(InetAddress.getByName(hap.getHost()), port);
    }

    public static InetAddressAndPort getByAddress(byte[] address) throws UnknownHostException
    {
        return getByAddressOverrideDefaults(InetAddress.getByAddress(address), address, null);
    }

    public static InetAddressAndPort getByAddress(InetAddress address)
    {
        return getByAddressOverrideDefaults(address, null);
    }

    public static InetAddressAndPort getByAddress(InetSocketAddress address)
    {
        if (address instanceof InetAddressAndPort)
            return (InetAddressAndPort) address;
        return new InetAddressAndPort(address.getAddress(), address.getAddress().getAddress(), address.getPort());
    }

    public static InetAddressAndPort getByAddressOverrideDefaults(InetAddress address, Integer port)
    {
        if (port == null)
        {
            port = defaultPort;
        }

        return new InetAddressAndPort(address, address.getAddress(), port);
    }

    public static InetAddressAndPort getByAddressOverrideDefaults(InetAddress address, byte[] addressBytes, Integer port)
    {
        if (port == null)
        {
            port = defaultPort;
        }

        return new InetAddressAndPort(address, addressBytes, port);
    }

    public static InetAddressAndPort getLoopbackAddress()
    {
        return InetAddressAndPort.getByAddress(InetAddress.getLoopbackAddress());
    }

    public static InetAddressAndPort getLocalHost()
    {
        return FBUtilities.getLocalAddressAndPort();
    }

    public static void initializeDefaultPort(int port)
    {
        defaultPort = port;
    }

    static int getDefaultPort()
    {
        return defaultPort;
    }

    /**
     * As of version 4.0 the endpoint description includes a port number as an unsigned short
     */
    public static final class Serializer implements IVersionedSerializer<InetAddressAndPort>
    {
        public static final int MAXIMUM_SIZE = 19;

        // We put the static instance here, to avoid complexity with dtests.
        // InetAddressAndPort is one of the only classes we share between instances, which is possible cleanly
        // because it has no type-dependencies in its public API, however Serializer requires DataOutputPlus, which requires...
        // and the chain becomes quite unwieldy
        public static final Serializer inetAddressAndPortSerializer = new Serializer();

        private Serializer() {}

        public void serialize(InetAddressAndPort endpoint, DataOutputPlus out, int version) throws IOException
        {
            serialize(endpoint.addressBytes, endpoint.getPort(), out, version);
        }

        public void serialize(InetSocketAddress endpoint, DataOutputPlus out, int version) throws IOException
        {
            byte[] address = endpoint instanceof InetAddressAndPort ? ((InetAddressAndPort) endpoint).addressBytes : endpoint.getAddress().getAddress();
            serialize(address, endpoint.getPort(), out, version);
        }

        void serialize(byte[] address, int port, DataOutputPlus out, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40;
            out.writeByte(address.length + 2);
            out.write(address);
            out.writeShort(port);
        }

        public InetAddressAndPort deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readByte() & 0xFF;
            switch(size)
            {
                //The original pre-4.0 serialiation of just an address
                case 4:
                case 16:
                    throw new AssertionError("pre-4.0 serialization size " + size);
                //Address and one port
                case 6:
                case 18:
                {
                    byte[] bytes = new byte[size - 2];
                    in.readFully(bytes);

                    int port = in.readShort() & 0xFFFF;
                    return getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), bytes, port);
                }
                default:
                    throw new AssertionError("Unexpected size " + size);

            }
        }

        /**
         * Extract {@link InetAddressAndPort} from the provided {@link ByteBuffer} without altering its state.
         */
        public InetAddressAndPort extract(ByteBuffer buf, int position) throws IOException
        {
            int size = buf.get(position++) & 0xFF;
            if (size == 6 || size == 18)
            {
                byte[] bytes = new byte[size - 2];
                ByteBufferUtil.copyBytes(buf, position, bytes, 0, size - 2);
                position += (size - 2);
                int port = buf.getShort(position) & 0xFFFF;
                return getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), bytes, port);
            }

            throw new AssertionError("Unexpected pre-4.0 InetAddressAndPort size " + size);
        }

        public long serializedSize(InetAddressAndPort from, int version)
        {
            return serializedSize((InetSocketAddress) from, version);
        }

        public long serializedSize(InetSocketAddress from, int version)
        {
            assert version >= MessagingService.VERSION_40;
            if (from.getAddress() instanceof Inet4Address)
                return 1 + 4 + 2;
            assert from.getAddress() instanceof Inet6Address;
            return 1 + 16 + 2;
        }
    }

    /** Serializer for handling FWD_FRM message parameters. 
     */
    public static final class FwdFrmSerializer implements IVersionedSerializer<InetAddressAndPort>
    {
        public static final FwdFrmSerializer fwdFrmSerializer = new FwdFrmSerializer();
        private FwdFrmSerializer() { }

        public void serialize(InetAddressAndPort endpoint, DataOutputPlus out, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40;
            byte[] buf = endpoint.addressBytes;
            out.writeByte(buf.length + 2);
            out.write(buf);
            out.writeShort(endpoint.getPort());
        }

        public long serializedSize(InetAddressAndPort from, int version)
        {
            assert version >= MessagingService.VERSION_40;
            if (from.getAddress() instanceof Inet4Address)
                return 1 + 4 + 2;
            assert from.getAddress() instanceof Inet6Address;
            return 1 + 16 + 2;
        }

        @Override
        public InetAddressAndPort deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version >= MessagingService.VERSION_40 : "FWD_FRM deserializations should be special-cased pre-4.0";
            int size = in.readByte() & 0xFF;
            switch (size)
            {
                //Address and one port
                case 6:
                case 18:
                {
                    byte[] bytes = new byte[size - 2];
                    in.readFully(bytes);

                    int port = in.readShort() & 0xFFFF;
                    return getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), bytes, port);
                }
                default:
                    throw new AssertionError("Unexpected size " + size);
            }
        }

    }
}
