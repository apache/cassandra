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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

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
@SuppressWarnings("UnstableApiUsage")
public final class InetAddressAndPort implements Comparable<InetAddressAndPort>, Serializable
{
    private static final long serialVersionUID = 0;

    //Store these here to avoid requiring DatabaseDescriptor to be loaded. DatabaseDescriptor will set
    //these when it loads the config. A lot of unit tests won't end up loading DatabaseDescriptor.
    //Tools that might use this class also might not load database descriptor. Those tools are expected
    //to always override the defaults.
    static volatile int defaultPort = 7000;

    public final InetAddress address;
    public final byte[] addressBytes;
    public final int port;

    private InetAddressAndPort(InetAddress address, byte[] addressBytes, int port)
    {
        Preconditions.checkNotNull(address);
        Preconditions.checkNotNull(addressBytes);
        validatePortRange(port);
        this.address = address;
        this.port = port;
        this.addressBytes = addressBytes;
    }

    public InetAddressAndPort withPort(int port)
    {
        return new InetAddressAndPort(address, addressBytes, port);
    }

    private static void validatePortRange(int port)
    {
        if (port < 0 | port > 65535)
        {
            throw new IllegalArgumentException("Port " + port + " is not a valid port number in the range 0-65535");
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InetAddressAndPort that = (InetAddressAndPort) o;

        if (port != that.port) return false;
        return address.equals(that.address);
    }

    @Override
    public int hashCode()
    {
        int result = address.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public int compareTo(InetAddressAndPort o)
    {
        int retval = FastByteOperations.compareUnsigned(addressBytes, 0, addressBytes.length, o.addressBytes, 0, o.addressBytes.length);
        if (retval != 0)
        {
            return retval;
        }

        return Integer.compare(port, o.port);
    }

    public String getHostAddress(boolean withPort)
    {
        if (withPort)
        {
            return toString();
        }
        else
        {
            return address.getHostAddress();
        }
    }

    @Override
    public String toString()
    {
        return toString(true);
    }

    public String toString(boolean withPort)
    {
        if (withPort)
        {
            return toString(address, port);
        }
        else
        {
            return address.toString();
        }
    }

    public static String toString(InetAddress address, int port)
    {
        return HostAndPort.fromParts(address.getHostAddress(), port).toString();
    }

    public static InetAddressAndPort getByName(String name) throws UnknownHostException
    {
        return getByNameOverrideDefaults(name, null);
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

    /*
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
            byte[] buf = endpoint.addressBytes;

            if (version >= MessagingService.VERSION_40)
            {
                out.writeByte(buf.length + 2);
                out.write(buf);
                out.writeShort(endpoint.port);
            }
            else
            {
                out.writeByte(buf.length);
                out.write(buf);
            }
        }

        public InetAddressAndPort deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readByte() & 0xFF;
            switch(size)
            {
                //The original pre-4.0 serialiation of just an address
                case 4:
                case 16:
                {
                    byte[] bytes = new byte[size];
                    in.readFully(bytes, 0, bytes.length);
                    return getByAddress(bytes);
                }
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
            if (size == 4 || size == 16)
            {
                byte[] bytes = new byte[size];
                ByteBufferUtil.copyBytes(buf, position, bytes, 0, size);
                return getByAddress(bytes);
            }
            else if (size == 6 || size == 18)
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
            //4.0 includes a port number
            if (version >= MessagingService.VERSION_40)
            {
                if (from.address instanceof Inet4Address)
                    return 1 + 4 + 2;
                assert from.address instanceof Inet6Address;
                return 1 + 16 + 2;
            }
            else
            {
                if (from.address instanceof Inet4Address)
                    return 1 + 4;
                assert from.address instanceof Inet6Address;
                return 1 + 16;
            }
        }
    }
}
