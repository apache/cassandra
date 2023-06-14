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
package org.apache.cassandra.cql3;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

import org.apache.cassandra.utils.Pair;

/**
 * Contains a CIDR, and operations on it
 */
public final class CIDR
{
    private final InetAddress startIpAddress;
    private final InetAddress endIpAddress;
    // max mask value with IPv6 is 128, not easy to handle value 128 using Byte, hence using short
    private final short netMask;

    /**
     * Generates a CIDR from given IP and netmask
     * @param ipAddress IP address of CIDR
     * @param netMask   netmask of CIDR
     */
    public CIDR(InetAddress ipAddress, short netMask)
    {
        if (netMask > maxNetMaskAllowed(ipAddress))
            throw new IllegalArgumentException("Invalid netmask " + netMask + " for IP " + ipAddress.getHostAddress());

        Pair<InetAddress, InetAddress> ipRange = calcIpRangeOfCidr(ipAddress, netMask);
        this.startIpAddress = ipRange.left();
        this.endIpAddress = ipRange.right();
        this.netMask = netMask;
    }

    /**
     * Generates a CIDR from given string
     * @param cidrStr CIDR as string
     */
    public static CIDR getInstance(String cidrStr)
    {
        if (cidrStr == null || cidrStr.isEmpty())
        {
            throw new IllegalArgumentException(String.format("%s is not a valid CIDR String", cidrStr));
        }

        String[] parts = cidrStr.split("/");
        if (parts.length != 2)
        {
            throw new IllegalArgumentException(String.format("%s is not a valid CIDR String", cidrStr));
        }

        short netMask = Short.parseShort(parts[1]);

        InetAddress ipAddress;
        try
        {
            ipAddress = InetAddress.getByName(parts[0]);
            if (ipAddress instanceof Inet4Address && parts[0].contains(":") && parts[0].contains("."))
            {
                // Input string is in IPv4 mapped IPv6 format. InetAddress converted it to IPv4
                // So adjust the net mask accordingly
                // example, 0:0:0:0:0:ffff:192.1.56.10/96 would be converted to 192.1.56.10/0
                netMask -= 96; // 6 * 16 bits
            }
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException(String.format("%s is not a valid CIDR String", cidrStr));
        }

        short maxMaskValue = maxNetMaskAllowed(ipAddress);
        if (netMask < 0 || netMask > maxMaskValue)
        {
            throw new IllegalArgumentException(String.format("%s is not a valid CIDR String", cidrStr));
        }

        return new CIDR(ipAddress, netMask);
    }

    private static short maxNetMaskAllowed(InetAddress ipAddress)
    {
        if (ipAddress instanceof Inet6Address)
            return 128;

        return 32;
    }

    /**
     * This function calculates starting and ending IP of a CIDR.
     * For example, for CIDR 10.20.33.40/24, starting IP is 10.20.33.0, ending IP is 10.20.33.255
     * @param ipAddress IP address of the CIDR
     * @param netMask   net mask of the CIDR
     * @return pair of starting and ending IP of the CIDR
     */
    private static Pair<InetAddress, InetAddress> calcIpRangeOfCidr(InetAddress ipAddress, short netMask)
    {
        if (netMask > maxNetMaskAllowed(ipAddress))
            throw new IllegalArgumentException("Invalid netmask " + netMask + " for IP " + ipAddress.getHostAddress());

        // Starting and ending IP are same as CIDR's IP if net mask is 32 for IPv4, 128 for IPv6
        if (netMask == maxNetMaskAllowed(ipAddress))
            return Pair.create(ipAddress, ipAddress);

        byte[] startIpBytes = ipAddress.getAddress();
        byte[] endIpBytes = ipAddress.getAddress();

        // netmask indicates number of bits to remain unchanged. Let's call that as netmask bit.
        // Calculate the offset of the byte where netmask bit ends
        int byteOffset = netMask / Byte.SIZE;

        // Calculate the bit number within that byte, where netmask bit ends
        int bitOffset = netMask % Byte.SIZE;

        // In that byte, set bits after netmask bit to 0 for starting IP, to 1 for ending IP,
        // keeping bits before including netmask bit as it is
        int unsignedByte = Byte.toUnsignedInt(startIpBytes[byteOffset]);
        startIpBytes[byteOffset] = (byte) (unsignedByte & (0xff << (Byte.SIZE - bitOffset)));
        endIpBytes[byteOffset] = (byte) (unsignedByte | (0xFF >>> bitOffset));

        // set all remaining bits after netmask to 0
        for (byteOffset += 1; byteOffset < startIpBytes.length; byteOffset++)
        {
            startIpBytes[byteOffset] = 0;
            endIpBytes[byteOffset] = (byte) 0xFF;
        }

        try
        {
            return Pair.create(InetAddress.getByAddress(startIpBytes), InetAddress.getByAddress(endIpBytes));
        }
        catch (UnknownHostException e)
        {
            throw new IllegalStateException("Invalid bytes for constructing IP", e);
        }
    }

    /**
     * Get starting IP of the CIDR
     * @return returns IP address
     */
    public InetAddress getStartIpAddress()
    {
        return startIpAddress;
    }

    /**
     * Get ending IP of the CIDR
     * @return returns IP address
     */
    public InetAddress getEndIpAddress()
    {
        return endIpAddress;
    }

    /**
     * Get netmask of the CIDR
     * @return returns netmask as short
     */
    public short getNetMask()
    {
        return netMask;
    }

    /**
     * Tells is this IPv4 format CIDR
     * @return true if IPv4 CIDR, otherwise false
     */
    public boolean isIPv4()
    {
        return (startIpAddress instanceof Inet4Address);
    }

    /**
     * Tells is this IPv6 format CIDR
     * @return true if IPv6 CIDR, otherwise false
     */
    public boolean isIPv6()
    {
        return (startIpAddress instanceof Inet6Address);
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CIDR cidr = (CIDR) o;
        return netMask == cidr.netMask &&
               startIpAddress.equals(cidr.startIpAddress) &&
               endIpAddress.equals(cidr.endIpAddress);
    }

    public int hashCode()
    {
        return Objects.hash(startIpAddress, endIpAddress, netMask);
    }

    /**
     * Constructs CIDR as string
     * @return returns CIDR as string
     */
    @Override
    public String toString()
    {
        return this.startIpAddress.getHostAddress() + '/' + this.netMask;
    }

    public String asCqlTupleString()
    {
        return "('" + this.startIpAddress.getHostAddress() + "', " + this.netMask + ')';
    }

    /**
     * Compare 2 IpAddresses objects lexicographically
     * @return true if IP ranges overlap with each other; otherwise, return false
     */
    public static int compareIPs(InetAddress l, InetAddress r)
    {
        byte[] lBytes = l.getAddress();
        byte[] rBytes = r.getAddress();

        for (int i = 0; i < lBytes.length; i++)
        {
            int comp = Byte.toUnsignedInt(lBytes[i]) - Byte.toUnsignedInt(rBytes[i]);
            if (comp != 0)
            {
                return comp;
            }
        }
        return 0;
    }

    public static boolean overlaps(CIDR left, CIDR right)
    {
        // Sort IP ranges by the start address
        CIDR lower = left;
        CIDR higher = right;

        if (compareIPs(left.startIpAddress, right.startIpAddress) > 0)
        {
            lower = right;
            higher = left;
        }

        // Overlaps when lower end is >= to higher start address
        return compareIPs(lower.endIpAddress, higher.startIpAddress) >= 0;
    }
}
