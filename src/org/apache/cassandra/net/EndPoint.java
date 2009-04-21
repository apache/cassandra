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

package org.apache.cassandra.net;


import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class EndPoint implements Serializable, Comparable<EndPoint>
{
    // logging and profiling.
    private static Logger logger_ = Logger.getLogger(EndPoint.class);
    private static final long serialVersionUID = -4962625949179835907L;
    private static Map<CharBuffer, String> hostNames_ = new HashMap<CharBuffer, String>();
    protected static final int randomPort_ = 5555;
    public static EndPoint randomLocalEndPoint_;
    
    static
    {
        try
        {
            randomLocalEndPoint_ = new EndPoint(FBUtilities.getHostName(), EndPoint.randomPort_);
        }        
        catch ( IOException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }
    }

    private String host_;
    private int port_;

    private transient InetSocketAddress ia_;

    /* Ctor for JAXB. DO NOT DELETE */
    private EndPoint()
    {
    }

    public EndPoint(String host, int port)
    {
        /*
         * Attempts to resolve the host, but does not fail if it cannot.
         */
        host_ = host;
        port_ = port;
    }

    // create a local endpoint id
    public EndPoint(int port)
    {
        try
        {
            host_ = FBUtilities.getHostName();
            port_ = port;
        }
        catch (UnknownHostException e)
        {
            logger_.warn(LogUtil.throwableToString(e));
        }
    }

    public String getHost()
    {
        return host_;
    }

    public int getPort()
    {
        return port_;
    }

    public void setPort(int port)
    {
        port_ = port;
    }

    public InetSocketAddress getInetAddress()
    {
        if (ia_ == null || ia_.isUnresolved())
        {
            ia_ = new InetSocketAddress(host_, port_);
        }
        return ia_;
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof EndPoint))
            return false;

        EndPoint rhs = (EndPoint) o;
        return (host_.equals(rhs.host_) && port_ == rhs.port_);
    }

    public int hashCode()
    {
        return (host_ + port_).hashCode();
    }

    public int compareTo(EndPoint rhs)
    {
        return host_.compareTo(rhs.host_);
    }

    public String toString()
    {
        return (host_ + ":" + port_);
    }

    public static EndPoint fromString(String str)
    {
        String[] values = str.split(":");
        return new EndPoint(values[0], Integer.parseInt(values[1]));
    }

    public static byte[] toBytes(EndPoint ep)
    {
        ByteBuffer buffer = ByteBuffer.allocate(6);
        byte[] iaBytes = ep.getInetAddress().getAddress().getAddress();
        buffer.put(iaBytes);
        buffer.put(MessagingService.toByteArray((short) ep.getPort()));
        buffer.flip();
        return buffer.array();
    }

    public static EndPoint fromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        System.arraycopy(bytes, 0, buffer.array(), 0, 4);
        byte[] portBytes = new byte[2];
        System.arraycopy(bytes, 4, portBytes, 0, portBytes.length);
        try
        {
            CharBuffer charBuffer = buffer.asCharBuffer();
            String host = hostNames_.get(charBuffer);
            if (host == null)
            {               
                host = InetAddress.getByAddress(buffer.array()).getHostName();              
                hostNames_.put(charBuffer, host);
            }
            int port = (int) MessagingService.byteArrayToShort(portBytes);
            return new EndPoint(host, port);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException(e);
        }
    }
}

