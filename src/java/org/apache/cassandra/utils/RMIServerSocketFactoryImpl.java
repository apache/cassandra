package org.apache.cassandra.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import javax.net.ServerSocketFactory;


public class RMIServerSocketFactoryImpl implements RMIServerSocketFactory
{

    public ServerSocket createServerSocket(final int pPort) throws IOException
    {
        ServerSocket socket = ServerSocketFactory.getDefault()
                                                 .createServerSocket(pPort, 0, InetAddress.getLoopbackAddress());
        socket.setReuseAddress(true);
        return socket;
    }

    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }

        return obj.getClass().equals(getClass());
    }

    public int hashCode()
    {
        return RMIServerSocketFactoryImpl.class.hashCode();
    }
}
