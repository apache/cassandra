package org.apache.cassandra.service;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SocketSessionManagementService
{
    public final static SocketSessionManagementService instance = new SocketSessionManagementService();
    public final static ThreadLocal<SocketAddress> remoteSocket = new ThreadLocal<SocketAddress>();
    private Map<SocketAddress, ClientState> activeSocketSessions = new ConcurrentHashMap<SocketAddress, ClientState>();

    public ClientState get(SocketAddress key)
    {
        ClientState retval = null;
        if (null != key)
        {
            retval = activeSocketSessions.get(key);
        }
        return retval;
    }

    public void put(SocketAddress key, ClientState value)
    {
        if (null != key && null != value)
        {
            activeSocketSessions.put(key, value);
        }
    }

    public boolean remove(SocketAddress key)
    {
        assert null != key;
        boolean retval = false;
        if (null != activeSocketSessions.remove(key))
        {
            retval = true;
        }
        return retval;
    }

    public void clear()
    {
        activeSocketSessions.clear();
    }

}
