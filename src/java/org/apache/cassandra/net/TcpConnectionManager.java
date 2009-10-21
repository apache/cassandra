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
import java.util.*;
import java.util.concurrent.locks.*;
import java.net.InetAddress;

import org.apache.log4j.Logger;

class TcpConnectionManager
{
    private Lock lock_ = new ReentrantLock();
    private List<TcpConnection> allConnections_;
    private InetAddress localEp_;
    private InetAddress remoteEp_;
    private int maxSize_;

    private int inUse_;

    // TODO! this whole thing is a giant no-op, since "contains" only relies on TcpConnection.equals, which
    // is true for any (local, remote) pairs.  So there is only ever at most one TcpConnection per Manager!
    TcpConnectionManager(int initialSize, int growthFactor, int maxSize, InetAddress localEp, InetAddress remoteEp)
    {
        maxSize_ = maxSize;
        localEp_ = localEp;
        remoteEp_ = remoteEp;
        allConnections_ = new ArrayList<TcpConnection>();
    }

    /**
     * returns the least loaded connection to remoteEp, creating a new connection if necessary
     */
    TcpConnection getConnection() throws IOException
    {
        lock_.lock();
        try
        {
            if (allConnections_.isEmpty())
            {
                TcpConnection conn = new TcpConnection(this, localEp_, remoteEp_);
                addToPool(conn);
                conn.inUse_ = true;
                incUsed();
                return conn;
            }

            TcpConnection least = getLeastLoaded();

            if ((least != null && least.pending() == 0) || allConnections_.size() == maxSize_)
            {
                least.inUse_ = true;
                incUsed();
                return least;
            }

            TcpConnection connection = new TcpConnection(this, localEp_, remoteEp_);
            if (!contains(connection))
            {
                addToPool(connection);
                connection.inUse_ = true;
                incUsed();
                return connection;
            }
            else
            {
                connection.closeSocket();
                return getLeastLoaded();
            }
        }
        finally
        {
            lock_.unlock();
        }
    }

    protected TcpConnection getLeastLoaded()
    {
        TcpConnection connection = null;
        lock_.lock();
        try
        {
            Collections.sort(allConnections_);
            connection = (allConnections_.size() > 0) ? allConnections_.get(0) : null;
        }
        finally
        {
            lock_.unlock();
        }
        return connection;
    }

    void removeConnection(TcpConnection connection)
    {
        lock_.lock();
        try
        {
            allConnections_.remove(connection);
        }
        finally
        {
            lock_.unlock();
        }
    }

    void incUsed()
    {
        inUse_++;
    }

    void decUsed()
    {
        inUse_--;
    }

    int getConnectionsInUse()
    {
        return inUse_;
    }

    void addToPool(TcpConnection connection)
    {
        lock_.lock();
        try
        {
            if (contains(connection))
                return;

            if (allConnections_.size() < maxSize_)
            {
                allConnections_.add(connection);
            }
            else
            {
                connection.closeSocket();
            }
        }
        finally
        {
            lock_.unlock();
        }
    }

    void shutdown()
    {
        lock_.lock();
        try
        {
            while (allConnections_.size() > 0)
            {
                TcpConnection connection = allConnections_.remove(0);
                connection.closeSocket();
            }
        }
        finally
        {
            lock_.unlock();
        }
    }

    int getPoolSize()
    {
        lock_.lock();
        try
        {
            return allConnections_.size();
        }
        finally
        {
            lock_.unlock();
        }
    }

    InetAddress getLocalEndPoint()
    {
        return localEp_;
    }

    InetAddress getRemoteEndPoint()
    {
        return remoteEp_;
    }

    int getPendingWrites()
    {
        int total = 0;
        lock_.lock();
        try
        {
            for (TcpConnection connection : allConnections_)
            {
                total += connection.pending();
            }
        }
        finally
        {
            lock_.unlock();
        }
        return total;
    }

    boolean contains(TcpConnection connection)
    {
        lock_.lock();
        try
        {
            return allConnections_.contains(connection);
        }
        finally
        {
            lock_.unlock();
        }
    }
}
