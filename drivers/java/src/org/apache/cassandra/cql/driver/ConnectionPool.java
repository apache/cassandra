/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.cql.driver;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple connection-caching pool implementation.
 * 
 * <p>A <code>ConnectionPool</code> provides the simplest possible connection
 * pooling, lazily creating new connections if needed as
 * <code>borrowClient</code> is called.  Connections are re-added to the pool
 * by <code>returnConnection</code>, unless doing so would exceed the maximum
 * pool size.</p>
 * 
 * <p>Example usage:</p>
 * 
 * <code>
 * IConnectionPool pool = new ConnectionPool("localhost", 9160);<br />
 * Connection conn = pool.borrowConnection();<br />
 * conn.execute(...);<br />
 * pool.returnConnection(pool);<br />
 * </code>
 */
public class ConnectionPool implements IConnectionPool
{
    public static final int DEFAULT_MAX_CONNECTIONS = 25;
    public static final int DEFAULT_PORT = 9160;
    public static final int DEFAULT_MAX_IDLE = 5;
    public static final long DEFAULT_EVICTION_DELAY_MILLIS = 10000; // 10 seconds
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
    
    private ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<Connection>();
    private Timer eviction;
    private String hostName;
    private int portNo;
    private int maxConns, maxIdle;
    
    /**
     * Create a new <code>ConnectionPool</code> for a given hostname.
     * 
     * @param hostName hostname or IP address to open connections to
     * @throws TTransportException if unable to connect
     */
    public ConnectionPool(String hostName) throws TTransportException
    {
        this(hostName, DEFAULT_PORT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_IDLE, DEFAULT_EVICTION_DELAY_MILLIS);
    }
    
    /**
     * Create a new <code>ConnectionPool</code> for the given hostname and
     * port number.
     * 
     * @param hostName hostname or IP address to open connections to
     * @param portNo port number to connect to
     * @throws TTransportException if unable to connect
     */
    public ConnectionPool(String hostName, int portNo) throws TTransportException
    {
        this(hostName, portNo, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_IDLE, DEFAULT_EVICTION_DELAY_MILLIS);
    }
    
    /**
     * Create a new <code>ConnectionPool</code>.
     * 
     * @param hostName hostname or IP address to open connections to
     * @param portNo portNo port number to connect to
     * @param maxConns the maximum number of connections to save in the pool
     * @param maxIdle the max number of connections allowed in the pool after an eviction
     * @param evictionDelay the number milliseconds between eviction runs
     * @throws TTransportException if unable to connect
     */
    public ConnectionPool(String hostName, int portNo, int maxConns, int maxIdle, long evictionDelay)
    throws TTransportException
    {
        this.hostName = hostName;
        this.portNo = portNo;
        this.maxConns = maxConns;
        this.maxIdle = maxIdle;
        
        eviction = new Timer("EVICTION-THREAD", true);
        eviction.schedule(new EvictionTask(), new Date(), evictionDelay);
        
        connections.add(new Connection(hostName, portNo));
    }
    
    /**
     * Check a <code>Connection</code> instance out from the pool, creating a
     * new one if the pool is exhausted.
     */
    public Connection borrowConnection()
    {
        Connection conn = null;
        
        if ((conn = connections.poll()) == null)
        {
            try
            {
                conn = new Connection(hostName, portNo);
            }
            catch (TTransportException error)
            {
                logger.error(String.format("Error connecting to %s:%s", hostName, portNo), error);
            }
        }
        
        return conn;
    }
    
    /**
     * Returns an <code>Connection</code> instance to the pool.  If the pool
     * already contains the maximum number of allowed connections, then the
     * instance's <code>close</code> method is called and it is discarded.
     */
    public void returnConnection(Connection connection)
    {
        if (connections.size() >= maxConns)
        {
            if (connection.isOpen()) connection.close();
            logger.warn("Max pool size reached; Connection discarded.");
            return;
        }
        
        if (!connection.isOpen())
        {
            logger.warn("Stubbornly refusing to return a closed connection to the pool (discarded instead).");
            return;
        }
        
        connections.add(connection);
    }
    
    private class EvictionTask extends TimerTask
    {
        public void run()
        {
            int count = 0;
            
            while (connections.size() > maxIdle)
            {
                Connection conn = connections.poll();
                if (conn.isOpen()) conn.close();
                count++;
            }
            
            if (count > 0)
                logger.debug("Eviction run complete: {} connections evicted.", count);
        }
    }
}
