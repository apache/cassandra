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

package org.apache.cassandra.locator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;
import org.apache.cassandra.net.EndPoint;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TokenMetadata
{
    /* Maintains token to endpoint map of every node in the cluster. */
    private Map<BigInteger, EndPoint> tokenToEndPointMap_ = new HashMap<BigInteger, EndPoint>();    
    /* Maintains a reverse index of endpoint to token in the cluster. */
    private Map<EndPoint, BigInteger> endPointToTokenMap_ = new HashMap<EndPoint, BigInteger>();
    
    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock_ = new ReentrantReadWriteLock(true);

    public TokenMetadata()
    {
    }

    private TokenMetadata(Map<BigInteger, EndPoint> tokenToEndPointMap, Map<EndPoint, BigInteger> endPointToTokenMap)
    {
        tokenToEndPointMap_ = tokenToEndPointMap;
        endPointToTokenMap_ = endPointToTokenMap;
    }
    
    public TokenMetadata cloneMe()
    {
        Map<BigInteger, EndPoint> tokenToEndPointMap = cloneTokenEndPointMap();
        Map<EndPoint, BigInteger> endPointToTokenMap = cloneEndPointTokenMap();
        return new TokenMetadata( tokenToEndPointMap, endPointToTokenMap );
    }
    
    /**
     * Update the two maps in an safe mode. 
    */
    public void update(BigInteger token, EndPoint endpoint)
    {
        lock_.writeLock().lock();
        try
        {            
            BigInteger oldToken = endPointToTokenMap_.get(endpoint);
            if ( oldToken != null )
                tokenToEndPointMap_.remove(oldToken);
            tokenToEndPointMap_.put(token, endpoint);
            endPointToTokenMap_.put(endpoint, token);
        }
        finally
        {
            lock_.writeLock().unlock();
        }
    }
    
    /**
     * Remove the entries in the two maps.
     * @param endpoint
     */
    public void remove(EndPoint endpoint)
    {
        lock_.writeLock().lock();
        try
        {            
            BigInteger oldToken = endPointToTokenMap_.get(endpoint);
            if ( oldToken != null )
                tokenToEndPointMap_.remove(oldToken);            
            endPointToTokenMap_.remove(endpoint);
        }
        finally
        {
            lock_.writeLock().unlock();
        }
    }
    
    public BigInteger getToken(EndPoint endpoint)
    {
        lock_.readLock().lock();
        try
        {
            return endPointToTokenMap_.get(endpoint);
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }
    
    public boolean isKnownEndPoint(EndPoint ep)
    {
        lock_.readLock().lock();
        try
        {
            return endPointToTokenMap_.containsKey(ep);
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }
    
    /*
     * Returns a safe clone of tokenToEndPointMap_.
    */
    public Map<BigInteger, EndPoint> cloneTokenEndPointMap()
    {
        lock_.readLock().lock();
        try
        {            
            return new HashMap<BigInteger, EndPoint>( tokenToEndPointMap_ );
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }
    
    /*
     * Returns a safe clone of endPointTokenMap_.
    */
    public Map<EndPoint, BigInteger> cloneEndPointTokenMap()
    {
        lock_.readLock().lock();
        try
        {            
            return new HashMap<EndPoint, BigInteger>( endPointToTokenMap_ );
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Set<EndPoint> eps = endPointToTokenMap_.keySet();
        
        for ( EndPoint ep : eps )
        {
            sb.append(ep);
            sb.append(":");
            sb.append(endPointToTokenMap_.get(ep));
            sb.append(System.getProperty("line.separator"));
        }
        
        return sb.toString();
    }
}
