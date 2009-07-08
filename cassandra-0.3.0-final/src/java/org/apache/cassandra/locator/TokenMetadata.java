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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.EndPoint;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class TokenMetadata
{
    /* Maintains token to endpoint map of every node in the cluster. */
    private Map<Token, EndPoint> tokenToEndPointMap_ = new HashMap<Token, EndPoint>();
    /* Maintains a reverse index of endpoint to token in the cluster. */
    private Map<EndPoint, Token> endPointToTokenMap_ = new HashMap<EndPoint, Token>();
    
    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock_ = new ReentrantReadWriteLock(true);

    public TokenMetadata()
    {
    }

    private TokenMetadata(Map<Token, EndPoint> tokenToEndPointMap, Map<EndPoint, Token> endPointToTokenMap)
    {
        tokenToEndPointMap_ = tokenToEndPointMap;
        endPointToTokenMap_ = endPointToTokenMap;
    }
    
    public TokenMetadata cloneMe()
    {
        return new TokenMetadata(cloneTokenEndPointMap(), cloneEndPointTokenMap());
    }
    
    /**
     * Update the two maps in an safe mode. 
    */
    public void update(Token token, EndPoint endpoint)
    {
        lock_.writeLock().lock();
        try
        {            
            Token oldToken = endPointToTokenMap_.get(endpoint);
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
            Token oldToken = endPointToTokenMap_.get(endpoint);
            if ( oldToken != null )
                tokenToEndPointMap_.remove(oldToken);            
            endPointToTokenMap_.remove(endpoint);
        }
        finally
        {
            lock_.writeLock().unlock();
        }
    }
    
    public Token getToken(EndPoint endpoint)
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
    public Map<Token, EndPoint> cloneTokenEndPointMap()
    {
        lock_.readLock().lock();
        try
        {            
            return new HashMap<Token, EndPoint>( tokenToEndPointMap_ );
        }
        finally
        {
            lock_.readLock().unlock();
        }
    }
    
    /*
     * Returns a safe clone of endPointTokenMap_.
    */
    public Map<EndPoint, Token> cloneEndPointTokenMap()
    {
        lock_.readLock().lock();
        try
        {            
            return new HashMap<EndPoint, Token>( endPointToTokenMap_ );
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
