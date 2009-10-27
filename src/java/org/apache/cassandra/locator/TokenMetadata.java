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

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.dht.Token;
import java.net.InetAddress;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.UnavailableException;

public class TokenMetadata
{
    /* Maintains token to endpoint map of every node in the cluster. */
    private Map<Token, InetAddress> tokenToEndPointMap;
    /* Maintains a reverse index of endpoint to token in the cluster. */
    private Map<InetAddress, Token> endPointToTokenMap;
    /* Bootstrapping nodes and their tokens */
    private Map<Token, InetAddress> bootstrapNodes;
    
    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    public TokenMetadata()
    {
        tokenToEndPointMap = new HashMap<Token, InetAddress>();
        endPointToTokenMap = new HashMap<InetAddress, Token>();
        this.bootstrapNodes = Collections.synchronizedMap(new HashMap<Token, InetAddress>());
    }

    public TokenMetadata(Map<Token, InetAddress> tokenToEndPointMap, Map<InetAddress, Token> endPointToTokenMap, Map<Token, InetAddress> bootstrapNodes)
    {
        this.tokenToEndPointMap = tokenToEndPointMap;
        this.endPointToTokenMap = endPointToTokenMap;
        this.bootstrapNodes = bootstrapNodes;
    }
    
    public TokenMetadata cloneMe()
    {
        return new TokenMetadata(cloneTokenEndPointMap(), cloneEndPointTokenMap(), cloneBootstrapNodes());
    }
        
    public void update(Token token, InetAddress endpoint)
    {
        this.update(token, endpoint, false);
    }
    /**
     * Update the two maps in an safe mode. 
    */
    public void update(Token token, InetAddress endpoint, boolean bootstrapState)
    {
        lock.writeLock().lock();
        try
        {
            if (bootstrapState)
            {
                bootstrapNodes.put(token, endpoint);
                this.remove(endpoint);
            }
            else
            {
                bootstrapNodes.remove(token); // If this happened to be there 
                Token oldToken = endPointToTokenMap.get(endpoint);
                if ( oldToken != null )
                    tokenToEndPointMap.remove(oldToken);
                tokenToEndPointMap.put(token, endpoint);
                endPointToTokenMap.put(endpoint, token);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Remove the entries in the two maps.
     * @param endpoint
     */
    public void remove(InetAddress endpoint)
    {
        lock.writeLock().lock();
        try
        {            
            Token oldToken = endPointToTokenMap.get(endpoint);
            if ( oldToken != null )
                tokenToEndPointMap.remove(oldToken);
            endPointToTokenMap.remove(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    public Token getToken(InetAddress endpoint)
    {
        lock.readLock().lock();
        try
        {
            return endPointToTokenMap.get(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public boolean isKnownEndPoint(InetAddress ep)
    {
        lock.readLock().lock();
        try
        {
            return endPointToTokenMap.containsKey(ep);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public InetAddress getFirstEndpoint()
    {
        lock.readLock().lock();
        try
        {
            ArrayList<Token> tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
            if (tokens.isEmpty())
                return null;
            Collections.sort(tokens);
            return tokenToEndPointMap.get(tokens.get(0));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    

    public InetAddress getNextEndpoint(InetAddress endPoint) throws UnavailableException
    {
        lock.readLock().lock();
        try
        {
            ArrayList<Token> tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
            if (tokens.isEmpty())
                return null;
            Collections.sort(tokens);
            int i = tokens.indexOf(endPointToTokenMap.get(endPoint)); // TODO binary search
            int j = 1;
            InetAddress ep;
            while (!FailureDetector.instance().isAlive((ep = tokenToEndPointMap.get(tokens.get((i + j) % tokens.size())))))
            {
                if (++j > DatabaseDescriptor.getReplicationFactor())
                {
                    throw new UnavailableException();
                }
            }
            return ep;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public Map<Token, InetAddress> cloneBootstrapNodes()
    {
        lock.readLock().lock();
        try
        {            
            return new HashMap<Token, InetAddress>( bootstrapNodes );
        }
        finally
        {
            lock.readLock().unlock();
        }
        
    }

    /*
     * Returns a safe clone of tokenToEndPointMap_.
    */
    public Map<Token, InetAddress> cloneTokenEndPointMap()
    {
        lock.readLock().lock();
        try
        {            
            return new HashMap<Token, InetAddress>(tokenToEndPointMap);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    /*
     * Returns a safe clone of endPointTokenMap_.
    */
    public Map<InetAddress, Token> cloneEndPointTokenMap()
    {
        lock.readLock().lock();
        try
        {            
            return new HashMap<InetAddress, Token>(endPointToTokenMap);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        Set<InetAddress> eps = endPointToTokenMap.keySet();
        
        for ( InetAddress ep : eps )
        {
            sb.append(ep);
            sb.append(":");
            sb.append(endPointToTokenMap.get(ep));
            sb.append(System.getProperty("line.separator"));
        }
        
        return sb.toString();
    }

    public InetAddress getEndPoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            return tokenToEndPointMap.get(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
}
