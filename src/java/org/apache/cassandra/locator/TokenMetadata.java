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
import org.apache.cassandra.dht.Range;

import java.net.InetAddress;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.UnavailableException;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class TokenMetadata
{
    /* Maintains token to endpoint map of every node in the cluster. */
    private BiMap<Token, InetAddress> tokenToEndPointMap;
    /* Bootstrapping nodes and their tokens */
    private Set<InetAddress> bootstrapping;
    private BiMap<Token, InetAddress> bootstrapTokenMap;
    
    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    public TokenMetadata()
    {
        this(null, null);
    }

    public TokenMetadata(BiMap<Token, InetAddress> tokenToEndPointMap, BiMap<Token, InetAddress> bootstrapTokenMap)
    {
        bootstrapping = new NonBlockingHashSet<InetAddress>();
        if (tokenToEndPointMap == null)
            tokenToEndPointMap = HashBiMap.create();
        if (bootstrapTokenMap == null)
            bootstrapTokenMap = HashBiMap.create();
        this.tokenToEndPointMap = tokenToEndPointMap;
        this.bootstrapTokenMap = bootstrapTokenMap;
    }

    public TokenMetadata(BiMap<Token, InetAddress> tokenEndpointMap)
    {
        this(tokenEndpointMap, null);
    }

    public void setBootstrapping(InetAddress endpoint, boolean isBootstrapping)
    {
        if (isBootstrapping)
            bootstrapping.add(endpoint);
        else
            bootstrapping.remove(endpoint);

        lock.writeLock().lock();
        try
        {
            BiMap<Token, InetAddress> otherMap = bootstrapping.contains(endpoint) ? tokenToEndPointMap : bootstrapTokenMap;
            Token t = otherMap.inverse().get(endpoint);
            if (t != null)
            {
                Map<Token, InetAddress> map = bootstrapping.contains(endpoint) ? bootstrapTokenMap : tokenToEndPointMap;
                map.put(t, endpoint);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update the two maps in an safe mode. 
    */
    public void update(Token token, InetAddress endpoint)
    {
        assert token != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            Map<Token, InetAddress> map = bootstrapping.contains(endpoint) ? bootstrapTokenMap : tokenToEndPointMap;
            Map<Token, InetAddress> otherMap = bootstrapping.contains(endpoint) ? tokenToEndPointMap : bootstrapTokenMap;
            map.put(token, endpoint);
            otherMap.remove(token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Token getToken(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndPointMap.inverse().get(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public boolean isKnownEndPoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndPointMap.inverse().containsKey(endpoint);
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
    

    public InetAddress getNextEndpoint(InetAddress endpoint) throws UnavailableException
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            ArrayList<Token> tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
            if (tokens.isEmpty())
                return null;
            Collections.sort(tokens);
            int i = tokens.indexOf(tokenToEndPointMap.inverse().get(endpoint)); // TODO binary search
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

    public TokenMetadata cloneMe()
    {
        lock.readLock().lock();
        try
        {
            return new TokenMetadata(HashBiMap.create(tokenToEndPointMap), HashBiMap.create(bootstrapTokenMap));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            Set<InetAddress> eps = tokenToEndPointMap.inverse().keySet();

            for (InetAddress ep : eps)
            {
                sb.append(ep);
                sb.append(":");
                sb.append(tokenToEndPointMap.inverse().get(ep));
                sb.append(System.getProperty("line.separator"));
            }
        }
        finally
        {
            lock.readLock().unlock();
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

    public void clearUnsafe()
    {
        tokenToEndPointMap.clear();
        bootstrapTokenMap.clear();
    }

    public Range getPrimaryRangeFor(Token right)
    {
        return new Range(getPredecessor(right), right);
    }

    public List<Token> sortedTokens()
    {
        List<Token> tokens;
        lock.readLock().lock();
        try
        {
            tokens = new ArrayList<Token>(tokenToEndPointMap.keySet());
        }
        finally
        {
            lock.readLock().unlock();
        }
        Collections.sort(tokens);
        return tokens;
    }

    public Token getPredecessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        return (Token) (index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(--index));
    }

    public Token getSuccessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        return (Token) ((index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(++index));
    }

    public Iterable<? extends Token> bootstrapTokens()
    {
        return bootstrapTokenMap.keySet();
    }

    public InetAddress getBootstrapEndpoint(Token token)
    {
        return bootstrapTokenMap.get(token);
    }
}
