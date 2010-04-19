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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Range;

import java.net.InetAddress;

import org.apache.commons.lang.StringUtils;

public class TokenMetadata
{
    /* Maintains token to endpoint map of every node in the cluster. */
    private BiMap<Token, InetAddress> tokenToEndpointMap;

    // Suppose that there is a ring of nodes A, C and E, with replication factor 3.
    // Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
    // Now suppose node B bootstraps between A and C at the same time. Its pending ranges would be C-E, E-A and A-B.
    // Now both nodes have pending range E-A in their list, which will cause pending range collision
    // even though we're only talking about replica range, not even primary range. The same thing happens
    // for any nodes that boot simultaneously between same two nodes. For this we cannot simply make pending ranges a multimap,
    // since that would make us unable to notice the real problem of two nodes trying to boot using the same token.
    // In order to do this properly, we need to know what tokens are booting at any time.
    private BiMap<Token, InetAddress> bootstrapTokens;

    // we will need to know at all times what nodes are leaving and calculate ranges accordingly.
    // An anonymous pending ranges list is not enough, as that does not tell which node is leaving
    // and/or if the ranges are there because of bootstrap or leave operation.
    // (See CASSANDRA-603 for more detail + examples).
    private Set<InetAddress> leavingEndpoints;

    private ConcurrentMap<String, Multimap<Range, InetAddress>> pendingRanges;

    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private List<Token> sortedTokens;

    public TokenMetadata()
    {
        this(null);
    }

    public TokenMetadata(BiMap<Token, InetAddress> tokenToEndpointMap)
    {
        if (tokenToEndpointMap == null)
            tokenToEndpointMap = HashBiMap.create();
        this.tokenToEndpointMap = tokenToEndpointMap;
        bootstrapTokens = HashBiMap.create();
        leavingEndpoints = new HashSet<InetAddress>();
        pendingRanges = new ConcurrentHashMap<String, Multimap<Range, InetAddress>>();
        sortedTokens = sortTokens();
    }

    private List<Token> sortTokens()
    {
        List<Token> tokens = new ArrayList<Token>(tokenToEndpointMap.keySet());
        Collections.sort(tokens);
        return Collections.unmodifiableList(tokens);
    }

    /** @return the number of nodes bootstrapping into source's primary range */
    public int pendingRangeChanges(InetAddress source)
    {
        int n = 0;
        Range sourceRange = getPrimaryRangeFor(getToken(source));
        for (Token token : bootstrapTokens.keySet())
            if (sourceRange.contains(token))
                n++;
        return n;
    }

    public void updateNormalToken(Token token, InetAddress endpoint)
    {
        assert token != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.inverse().remove(endpoint);
            tokenToEndpointMap.inverse().remove(endpoint);
            if (!endpoint.equals(tokenToEndpointMap.put(token, endpoint)))
            {
                sortedTokens = sortTokens();
            }
            leavingEndpoints.remove(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addBootstrapToken(Token token, InetAddress endpoint)
    {
        assert token != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            InetAddress oldEndpoint = null;

            oldEndpoint = bootstrapTokens.get(token);
            if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);

            oldEndpoint = tokenToEndpointMap.get(token);
            if (oldEndpoint != null && !oldEndpoint.equals(endpoint))
                throw new RuntimeException("Bootstrap Token collision between " + oldEndpoint + " and " + endpoint + " (token " + token);

            bootstrapTokens.inverse().remove(endpoint);
            bootstrapTokens.put(token, endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeBootstrapToken(Token token)
    {
        assert token != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.remove(token);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void addLeavingEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.add(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeLeavingEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            leavingEndpoints.remove(endpoint);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(InetAddress endpoint)
    {
        assert tokenToEndpointMap.containsValue(endpoint);
        lock.writeLock().lock();
        try
        {
            bootstrapTokens.inverse().remove(endpoint);
            tokenToEndpointMap.inverse().remove(endpoint);
            leavingEndpoints.remove(endpoint);
            sortedTokens = sortTokens();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public Token getToken(InetAddress endpoint)
    {
        assert endpoint != null;
        assert isMember(endpoint); // don't want to return nulls
        
        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.inverse().get(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
    
    public boolean isMember(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.inverse().containsKey(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();
        try
        {
            return leavingEndpoints.contains(endpoint);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public InetAddress getFirstEndpoint()
    {
        assert tokenToEndpointMap.size() > 0;

        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.get(sortedTokens.get(0));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public TokenMetadata cloneOnlyTokenMap()
    {
        lock.readLock().lock();
        try
        {
            return new TokenMetadata(HashBiMap.create(tokenToEndpointMap));
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     */
    public TokenMetadata cloneAfterAllLeft()
    {
        lock.readLock().lock();
        try
        {
            TokenMetadata allLeftMetadata = cloneOnlyTokenMap();
            for (InetAddress endpoint : leavingEndpoints)
                allLeftMetadata.removeEndpoint(endpoint);
            return allLeftMetadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public InetAddress getEndpoint(Token token)
    {
        lock.readLock().lock();
        try
        {
            return tokenToEndpointMap.get(token);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Range getPrimaryRangeFor(Token right)
    {
        return new Range(getPredecessor(right), right);
    }

    public List<Token> sortedTokens()
    {
        lock.readLock().lock();
        try
        {
            return sortedTokens;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    private synchronized Multimap<Range, InetAddress> getPendingRangesMM(String table)
    {
        Multimap<Range, InetAddress> map = pendingRanges.get(table);
        if (map == null)
        {
             map = HashMultimap.create();
            pendingRanges.put(table, map);
        }
        return map;
    }

    /** a mutable map may be returned but caller should not modify it */
    public Map<Range, Collection<InetAddress>> getPendingRanges(String table)
    {
        return getPendingRangesMM(table).asMap();
    }

    public List<Range> getPendingRanges(String table, InetAddress endpoint)
    {
        List<Range> ranges = new ArrayList<Range>();
        for (Map.Entry<Range, InetAddress> entry : getPendingRangesMM(table).entries())
        {
            if (entry.getValue().equals(endpoint))
            {
                ranges.add(entry.getKey());
            }
        }
        return ranges;
    }

    public void setPendingRanges(String table, Multimap<Range, InetAddress> rangeMap)
    {
        pendingRanges.put(table, rangeMap);
    }

    public Token getPredecessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (Token) (index == 0 ? tokens.get(tokens.size() - 1) : tokens.get(index - 1));
    }

    public Token getSuccessor(Token token)
    {
        List tokens = sortedTokens();
        int index = Collections.binarySearch(tokens, token);
        assert index >= 0 : token + " not found in " + StringUtils.join(tokenToEndpointMap.keySet(), ", ");
        return (Token) ((index == (tokens.size() - 1)) ? tokens.get(0) : tokens.get(index + 1));
    }

    public InetAddress getSuccessor(InetAddress endpoint)
    {
        return getEndpoint(getSuccessor(getToken(endpoint)));
    }

    /** caller should not modify bootstrapTokens */
    public Map<Token, InetAddress> getBootstrapTokens()
    {
        return bootstrapTokens;
    }

    /** caller should not modify leavigEndpoints */
    public Set<InetAddress> getLeavingEndpoints()
    {
        return leavingEndpoints;
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     */
    public static Iterator<Token> ringIterator(final List ring, Token start)
    {
        assert ring.size() > 0;
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
            {
                i = 0;
            }
        }
        final int startIndex = i;
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < 0)
                    return endOfData();
                try
                {
                    return (Token) ring.get(j);
                }
                finally
                {
                    j = (j + 1) % ring.size();
                    if (j == startIndex)
                        j = -1;
                }
            }
        };
    }

    /** used by tests */
    public void clearUnsafe()
    {
        bootstrapTokens.clear();
        tokenToEndpointMap.clear();
        leavingEndpoints.clear();
        pendingRanges.clear();
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try
        {
            Set<InetAddress> eps = tokenToEndpointMap.inverse().keySet();

            if (!eps.isEmpty())
            {
                sb.append("Normal Tokens:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : eps)
                {
                    sb.append(ep);
                    sb.append(":");
                    sb.append(tokenToEndpointMap.inverse().get(ep));
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!bootstrapTokens.isEmpty())
            {
                sb.append("Bootstrapping Tokens:" );
                sb.append(System.getProperty("line.separator"));
                for (Map.Entry<Token, InetAddress> entry : bootstrapTokens.entrySet())
                {
                    sb.append(entry.getValue() + ":" + entry.getKey());
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!leavingEndpoints.isEmpty())
            {
                sb.append("Leaving Endpoints:");
                sb.append(System.getProperty("line.separator"));
                for (InetAddress ep : leavingEndpoints)
                {
                    sb.append(ep);
                    sb.append(System.getProperty("line.separator"));
                }
            }

            if (!pendingRanges.isEmpty())
            {
                sb.append("Pending Ranges:");
                sb.append(System.getProperty("line.separator"));
                sb.append(printPendingRanges());
            }
        }
        finally
        {
            lock.readLock().unlock();
        }

        return sb.toString();
    }

    public String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Multimap<Range, InetAddress>> entry : pendingRanges.entrySet())
        {
            for (Map.Entry<Range, InetAddress> rmap : entry.getValue().entries())
            {
                sb.append(rmap.getValue() + ":" + rmap.getKey());
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();
    }
}
