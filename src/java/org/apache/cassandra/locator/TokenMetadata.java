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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.*;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

public class TokenMetadata
{
    private static Logger logger = LoggerFactory.getLogger(TokenMetadata.class);

    /* Maintains token to endpoint map of every node in the cluster. */
    private BiMap<Token, InetAddress> tokenToEndpointMap;

    // Prior to CASSANDRA-603, we just had <tt>Map<Range, InetAddress> pendingRanges<tt>,
    // which was added to when a node began bootstrap and removed from when it finished.
    //
    // This is inadequate when multiple changes are allowed simultaneously.  For example,
    // suppose that there is a ring of nodes A, C and E, with replication factor 3.
    // Node D bootstraps between C and E, so its pending ranges will be E-A, A-C and C-D.
    // Now suppose node B bootstraps between A and C at the same time. Its pending ranges
    // would be C-E, E-A and A-B. Now both nodes need to be assigned pending range E-A,
    // which we would be unable to represent with the old Map.  The same thing happens
    // even more obviously for any nodes that boot simultaneously between same two nodes.
    //
    // So, we made two changes:
    //
    // First, we changed pendingRanges to a <tt>Multimap<Range, InetAddress></tt> (now
    // <tt>Map<String, Multimap<Range, InetAddress>></tt>, because replication strategy
    // and options are per-KeySpace).
    //
    // Second, we added the bootstrapTokens and leavingEndpoints collections, so we can
    // rebuild pendingRanges from the complete information of what is going on, when
    // additional changes are made mid-operation.
    //
    // Finally, note that recording the tokens of joining nodes in bootstrapTokens also
    // means we can detect and reject the addition of multiple nodes at the same token
    // before one becomes part of the ring.
    private BiMap<Token, InetAddress> bootstrapTokens = Maps.synchronizedBiMap(HashBiMap.<Token, InetAddress>create());
    // (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
    private Set<InetAddress> leavingEndpoints = new HashSet<InetAddress>();
    // this is a cache of the calculation from {tokenToEndpointMap, bootstrapTokens, leavingEndpoints}
    private ConcurrentMap<String, Multimap<Range, InetAddress>> pendingRanges = new ConcurrentHashMap<String, Multimap<Range, InetAddress>>();

    // nodes which are migrating to the new tokens in the ring
    private Set<Pair<Token, InetAddress>> movingEndpoints = new HashSet<Pair<Token, InetAddress>>();


    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private ArrayList<Token> sortedTokens;

    /* list of subscribers that are notified when the tokenToEndpointMap changed */
    private final CopyOnWriteArrayList<AbstractReplicationStrategy> subscribers = new CopyOnWriteArrayList<AbstractReplicationStrategy>();

    public TokenMetadata()
    {
        this(null);
    }

    public TokenMetadata(BiMap<Token, InetAddress> tokenToEndpointMap)
    {
        if (tokenToEndpointMap == null)
            tokenToEndpointMap = HashBiMap.create();
        this.tokenToEndpointMap = tokenToEndpointMap;
        sortedTokens = sortTokens();
    }

    private ArrayList<Token> sortTokens()
    {
        ArrayList<Token> tokens = new ArrayList<Token>(tokenToEndpointMap.keySet());
        Collections.sort(tokens);
        return tokens;
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
            InetAddress prev = tokenToEndpointMap.put(token, endpoint);
            if (!endpoint.equals(prev))
            {
                if (prev != null)
                    logger.warn("Token " + token + " changing ownership from " + prev + " to " + endpoint);
                sortedTokens = sortTokens();
            }
            leavingEndpoints.remove(endpoint);
            removeFromMoving(endpoint); // also removing this endpoint from moving
            invalidateCaches();
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
            InetAddress oldEndpoint;

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

    /**
     * Add a new moving endpoint
     * @param token token which is node moving to
     * @param endpoint address of the moving node
     */
    public void addMovingEndpoint(Token token, InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();

        try
        {
            movingEndpoints.add(new Pair<Token, InetAddress>(token, endpoint));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            bootstrapTokens.inverse().remove(endpoint);
            tokenToEndpointMap.inverse().remove(endpoint);
            leavingEndpoints.remove(endpoint);
            sortedTokens = sortTokens();
            invalidateCaches();
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove pair of token/address from moving endpoints
     * @param endpoint address of the moving node
     */
    public void removeFromMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.writeLock().lock();
        try
        {
            for (Pair<Token, InetAddress> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                {
                    movingEndpoints.remove(pair);
                    break;
                }
            }

            invalidateCaches();
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

    public boolean isMoving(InetAddress endpoint)
    {
        assert endpoint != null;

        lock.readLock().lock();

        try
        {
            for (Pair<Token, InetAddress> pair : movingEndpoints)
            {
                if (pair.right.equals(endpoint))
                    return true;
            }

            return false;
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
     *
     * @return new token metadata
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

    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave and move operations have finished.
     *
     * @return new token metadata
     */
    public TokenMetadata cloneAfterAllSettled()
    {
        lock.readLock().lock();

        try
        {
            TokenMetadata metadata = cloneOnlyTokenMap();

            for (InetAddress endpoint : leavingEndpoints)
                metadata.removeEndpoint(endpoint);


            for (Pair<Token, InetAddress> pair : movingEndpoints)
                metadata.updateNormalToken(pair.left, pair.right);

            return metadata;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public Set<Map.Entry<Token,InetAddress>> entrySet()
    {
        return tokenToEndpointMap.entrySet();
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

    public ArrayList<Token> sortedTokens()
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

    private Multimap<Range, InetAddress> getPendingRangesMM(String table)
    {
        Multimap<Range, InetAddress> map = pendingRanges.get(table);
        if (map == null)
        {
            map = HashMultimap.create();
            Multimap<Range, InetAddress> priorMap = pendingRanges.putIfAbsent(table, map);
            if (priorMap != null)
                map = priorMap;
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

    /** caller should not modify bootstrapTokens */
    public Map<Token, InetAddress> getBootstrapTokens()
    {
        return bootstrapTokens;
    }

    /** caller should not modify leavingEndpoints */
    public Set<InetAddress> getLeavingEndpoints()
    {
        return leavingEndpoints;
    }

    /**
     * Endpoints which are migrating to the new tokens
     * @return set of addresses of moving endpoints
     */
    public Set<Pair<Token, InetAddress>> getMovingEndpoints()
    {
        return movingEndpoints;
    }

    public static int firstTokenIndex(final ArrayList ring, Token start, boolean insertMin)
    {
        assert ring.size() > 0;
        // insert the minimum token (at index == -1) if we were asked to include it and it isn't a member of the ring
        int i = Collections.binarySearch(ring, start);
        if (i < 0)
        {
            i = (i + 1) * (-1);
            if (i >= ring.size())
                i = insertMin ? -1 : 0;
        }
        return i;
    }

    public static Token firstToken(final ArrayList<Token> ring, Token start)
    {
        return ring.get(firstTokenIndex(ring, start, false));
    }

    /**
     * iterator over the Tokens in the given ring, starting with the token for the node owning start
     * (which does not have to be a Token in the ring)
     * @param includeMin True if the minimum token should be returned in the ring even if it has no owner.
     */
    public static Iterator<Token> ringIterator(final ArrayList<Token> ring, Token start, boolean includeMin)
    {
        if (ring.isEmpty())
            return includeMin ? Iterators.singletonIterator(StorageService.getPartitioner().getMinimumToken())
                              : Iterators.<Token>emptyIterator();

        final boolean insertMin = (includeMin && !ring.get(0).equals(StorageService.getPartitioner().getMinimumToken())) ? true : false;
        final int startIndex = firstTokenIndex(ring, start, insertMin);
        return new AbstractIterator<Token>()
        {
            int j = startIndex;
            protected Token computeNext()
            {
                if (j < -1)
                    return endOfData();
                try
                {
                    // return minimum for index == -1
                    if (j == -1)
                        return StorageService.getPartitioner().getMinimumToken();
                    // return ring token for other indexes
                    return ring.get(j);
                }
                finally
                {
                    j++;
                    if (j == ring.size())
                        j = insertMin ? -1 : 0;
                    if (j == startIndex)
                        // end iteration
                        j = -2;
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
        invalidateCaches();
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

    public void invalidateCaches()
    {
        for (AbstractReplicationStrategy subscriber : subscribers)
        {
            subscriber.invalidateCachedTokenEndpointValues();
        }
    }

    public void register(AbstractReplicationStrategy subscriber)
    {
        subscribers.add(subscriber);
    }

    public void unregister(AbstractReplicationStrategy subscriber)
    {
        subscribers.remove(subscriber);
    }

    /**
     * write endpoints may be different from read endpoints, because read endpoints only need care about the
     * "natural" nodes for a token, but write endpoints also need to account for nodes that are bootstrapping
     * into the ring, and write data there too so that they stay up to date during the bootstrap process.
     * Thus, this method may return more nodes than the Replication Factor.
     *
     * If possible, will return the same collection it was passed, for efficiency.
     *
     * Only ReplicationStrategy should care about this method (higher level users should only ask for Hinted).
     */
    public Collection<InetAddress> getWriteEndpoints(Token token, String table, Collection<InetAddress> naturalEndpoints)
    {
        Map<Range, Collection<InetAddress>> ranges = getPendingRanges(table);
        if (ranges.isEmpty())
            return naturalEndpoints;

        Set<InetAddress> endpoints = new HashSet<InetAddress>(naturalEndpoints);

        for (Map.Entry<Range, Collection<InetAddress>> entry : ranges.entrySet())
        {
            if (entry.getKey().contains(token))
            {
                endpoints.addAll(entry.getValue());
            }
        }

        return endpoints;
    }

    /**
     * Return the Token to Endpoint map for all the node in the cluster, including bootstrapping ones.
     */
    public Map<Token, InetAddress> getTokenToEndpointMap()
    {
        Map<Token, InetAddress> map = new HashMap<Token, InetAddress>(tokenToEndpointMap.size() + bootstrapTokens.size());
        map.putAll(tokenToEndpointMap);
        map.putAll(bootstrapTokens);
        return map;
    }
}
