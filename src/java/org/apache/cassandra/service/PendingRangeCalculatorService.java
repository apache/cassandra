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

package org.apache.cassandra.service;

import org.apache.cassandra.utils.BiMultiValMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.concurrent.*;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();

    private static Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);
    private final JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1), new NamedThreadFactory("PendingRangeCalculator"), "internal");

    public PendingRangeCalculatorService()
    {
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
    }

    private static class PendingRangeTask implements Runnable
    {
        public void run()
        {
            long start = System.currentTimeMillis();
            for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
            {
                calculatePendingRanges(Keyspace.open(keyspaceName).getReplicationStrategy(), keyspaceName);
            }
            logger.debug("finished calculation for {} keyspaces in {}ms", Schema.instance.getNonSystemKeyspaces().size(), System.currentTimeMillis() - start);
        }
    }

    public Future<?> update()
    {
        return executor.submit(new PendingRangeTask());
    }

    public void blockUntilFinished()
    {
        while (true)
        {
            if (executor.getActiveCount() + executor.getPendingTasks() == 0)
                break;
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    // public & static for testing purposes
    public static void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        TokenMetadata tm = StorageService.instance.getTokenMetadata();
        Multimap<Range<Token>, InetAddress> pendingRanges = HashMultimap.create();
        BiMultiValMap<Token, InetAddress> bootstrapTokens = tm.getBootstrapTokens();
        Set<InetAddress> leavingEndpoints = tm.getLeavingEndpoints();

        if (bootstrapTokens.isEmpty() && leavingEndpoints.isEmpty() && tm.getMovingEndpoints().isEmpty())
        {
            if (logger.isDebugEnabled())
                logger.debug("No bootstrapping, leaving or moving nodes, and no relocating tokens -> empty pending ranges for {}", keyspaceName);
            tm.setPendingRanges(keyspaceName, pendingRanges);
            return;
        }

        Multimap<InetAddress, Range<Token>> addressRanges = strategy.getAddressRanges();

        // Copy of metadata reflecting the situation after all leave operations are finished.
        TokenMetadata allLeftMetadata = tm.cloneAfterAllLeft();

        // get all ranges that will be affected by leaving nodes
        Set<Range<Token>> affectedRanges = new HashSet<Range<Token>>();
        for (InetAddress endpoint : leavingEndpoints)
            affectedRanges.addAll(addressRanges.get(endpoint));

        // for each of those ranges, find what new nodes will be responsible for the range when
        // all leaving nodes are gone.
        TokenMetadata metadata = tm.cloneOnlyTokenMap(); // don't do this in the loop! #7758
        for (Range<Token> range : affectedRanges)
        {
            Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, metadata));
            Set<InetAddress> newEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, allLeftMetadata));
            pendingRanges.putAll(range, Sets.difference(newEndpoints, currentEndpoints));
        }

        // At this stage pendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.

        // For each of the bootstrapping nodes, simply add and remove them one by one to
        // allLeftMetadata and check in between what their ranges would be.
        Multimap<InetAddress, Token> bootstrapAddresses = bootstrapTokens.inverse();
        for (InetAddress endpoint : bootstrapAddresses.keySet())
        {
            Collection<Token> tokens = bootstrapAddresses.get(endpoint);

            allLeftMetadata.updateNormalTokens(tokens, endpoint);
            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
                pendingRanges.put(range, endpoint);
            allLeftMetadata.removeEndpoint(endpoint);
        }

        // At this stage pendingRanges has been updated according to leaving and bootstrapping nodes.
        // We can now finish the calculation by checking moving and relocating nodes.

        // For each of the moving nodes, we do the same thing we did for bootstrapping:
        // simply add and remove them one by one to allLeftMetadata and check in between what their ranges would be.
        for (Pair<Token, InetAddress> moving : tm.getMovingEndpoints())
        {
            InetAddress endpoint = moving.right; // address of the moving node

            //  moving.left is a new token of the endpoint
            allLeftMetadata.updateNormalToken(moving.left, endpoint);

            for (Range<Token> range : strategy.getAddressRanges(allLeftMetadata).get(endpoint))
            {
                pendingRanges.put(range, endpoint);
            }

            allLeftMetadata.removeEndpoint(endpoint);
        }

        tm.setPendingRanges(keyspaceName, pendingRanges);

        if (logger.isDebugEnabled())
            logger.debug("Pending ranges:\n" + (pendingRanges.isEmpty() ? "<empty>" : tm.printPendingRanges()));
    }
}
