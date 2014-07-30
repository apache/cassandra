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

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();

    private static Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);
    private final JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1), new NamedThreadFactory("PendingRangeCalculator"), "internal");

    private AtomicInteger updateJobs = new AtomicInteger(0);

    public PendingRangeCalculatorService()
    {
        executor.setRejectedExecutionHandler(new RejectedExecutionHandler()
        {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e)
            {
                PendingRangeCalculatorService.instance.finishUpdate();
            }
        }
        );
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
            PendingRangeCalculatorService.instance.finishUpdate();
            logger.debug("finished calculation for {} keyspaces in {}ms", Schema.instance.getNonSystemKeyspaces().size(), System.currentTimeMillis() - start);
        }
    }

    private void finishUpdate()
    {
        updateJobs.decrementAndGet();
    }

    public void update()
    {
        updateJobs.incrementAndGet();
        executor.submit(new PendingRangeTask());
    }

    public void blockUntilFinished()
    {
        // We want to be sure the job we're blocking for is actually finished and we can't trust the TPE's active job count
        while (updateJobs.get() > 0)
        {
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
                logger.debug("No bootstrapping, leaving or moving nodes -> empty pending ranges for {}", keyspaceName);
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
        for (Range<Token> range : affectedRanges)
        {
            Set<InetAddress> currentEndpoints = ImmutableSet.copyOf(strategy.calculateNaturalEndpoints(range.right, tm.cloneOnlyTokenMap()));
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
