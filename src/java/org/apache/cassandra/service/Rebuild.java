/*
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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RangeStreamer;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public class Rebuild
{
    private static final AtomicBoolean isRebuilding = new AtomicBoolean();

    private static final Logger logger = LoggerFactory.getLogger(Rebuild.class);

    @VisibleForTesting
    public static void unsafeResetRebuilding()
    {
        isRebuilding.set(false);
    }

    public static void rebuild(String sourceDc, String keyspace, String tokens, String specificSources, boolean excludeLocalDatacenterNodes, boolean refetchData)
    {
        // check ongoing rebuild
        if (!isRebuilding.compareAndSet(false, true))
        {
            throw new IllegalStateException("Node is still rebuilding. Check nodetool netstats.");
        }

        if (sourceDc != null)
        {
            if (sourceDc.equals(DatabaseDescriptor.getLocalDataCenter()) && excludeLocalDatacenterNodes) // fail if source DC is local and --exclude-local-dc is set
                throw new IllegalArgumentException("Cannot set source data center to be local data center, when excludeLocalDataCenter flag is set");
            Set<String> availableDCs = ClusterMetadata.current().directory.knownDatacenters();
            if (!availableDCs.contains(sourceDc))
            {
                throw new IllegalArgumentException(String.format("Provided datacenter '%s' is not a valid datacenter, available datacenters are: %s",
                                                                 sourceDc, String.join(",", availableDCs)));
            }
        }

        // if refetchData is set to true, truncate local available_ranges_v2 table so the node will fetch all data
        // from remote sources
        if (refetchData)
        {
            SystemKeyspace.resetAvailableStreamedRanges();
        }

        try
        {
            // check the arguments
            if (keyspace == null && tokens != null)
            {
                throw new IllegalArgumentException("Cannot specify tokens without keyspace.");
            }

            logger.info("rebuild from dc: {}, {}, {}", sourceDc == null ? "(any dc)" : sourceDc,
                        keyspace == null ? "(All keyspaces)" : keyspace,
                        tokens == null ? "(All tokens)" : tokens);

            StorageService.instance.repairPaxosForTopologyChange("rebuild");
            ClusterMetadata metadata = ClusterMetadata.current();
            MovementMap rebuildMovements = movementMap(metadata, keyspace, tokens);
            logger.info("Rebuild movements: {}", rebuildMovements);
            RangeStreamer streamer = new RangeStreamer(metadata,
                                                       StreamOperation.REBUILD,
                                                       false, // no strict consistency when rebuilding
                                                       DatabaseDescriptor.getEndpointSnitch(),
                                                       StorageService.instance.streamStateStore,
                                                       false,
                                                       DatabaseDescriptor.getStreamingConnectionsPerHost(),
                                                       rebuildMovements,
                                                       null);
            if (sourceDc != null)
                streamer.addSourceFilter(new RangeStreamer.SingleDatacenterFilter(DatabaseDescriptor.getEndpointSnitch(), sourceDc));

            if (excludeLocalDatacenterNodes)
                streamer.addSourceFilter(new RangeStreamer.ExcludeLocalDatacenterFilter(DatabaseDescriptor.getEndpointSnitch()));

            if (keyspace == null)
            {
                for (String keyspaceName : Schema.instance.getNonLocalStrategyKeyspaces().names())
                    streamer.addKeyspaceToFetch(keyspaceName);
            }
            else if (tokens == null)
            {
                streamer.addKeyspaceToFetch(keyspace);
            }
            else
            {
                if (specificSources != null)
                {
                    String[] stringHosts = specificSources.split(",");
                    Set<InetAddressAndPort> sources = new HashSet<>(stringHosts.length);
                    for (String stringHost : stringHosts)
                    {
                        try
                        {
                            InetAddressAndPort endpoint = InetAddressAndPort.getByName(stringHost);
                            if (getBroadcastAddressAndPort().equals(endpoint))
                            {
                                throw new IllegalArgumentException("This host was specified as a source for rebuilding. Sources for a rebuild can only be other nodes in the cluster.");
                            }
                            sources.add(endpoint);
                        }
                        catch (UnknownHostException ex)
                        {
                            throw new IllegalArgumentException("Unknown host specified " + stringHost, ex);
                        }
                    }
                    streamer.addSourceFilter(new RangeStreamer.AllowedSourcesFilter(sources));
                }

                streamer.addKeyspaceToFetch(keyspace);
            }

            streamer.fetchAsync().get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            // This is used exclusively through JMX, so log the full trace but only throw a simple RTE
            logger.error("Error while rebuilding node", e.getCause());
            throw new RuntimeException("Error while rebuilding node: " + e.getCause().getMessage());
        }
        finally
        {
            // rebuild is done (successfully or not)
            isRebuilding.set(false);
        }
    }


    private static RangesAtEndpoint rangesForRebuildWithTokens(String tokens, String keyspace)
    {
        Token.TokenFactory factory = StorageService.instance.getTokenFactory();
        List<Range<Token>> ranges = new ArrayList<>();
        Pattern rangePattern = Pattern.compile("\\(\\s*(-?\\w+)\\s*,\\s*(-?\\w+)\\s*\\]");
        try (Scanner tokenScanner = new Scanner(tokens))
        {
            while (tokenScanner.findInLine(rangePattern) != null)
            {
                MatchResult range = tokenScanner.match();
                Token startToken = factory.fromString(range.group(1));
                Token endToken = factory.fromString(range.group(2));
                logger.info("adding range: ({},{}]", startToken, endToken);
                ranges.add(new Range<>(startToken, endToken));
            }
            if (tokenScanner.hasNext())
                throw new IllegalArgumentException("Unexpected string: " + tokenScanner.next());
        }

        // Ensure all specified ranges are actually ranges owned by this host
        RangesAtEndpoint localReplicas = StorageService.instance.getLocalReplicas(keyspace);
        RangesAtEndpoint.Builder streamRanges = new RangesAtEndpoint.Builder(getBroadcastAddressAndPort(), ranges.size());
        for (Range<Token> specifiedRange : ranges)
        {
            boolean foundParentRange = false;
            for (Replica localReplica : localReplicas)
            {
                if (localReplica.contains(specifiedRange))
                {
                    streamRanges.add(localReplica.decorateSubrange(specifiedRange));
                    foundParentRange = true;
                    break;
                }
            }
            if (!foundParentRange)
            {
                throw new IllegalArgumentException(String.format("The specified range %s is not a range that is owned by this node. Please ensure that all token ranges specified to be rebuilt belong to this node.", specifiedRange.toString()));
            }
        }
        return streamRanges.build();
    }

    private static MovementMap movementMap(ClusterMetadata metadata, String keyspace, String tokens)
    {
        MovementMap.Builder movementMapBuilder = MovementMap.builder();
        DataPlacements placements = metadata.placements;
        if (keyspace == null)
        {
            placements.forEach((params, placement) -> movementMapBuilder.put(params, addMovementsForParams(placement, null)));
        }
        else if (tokens == null)
        {
            ReplicationParams params = Keyspace.open(keyspace).getMetadata().params.replication;
            movementMapBuilder.put(params, addMovementsForParams(placements.get(params), null));
        }
        else
        {
            ReplicationParams params = Keyspace.open(keyspace).getMetadata().params.replication;
            RangesAtEndpoint ranges = rangesForRebuildWithTokens(tokens, keyspace);
            movementMapBuilder.put(params, addMovementsForParams(placements.get(params), ranges));
        }
        return movementMapBuilder.build();
    }

    private static EndpointsByReplica addMovementsForParams(DataPlacement placement, RangesAtEndpoint ranges)
    {
        EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
        RangesAtEndpoint localReplicas = ranges != null ? ranges : placement.reads.byEndpoint().get(getBroadcastAddressAndPort());
        for (Replica localReplica : localReplicas)
        {
            placement.reads.forRange(localReplica.range().right).forEach(r -> {
                if (!r.equals(localReplica))
                    movements.put(localReplica, r);
            });
        }
        return movements.build();
    }
}
