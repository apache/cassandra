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
package org.apache.cassandra.locator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.FBUtilities;

/**
 * <p>
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * </p>
 * <p>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totaling 6.
 * </p>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy
{
    public static final String REPLICATION_FACTOR = "replication_factor";
    
    private final Map<String, ReplicationFactor> datacenters;
    private final ReplicationFactor aggregateRf;
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

    public NetworkTopologyStrategy(String keyspaceName, Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, configOptions);

        int replicas = 0;
        int trans = 0;
        Map<String, ReplicationFactor> newDatacenters = new HashMap<>();
        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                // prepareOptions should have transformed any "replication_factor" options by now
                if (dc.equalsIgnoreCase(REPLICATION_FACTOR))
                    throw new ConfigurationException(REPLICATION_FACTOR + " should not appear as an option at construction time for NetworkTopologyStrategy");
                ReplicationFactor rf = ReplicationFactor.fromString(entry.getValue());
                replicas += rf.allReplicas;
                trans += rf.transientReplicas();
                newDatacenters.put(dc, rf);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        aggregateRf = ReplicationFactor.withTransient(replicas, trans);
    }

    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    private static final class DatacenterEndpoints
    {
        /** List accepted endpoints get pushed into. */
        EndpointsForRange.Builder replicas;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names aren't a problem.
         */
        Set<Location> racks;

        /** Number of replicas left to fill from this DC. */
        int rfLeft;
        int acceptableRackRepeats;
        int transients;

        DatacenterEndpoints(ReplicationFactor rf,
                            int rackCount,
                            int nodeCount,
                            EndpointsForRange.Builder replicas,
                            Set<Location> racks)
        {
            this.replicas = replicas;
            this.racks = racks;
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            this.rfLeft = Math.min(rf.allReplicas, nodeCount);
            // If there aren't enough racks in this DC to fill the RF, we'll still use at least one node from each rack,
            // and the difference is to be filled by the first encountered nodes.
            acceptableRackRepeats = rf.allReplicas - rackCount;

            // if we have fewer replicas than rf calls for, reduce transients accordingly
            int reduceTransients = rf.allReplicas - this.rfLeft;
            transients = Math.max(rf.transientReplicas() - reduceTransients, 0);
            ReplicationFactor.validate(rfLeft, transients);
        }

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the replicas set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        boolean addEndpointAndCheckIfDone(InetAddressAndPort ep, Location location, Range<Token> replicatedRange)
        {
            if (done())
                return false;

            if (replicas.endpoints().contains(ep))
                // Cannot repeat a node.
                return false;

            Replica replica = new Replica(ep, replicatedRange, rfLeft > transients);

            if (racks.add(location))
            {
                // New rack.
                --rfLeft;
                replicas.add(replica, Conflict.NONE);
                return done();
            }
            if (acceptableRackRepeats <= 0)
                // There must be rfLeft distinct racks left, do not add any more rack repeats.
                return false;

            replicas.add(replica, Conflict.NONE);
            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --acceptableRackRepeats;
            --rfLeft;
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }
    }

    @Override
    public DataPlacement calculateDataPlacement(Epoch epoch, List<Range<Token>> ranges, ClusterMetadata metadata)
    {
        return calculateDataPlacement(epoch, ranges, metadata.directory, metadata.tokenMap);
    }

    private DataPlacement calculateDataPlacement(Epoch epoch,
                                                 List<Range<Token>> ranges,
                                                 Directory directory,
                                                 TokenMap tokenMap)
    {
        ReplicaGroups.Builder builder = ReplicaGroups.builder();
        for (Range<Token> range : ranges)
        {
            EndpointsForRange endpointsForRange = calculateNaturalReplicas(range.right,
                                                                           range,
                                                                           directory,
                                                                           tokenMap,
                                                                           datacenters);
            builder.withReplicaGroup(VersionedEndpoints.forRange(epoch, endpointsForRange));
        }

        ReplicaGroups built = builder.build();
        return new DataPlacement(built, built);
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC.
     */
    @Override
    public EndpointsForRange calculateNaturalReplicas(Token searchToken, ClusterMetadata metadata)
    {
        // we want to preserve insertion order so that the first added endpoint becomes primary
        Range<Token> replicatedRange = TokenRingUtils.getRange(metadata.tokenMap.tokens(), searchToken);
        return calculateNaturalReplicas(searchToken, replicatedRange, metadata.directory, metadata.tokenMap, datacenters);
    }

    public static EndpointsForRange calculateNaturalReplicas(Token searchToken,
                                                             Range<Token> replicatedRange,
                                                             Directory directory,
                                                             TokenMap tokens,
                                                             Map<String, ReplicationFactor> datacenters)
    {
        EndpointsForRange.Builder builder = new EndpointsForRange.Builder(replicatedRange);
        Set<Location> seenRacks = new HashSet<>();

        // Check if we have exhausted all the members/racks of a DC
        assert !directory.allDatacenterEndpoints().isEmpty() && !directory.allDatacenterRacks().isEmpty() : "not aware of any cluster members";

        int dcsToFill = 0;
        Map<String, DatacenterEndpoints> dcs = new HashMap<>(datacenters.size() * 2);

        // Create a DatacenterEndpoints object for each non-empty DC.
        for (Map.Entry<String, ReplicationFactor> en : datacenters.entrySet())
        {
            String dc = en.getKey();
            ReplicationFactor rf = en.getValue();
            int nodeCount = sizeOrZero(directory.datacenterEndpoints(dc));

            if (rf.allReplicas <= 0 || nodeCount <= 0)
                continue;

            DatacenterEndpoints dcEndpoints = new DatacenterEndpoints(rf,
                                                                      sizeOrZero(directory.datacenterRacks(dc)),
                                                                      nodeCount,
                                                                      builder,
                                                                      seenRacks);
            dcs.put(dc, dcEndpoints);
            ++dcsToFill;
        }

        Iterator<Token> tokenIter = TokenRingUtils.ringIterator(tokens.tokens(), searchToken, false);
        while (dcsToFill > 0 && tokenIter.hasNext())
        {
            Token next = tokenIter.next();
            NodeId owner = tokens.owner(next);
            InetAddressAndPort ep = directory.endpoint(owner);
            Location location = directory.location(owner);
            DatacenterEndpoints dcEndpoints = dcs.get(location.datacenter);
            if (dcEndpoints != null && dcEndpoints.addEndpointAndCheckIfDone(ep, location, replicatedRange))
                --dcsToFill;
        }
        return builder.build();
    }

    private static int sizeOrZero(Multimap<?, ?> collection)
    {
        return collection != null ? collection.asMap().size() : 0;
    }

    private static int sizeOrZero(Collection<?> collection)
    {
        return collection != null ? collection.size() : 0;
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return aggregateRf;
    }

    public ReplicationFactor getReplicationFactor(String dc)
    {
        ReplicationFactor replicas = datacenters.get(dc);
        return replicas == null ? ReplicationFactor.ZERO : replicas;
    }

    public Set<String> getDatacenters()
    {
        return datacenters.keySet();
    }

    @Override
    public Collection<String> recognizedOptions(ClusterMetadata metadata)
    {
        // only valid options are valid DC names.
        return Datacenters.getValidDatacenters(metadata);
    }

    /**
     * Support datacenter auto-expansion for CASSANDRA-14303. This hook allows us to safely auto-expand
     * the "replication_factor" options out into the known datacenters. It is called via reflection from
     * {@link AbstractReplicationStrategy#prepareReplicationStrategyOptions(Class, Map, Map)}.
     *
     * @param options The proposed strategy options that will be potentially mutated. If empty, replication_factor will
     *                be added either from previousOptions if one exists, or from default_keyspace_rf configuration.
     * @param previousOptions Any previous strategy options in the case of an ALTER statement
     */
    protected static void prepareOptions(Map<String, String> options, Map<String, String> previousOptions)
    {
        // add replication_factor only if there is no explicit mention of DCs. Otherwise, non-mentioned DCs will be added with default RF
        if (options.isEmpty())
        {
            String rf = previousOptions.containsKey(REPLICATION_FACTOR) ? previousOptions.get(REPLICATION_FACTOR)
                                                                        : Integer.toString(DatabaseDescriptor.getDefaultKeyspaceRF());
            options.putIfAbsent(REPLICATION_FACTOR, rf);
        }

        String replication = options.remove(REPLICATION_FACTOR);

        if (replication == null && options.size() == 0)
        {
            // Support direct alters from SimpleStrategy to NTS
            replication = previousOptions.get(REPLICATION_FACTOR);
        }
        else if (replication != null)
        {
            // When datacenter auto-expansion occurs in e.g. an ALTER statement (meaning that the previousOptions
            // map is not empty) we choose not to alter existing datacenter replication levels for safety.
            previousOptions.entrySet().stream()
                           .filter(e -> !e.getKey().equals(REPLICATION_FACTOR)) // SimpleStrategy conversions
                           .forEach(e -> options.putIfAbsent(e.getKey(), e.getValue()));
        }

        if (replication != null) {
            ReplicationFactor defaultReplicas = ReplicationFactor.fromString(replication);
            Datacenters.getValidDatacenters(ClusterMetadata.current())
                       .forEach(dc -> options.putIfAbsent(dc, defaultReplicas.toParseableString()));
        }

        options.values().removeAll(Collections.singleton("0"));
    }

    @Override
    public void validateExpectedOptions(ClusterMetadata metadata) throws ConfigurationException
    {
        // Do not accept query with no data centers specified.
        if (this.configOptions.isEmpty())
        {
            throw new ConfigurationException("Configuration for at least one datacenter must be present");
        }

        // Validate the data center names
        super.validateExpectedOptions(metadata);

        if (keyspaceName.equalsIgnoreCase(SchemaConstants.AUTH_KEYSPACE_NAME))
        {
            Set<String> differenceSet = Sets.difference(metadata.directory.knownDatacenters(), configOptions.keySet());
            if (!differenceSet.isEmpty())
            {
                throw new ConfigurationException("Following datacenters have active nodes and must be present in replication options for keyspace " + SchemaConstants.AUTH_KEYSPACE_NAME + ": " + differenceSet.toString());
            }
        }
        logger.info("Configured datacenter replicas are {}", FBUtilities.toString(datacenters));
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            // prepareOptions should have transformed any "replication_factor" by now
            if (e.getKey().equalsIgnoreCase(REPLICATION_FACTOR))
                throw new ConfigurationException(REPLICATION_FACTOR + " should not appear as an option to NetworkTopologyStrategy");
            validateReplicationFactor(e.getValue());
        }
    }

    @Override
    public void maybeWarnOnOptions(ClientState state)
    {
        if (!SchemaConstants.isSystemKeyspace(keyspaceName))
        {
            Directory directory = ClusterMetadata.current().directory;
            Multimap<String, InetAddressAndPort> dcsNodes = directory.allDatacenterEndpoints();
            for (Entry<String, String> e : this.configOptions.entrySet())
            {
                String dc = e.getKey();
                ReplicationFactor rf = getReplicationFactor(dc);
                Guardrails.minimumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
                Guardrails.maximumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
                int nodeCount = dcsNodes.containsKey(dc) ? dcsNodes.get(dc).size() : 0;
                // nodeCount==0 on many tests
                if (rf.fullReplicas > nodeCount && nodeCount != 0)
                {
                    String msg = "Your replication factor " + rf.fullReplicas
                                 + " for keyspace "
                                 + keyspaceName
                                 + " is higher than the number of nodes "
                                 + nodeCount
                                 + " for datacenter "
                                 + dc;
                    ClientWarn.instance.warn(msg);
                    logger.warn(msg);
                }
            }
        }
    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        return super.hasSameSettings(other) && ((NetworkTopologyStrategy) other).datacenters.equals(datacenters);
    }
}
