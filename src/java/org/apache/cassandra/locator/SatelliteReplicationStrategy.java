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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.agrona.collections.IntHashSet;
import org.agrona.collections.Object2IntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.paxos.Commit.Agreed;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.SatellitePaxosParticipants;
import org.apache.cassandra.service.reads.AlwaysSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static java.lang.String.format;
import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;

/**
 * Replication strategy for  CEP-58: Satellite Datacenters
 * allows pairing full datacenters with smaller satellite DCs * that use witness replicas for mutation tracking and
 * enable transactionally consistent failover without requiring 3 full datacenters.
 *
 * Configuration syntax uses dot notation:
 *
 * CREATE KEYSPACE ks WITH replication = {
 *   'class': 'SatelliteReplicationStrategy',
 *   'DC1': '3',                    // Full datacenter with RF=3
 *   'DC1.satellite.ST1': '3/3',    // Satellite ST1 for DC1 with 3 witness replicas
 *   'DC2': '3',                    // Second full DC
 *   'DC2.satellite.ST2': '3/3',    // Satellite ST2 for DC2
 *   'primary': 'DC1'               // Primary datacenter designation
 *   'DC2.disabled': 'true',        // Replication to DC1 disabled (disabled DC can't be primary, use for down DCs)
 * } AND replication_type = tracked;
 *
 * Requirements:
 * - Keyspace must use tracked replication (replication_type = tracked)
 * - Cluster must have transient replication enabled (transient_replication_enabled: true)
 * - Satellites must use witness replica format 'N/N' where all replicas are transient
 *
 * Uses NetworkTopologyStrategy replica selection algorithm for compatibility with existing NTS keyspaces
 */
public class SatelliteReplicationStrategy extends AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SatelliteReplicationStrategy.class);

    private static boolean ADDL_CHECKS_ENABLED = CassandraRelevantProperties.SATELLITE_REPLICATION_ADDITIONAL_CHECKS.getBoolean();

    private static final String PRIMARY_DC_KEY = "primary";
    private static final String SATELLITE_KEY_PATTERN = ".satellite.";
    private static final String DISABLED_KEY_SUFFIX = ".disabled";

    private final Map<String, ReplicationFactor> fullDCs;

    private final Map<String, SatelliteInfo> satellites;

    private final Map<String, ReplicationFactor> allDCs;

    private final String primaryDC;

    private final Set<String> disabledDCs;

    private final ReplicationFactor aggregateRf;

    /**
     * Per-range failover state.
     * Volatile for visibility across threads.
     *
     * NOTE: In-memory for initial implementation. Future work will pull this
     * from TCM (Transactional Cluster Metadata) for persistence and coordination.
     *
     * Initialized to NORMAL state for all ranges.
     */
    private volatile SatelliteFailoverState.FailoverStateMap failoverState;

    private static class SatelliteInfo
    {
        public final String parentDC;

        public final String name;

        public final ReplicationFactor rf;

        SatelliteInfo(String parentDC, String name, ReplicationFactor rf)
        {
            this.parentDC = parentDC;
            this.name = name;
            this.rf = rf;
        }
    }

    private static void cfgEx(String format, Object... args)
    {
        throw new ConfigurationException(format(format, args));
    }

    private static class StrategyOptions
    {
        Map<String, ReplicationFactor> fullDatacenters = new HashMap<>();
        Map<String, SatelliteInfo> satellites = new HashMap<>();
        Map<String, ReplicationFactor> allDatacenters = new HashMap<>();
        Set<String> disabledDCs = new HashSet<>();
        String primary = null;

        StrategyOptions(Map<String, String> configOptions)
        {

            if (configOptions != null)
            {
                for (Map.Entry<String, String> entry : configOptions.entrySet())
                {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    if (setPrimary(key, value))
                        continue;

                    if (addSatellite(key, value))
                        continue;

                    if (addPrimary(key, value))
                        continue;

                    if (key.contains("."))
                        cfgEx("Datacenter names cannot contain dots: '%s'. Use '<DC>.satellite.<SAT>' for satellites", key);

                    ReplicationFactor rf = ReplicationFactor.fromString(value);
                    fullDatacenters.put(key, rf);
                }
            }

            allDatacenters.putAll(fullDatacenters);
            satellites.forEach((dc, info) -> allDatacenters.put(dc, info.rf));
        }

        boolean setPrimary(String key, String value)
        {
            if (!key.equalsIgnoreCase(PRIMARY_DC_KEY))
                return false;

            if (primary != null)
                cfgEx("'primary' option specified multiple times");

            primary = value;
            return true;
        }

        boolean addSatellite(String key, String value)
        {
            if (!key.contains(SATELLITE_KEY_PATTERN))
                return false;

            String[] parts = key.split("\\" + SATELLITE_KEY_PATTERN);
            if (parts.length != 2)
                cfgEx("Invalid satellite configuration key '%s'. Expected format: '<DC>.satellite.<SAT>'", key);

            String parentDc = parts[0];
            String satelliteName = parts[1];

            if (parentDc.contains(".") || satelliteName.contains("."))
                cfgEx("Datacenter names cannot contain dots: '%s'", key);

            // Parse RF - satellites must use witness format (e.g., '3/3')
            if (!value.contains("/"))
                cfgEx("Satellite replicas must be specified as witness replicas using format 'N/N' (e.g., '3/3'). Got '%s'", value);

            String[] rfParts = value.split("/");
            if (rfParts.length != 2)
                cfgEx("Invalid replication factor format for satellite '%s': '%s'", satelliteName, value);

            try
            {
                int total = Integer.parseInt(rfParts[0]);
                int transientCount = Integer.parseInt(rfParts[1]);

                if (transientCount != total)
                    cfgEx("Satellite replicas must all be witnesses. Expected '%d/%d' but got '%s'", total, total, value);

                if (total <= 0)
                    cfgEx("Satellite replication factor must be positive. Got '%s'", value);

                satellites.put(satelliteName, new SatelliteInfo(parentDc, satelliteName,
                                                               ReplicationFactor.withTransient(total, total)));
            }
            catch (NumberFormatException e)
            {
                cfgEx("Invalid replication factor format for satellite '%s': '%s'", satelliteName, value);
            }
            return true;
        }

        boolean addPrimary(String key, String value)
        {
            if (!key.endsWith(DISABLED_KEY_SUFFIX))
                return false;

            String dcName = key.substring(0, key.length() - DISABLED_KEY_SUFFIX.length());
            if (dcName.isEmpty())
                cfgEx("Invalid disabled configuration key '%s'", key);
            if (dcName.contains("."))
                cfgEx("Datacenter names cannot contain dots: '%s'", dcName);

            if ("true".equalsIgnoreCase(value))
                disabledDCs.add(dcName);
            else if (!"false".equalsIgnoreCase(value))
                cfgEx("Invalid value for '%s': expected 'true' or 'false', got '%s'", key, value);

            return true;
        }
    }

    public SatelliteReplicationStrategy(String keyspaceName,
                                        Map<String, String> configOptions,
                                        ReplicationType replicationType) throws ConfigurationException
    {
        super(keyspaceName, configOptions, replicationType);

        StrategyOptions opts = new StrategyOptions(configOptions);

        this.fullDCs = Collections.unmodifiableMap(opts.fullDatacenters);
        this.satellites = Collections.unmodifiableMap(opts.satellites);
        this.allDCs = Collections.unmodifiableMap(opts.allDatacenters);
        this.primaryDC = opts.primary;
        this.disabledDCs = Collections.unmodifiableSet(opts.disabledDCs);

        validate();

        int totalReplicas = 0;
        int totalTransient = 0;
        for (ReplicationFactor rf : fullDCs.values())
        {
            totalReplicas += rf.allReplicas;
            totalTransient += rf.transientReplicas();
        }

        this.aggregateRf = ReplicationFactor.withTransient(totalReplicas, totalTransient);

        this.failoverState = SatelliteFailoverState.FailoverStateMap.allRanges(
        SatelliteFailoverState.FailoverInfo.normal());

        if (disabledDCs.isEmpty())
            logger.info("Configured satellite datacenter replication for keyspace {} with full datacenters {} (primary: {}), satellites {}",
                        keyspaceName, fullDCs, primaryDC, satellites);
        else
            logger.info("Configured satellite datacenter replication for keyspace {} with full datacenters {} (primary: {}), satellites {}, disabled {}",
                        keyspaceName, fullDCs, primaryDC, satellites, disabledDCs);
    }

    private void validate()
    {
        if (primaryDC == null)
            cfgEx("'primary' option is required");

        if (!fullDCs.containsKey(primaryDC))
            cfgEx("Primary datacenter '%s' must be defined as a full datacenter", primaryDC);

        for (SatelliteInfo satInfo : satellites.values())
        {
            if (!fullDCs.containsKey(satInfo.parentDC))
                cfgEx("Satellite '%s' references non-existent full datacenter '%s'", satInfo.name, satInfo.parentDC);
        }

        for (String disabledDC : disabledDCs)
        {
            if (!fullDCs.containsKey(disabledDC))
                cfgEx("Disabled datacenter '%s' is not defined as a full datacenter", disabledDC);
        }

        if (disabledDCs.contains(primaryDC))
            cfgEx("Primary datacenter '%s' cannot be disabled", primaryDC);
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token searchToken, ClusterMetadata metadata)
    {
        return NetworkTopologyStrategy.calculateNaturalReplicas(
            searchToken,
            TokenRingUtils.getRange(metadata.tokenMap.tokens(), searchToken),
            metadata.directory,
            metadata.tokenMap,
            allDCs);
    }

    @Override
    public DataPlacement calculateDataPlacement(Epoch epoch, List<Range<Token>> ranges, ClusterMetadata metadata)
    {
        ReplicaGroups.Builder builder = ReplicaGroups.builder();

        for (Range<Token> range : ranges)
        {
            EndpointsForRange endpointsForRange = calculateNaturalReplicas(range.right, metadata);
            builder.withReplicaGroup(VersionedEndpoints.forRange(epoch, endpointsForRange));
        }

        ReplicaGroups built = builder.build();
        return new DataPlacement(built, built);
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return aggregateRf;
    }

    @Override
    public Collection<String> recognizedOptions(ClusterMetadata metadata)
    {
        // accept anything - validation happens in constructor
        return null;
    }

    @Override
    public void validateExpectedOptions(ClusterMetadata metadata) throws ConfigurationException
    {
        // must use tracked replication type
        if (!replicationType.isTracked())
        {
            throw new ConfigurationException(
                getClass().getSimpleName() + " requires tracked replication. " +
                "Use 'AND replication_type = tracked' when creating/updating the keyspace");
        }

        // must use paxos v2
        if (DatabaseDescriptor.getPaxosVariant() != Config.PaxosVariant.v2)
        {
            throw new ConfigurationException(
                getClass().getSimpleName() + " requires paxos_variant=v2. " +
                "Set 'paxos_variant: v2' in cassandra.yaml");
        }

        // must have witness replicas enabled
        if (!DatabaseDescriptor.isTransientReplicationEnabled())
        {
            throw new ConfigurationException(
                getClass().getSimpleName() + " requires transient replication to be enabled. " +
                "Set 'transient_replication_enabled: true' in cassandra.yaml");
        }

        // must have at least one full DC
        if (fullDCs.isEmpty())
        {
            throw new ConfigurationException(
                "Configuration for at least one full datacenter must be present");
        }

        // validate datacenter names against topology
        Set<String> knownDcs = metadata.directory.knownDatacenters();
        for (String dc : fullDCs.keySet())
        {
            if (!knownDcs.contains(dc))
            {
                throw new ConfigurationException(format(
                    "Unrecognized datacenter '%s'. Known DCs: %s", dc, knownDcs));
            }
        }

        // validate satellite DCs exist and don't overlap with full DCs
        for (SatelliteInfo satInfo : satellites.values())
        {
            if (!knownDcs.contains(satInfo.name))
            {
                throw new ConfigurationException(format(
                    "Unrecognized satellite datacenter '%s'. Known DCs: %s", satInfo.name, knownDcs));
            }

            if (fullDCs.containsKey(satInfo.name))
            {
                throw new ConfigurationException(format(
                    "Datacenter '%s' cannot be both a full datacenter and a satellite",
                    satInfo.name));
            }
        }

        // check for pending mutation tracking migrations
        MutationTrackingMigrationState migrationState = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo migrationInfo = migrationState.getKeyspaceInfo(keyspaceName);

        if (migrationInfo != null && !migrationInfo.isComplete())
        {
            throw new ConfigurationException(format(
                "Cannot alter replication strategy for keyspace %s while mutation tracking migration is in progress",
                keyspaceName));
        }
    }

    @Override
    public void validateOptions() throws ConfigurationException
    {
        // validate full DC replication factors
        for (Map.Entry<String, String> entry : this.configOptions.entrySet())
        {
            String key = entry.getKey();
            if (key.equalsIgnoreCase(PRIMARY_DC_KEY) || key.contains(SATELLITE_KEY_PATTERN) || key.endsWith(DISABLED_KEY_SUFFIX))
                continue;

            validateReplicationFactor(entry.getValue());
        }

        // satellites are validated in constructor
    }

    @Override
    public void maybeWarnOnOptions(ClientState state)
    {
        if (SchemaConstants.isSystemKeyspace(keyspaceName))
            return;

        Directory directory = ClusterMetadata.current().directory;
        Multimap<String, InetAddressAndPort> dcsNodes =
            directory.allDatacenterEndpoints();

        for (Map.Entry<String, ReplicationFactor> entry : fullDCs.entrySet())
        {
            String dc = entry.getKey();
            ReplicationFactor rf = entry.getValue();

            if (disabledDCs.contains(dc))
                continue;

            // apply guardrails
            Guardrails.minimumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);
            Guardrails.maximumReplicationFactor.guard(rf.fullReplicas, keyspaceName, false, state);

            // warn if rF > node count
            int nodeCount = dcsNodes.containsKey(dc) ? dcsNodes.get(dc).size() : 0;
            if (rf.fullReplicas > nodeCount && nodeCount != 0)
            {
                String msg = format(
                    "Your replication factor %d for keyspace %s is higher than the number of nodes %d for datacenter %s",
                    rf.fullReplicas, keyspaceName, nodeCount, dc);
                ClientWarn.instance.warn(msg);
                logger.warn(msg);
            }
        }

        for (SatelliteInfo satInfo : satellites.values())
        {
            if (disabledDCs.contains(satInfo.parentDC))
                continue;

            String dc = satInfo.name;
            int replicaCount = satInfo.rf.allReplicas;

            int nodeCount = dcsNodes.containsKey(dc) ? dcsNodes.get(dc).size() : 0;
            if (replicaCount > nodeCount && nodeCount != 0)
            {
                String msg = format(
                    "Your replication factor %d for keyspace %s is higher than the number of nodes %d for satellite datacenter %s",
                    replicaCount, keyspaceName, nodeCount, dc);
                ClientWarn.instance.warn(msg);
                logger.warn(msg);
            }
        }
    }

    public String getPrimaryDC()
    {
        return primaryDC;
    }

    public Set<String> getDatacenters()
    {
        return fullDCs.keySet();
    }

    public ReplicationFactor getReplicationFactor(String dc)
    {
        ReplicationFactor rf = fullDCs.get(dc);
        return rf != null ? rf : ReplicationFactor.ZERO;
    }

    public Map<String, Integer> getSatellites()
    {
        Map<String, Integer> result = Maps.newHashMapWithExpectedSize(satellites.size());
        satellites.forEach((k, v) -> result.put(k, v.rf.allReplicas));
        return result;
    }

    public Set<String> getDisabledDatacenters()
    {
        return disabledDCs;
    }

    public boolean isDisabled(String dc)
    {
        return disabledDCs.contains(dc);
    }

    @Override
    public Paxos.Participants paxosParticipants(ClusterMetadata metadata,
                                                 TableMetadata table,
                                                 Token token,
                                                 ConsistencyLevel consistencyForConsensus,
                                                 Predicate<Replica> isReplicaAlive)
    {
        // Reject paxos operations during TRANSITION_ACK to prevent conflicting paxos operations
        // across different full DCs. The temporary gap in paxos availability ensures that the old
        // primary has no in-flight proposals before the new primary begins serving paxos operations.
        SatelliteFailoverState.FailoverInfo failoverInfo = getFailoverInfo(token, metadata);
        if (failoverInfo.getState() == SatelliteFailoverState.State.TRANSITION_ACK)
            throw UnavailableException.create(consistencyForConsensus, 1, 0);

        KeyspaceMetadata keyspaceMetadata = metadata.schema.getKeyspaceMetadata(table.keyspace);
        ReplicaLayout.ForTokenWrite fullLayout = ReplicaLayout.forTokenWriteLiveAndDown(metadata, keyspaceMetadata, token);

        // Paxos consensus operates entirely within the primary DC
        Predicate<Replica> inPrimaryDC = rp -> metadata.locator.location(rp.endpoint()).datacenter.equals(primaryDC);
        ReplicaLayout.ForTokenWrite primaryAll = fullLayout.filter(inPrimaryDC);
        ReplicaLayout.ForTokenWrite primaryElectorate = primaryAll;

        // Satellite/secondary DC endpoints for reads during prepare and writes during commit.
        // Only include the primary DC's satellite and other full DCs — not satellites of other DCs.
        String primarySatellite = getSatelliteForDC(primaryDC);
        Predicate<Replica> isParticipatingNonPrimary = rp -> {
            String dc = metadata.locator.location(rp.endpoint()).datacenter;
            if (dc.equals(primaryDC))
                return false;
            if (dc.equals(primarySatellite))
                return true;
            return fullDCs.containsKey(dc);
        };
        EndpointsForToken satelliteEndpoints = fullLayout.all().filter(isParticipatingNonPrimary);

        EndpointsForToken live = fullLayout.all().filter(isReplicaAlive);

        Token actualToken = token;
        return new SatellitePaxosParticipants(metadata.epoch,
                                              Keyspace.open(table.keyspace),
                                              consistencyForConsensus,
                                              primaryAll,
                                              primaryElectorate,
                                              live,
                                              (cm) -> Paxos.Participants.get(cm, table, actualToken, consistencyForConsensus),
                                              satelliteEndpoints);
    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        if (!super.hasSameSettings(other))
            return false;

        if (!(other instanceof SatelliteReplicationStrategy))
            return false;

        SatelliteReplicationStrategy otherStrategy = (SatelliteReplicationStrategy) other;

        return fullDCs.equals(otherStrategy.fullDCs) &&
               satellites.equals(otherStrategy.satellites) &&
               primaryDC.equals(otherStrategy.primaryDC) &&
               disabledDCs.equals(otherStrategy.disabledDCs);
    }

    private AbstractReplicationStrategy strategy(ClusterMetadata metadata)
    {
        return metadata.schema.getKeyspaceMetadata(keyspaceName).replicationStrategy;
    }

    static abstract class CoordinationPlanner<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan<E, P>>
    {
        final ClusterMetadata metadata;
        final Keyspace keyspace;
        final ConsistencyLevel cl;
        final SatelliteReplicationStrategy strategy;
        final String primary;
        final String satellite;
        final List<String> dcs;
        final L fullLayout;
        final L liveLayout;
        final Map<String, L> fullLayouts;
        final Map<String, L> liveLayouts;
        final Map<String, E> fullEndpoints;
        final Map<String, E> liveEndpoints;

        public CoordinationPlanner(ClusterMetadata metadata, Keyspace keyspace, ConsistencyLevel cl, SatelliteReplicationStrategy strategy, String primary, L fullLayout)
        {
            this.metadata = metadata;
            this.keyspace = keyspace;
            this.cl = cl;
            this.strategy = strategy;
            this.primary = primary;
            this.satellite = strategy.getSatelliteForDC(primary);

            this.dcs = createDcList();
            Preconditions.checkState(!dcs.isEmpty(), "No DCs available for request (primary=%s, all disabled?)", primary);

            Set<String> dcSet = new HashSet<>(dcs);
            this.fullLayout = filterLayout(fullLayout, rp -> dcSet.contains(metadata.locator.location(rp.endpoint()).datacenter));
            this.liveLayout = filterLayout(this.fullLayout, FailureDetector.isReplicaAlive);

            this.fullLayouts = Maps.newHashMapWithExpectedSize(dcs.size());
            this.liveLayouts = Maps.newHashMapWithExpectedSize(dcs.size());

            this.fullEndpoints = Maps.newHashMapWithExpectedSize(dcs.size());
            this.liveEndpoints = Maps.newHashMapWithExpectedSize(dcs.size());

            populateDcLayouts();
        }

        AbstractReplicationStrategy strategy(ClusterMetadata metadata)
        {
            return metadata.schema.getKeyspaceMetadata(keyspace.getName()).replicationStrategy;
        }

        List<String> createDcList()
        {
            List<String> results = new ArrayList<>();
            if (!strategy.disabledDCs.contains(primary))
                results.add(primary);

            if (satellite != null && !strategy.disabledDCs.contains(satellite))
                results.add(satellite);

            for (String dc : strategy.fullDCs.keySet())
            {
                if (dc.equals(primary) || dc.equals(satellite) || strategy.disabledDCs.contains(dc))
                    continue;
                results.add(dc);
            }

            Preconditions.checkState(!results.isEmpty());
            return results;
        }

        void populateDcLayouts()
        {
            for (String dc : dcs)
            {
                Predicate<Replica> dcFilter = rp -> metadata.locator.location(rp.endpoint()).datacenter.equals(dc);
                L full = filterLayout(fullLayout, dcFilter);
                L live = filterLayout(liveLayout, dcFilter);

                fullLayouts.put(dc, full);
                liveLayouts.put(dc, live);

                fullEndpoints.put(dc, full.natural());
                liveEndpoints.put(dc, live.natural());
            }

            if (ADDL_CHECKS_ENABLED)
                Preconditions.checkState(fullLayouts.size() == dcs.size(), "populateDcLayouts: fullLayouts.size()=%s != dcs.size()=%s", fullLayouts.size(), dcs.size());
        }

        ResponseTrackerBuilder<E, L, P> createResponseTrackerBuilder()
        {
            switch (cl)
            {
                case ONE:
                case LOCAL_ONE:
                case NODE_LOCAL:
                case ANY:
                case TWO:
                case THREE:
                case ALL:
                    return new ResponseTrackerBuilder.Simple<>(this);

                case QUORUM:
                case LOCAL_QUORUM:
                    // LOCAL_QUORUM an QUORUM mean the same thing in SRS
                    return new ResponseTrackerBuilder.Quorum<>(this);

                case EACH_QUORUM:
                    return new ResponseTrackerBuilder.EachQuorum<>(this);

                default:
                    throw new IllegalStateException("Unsupported consistency level: " + cl);
            }
        }

        ResponseTracker createResponseTracker()
        {
            ResponseTrackerBuilder<E, L, P> builder = createResponseTrackerBuilder();
            return builder.build();
        }

        abstract L filterLayout(L layout, Predicate<Replica> predicate);
        abstract ResponseTracker createDcResponseTracker(String dc);

        abstract P recomputePlan(ClusterMetadata metadata);
        abstract P createReplicaPlan();

        interface IndexReadExceptionCheck<E extends Endpoints<E>>
        {
            void maybeThrowIndexReadException(int initial, int filtered, Map<String, E> candidates, Map<InetAddressAndPort, Index.Status> indexStatusMap);
        }

        static class ForWrite extends CoordinationPlanner<EndpointsForToken, ReplicaLayout.ForTokenWrite, ReplicaPlan.ForWrite>
        {
            final ReplicaPlans.Selector selector;

            public ForWrite(ClusterMetadata metadata, Keyspace keyspace, ConsistencyLevel cl, SatelliteReplicationStrategy strategy, String primary, ReplicaLayout.ForTokenWrite fullLayout, ReplicaPlans.Selector selector)
            {
                super(metadata, keyspace, cl, strategy, primary, fullLayout);
                this.selector = selector;
                if (ADDL_CHECKS_ENABLED)
                {
                    Preconditions.checkState(!strategy.disabledDCs.contains(primary));
                }
            }

            @Override
            ResponseTracker createDcResponseTracker(String dc)
            {
                // this basically hardcodes ReplicaPlans.writeAll
                Predicate<InetAddressAndPort> dcFilter = ep -> metadata.locator.location(ep).datacenter.equals(dc);

                ReplicaLayout.ForTokenWrite dcLayout = fullLayouts.get(dc);
                Preconditions.checkNotNull(dcLayout);

                int blockFor = strategy.calculateQuorum(dc);
                int totalContacts = dcLayout.all().size();

                // Only count pending replicas that are actually in the selected contacts
                int pendingInDc = dcLayout.pending().size();

                if (pendingInDc == 0)
                {
                    return new SimpleResponseTracker(blockFor, totalContacts, dcFilter);
                }
                else
                {
                    int committedContacts = totalContacts - pendingInDc;
                    int totalBlockFor = blockFor + pendingInDc;
                    Predicate<InetAddressAndPort> isPending = ep -> dcLayout.pending.endpoints().contains(ep);
                    return new WriteResponseTracker(blockFor, totalBlockFor,
                                                    committedContacts, pendingInDc,
                                                    isPending, dcFilter);
                }
            }

            @Override
            ReplicaPlan.ForWrite recomputePlan(ClusterMetadata metadata)
            {
                Token token = fullLayout.token();
                return strategy(metadata).planForWrite(metadata, keyspace, cl,
                                                              cm -> ReplicaLayout.forTokenWriteLiveAndDown(cm, keyspace, token),
                                                       selector).replicas();
            }

            @Override
            ReplicaLayout.ForTokenWrite filterLayout(ReplicaLayout.ForTokenWrite layout, Predicate<Replica> predicate)
            {
                return layout.filter(predicate);
            }

            private ReplicaLayout.ForTokenWrite mergeLayouts(Collection<ReplicaLayout.ForTokenWrite> layouts)
            {
                ReplicaCollection.Builder<EndpointsForToken> naturalBuilder = null;
                ReplicaCollection.Builder<EndpointsForToken> pendingBuilder = null;

                for (ReplicaLayout.ForTokenWrite layout : layouts)
                {
                    EndpointsForToken natural = layout.natural();
                    EndpointsForToken pending = layout.pending();
                    if (naturalBuilder == null)
                    {
                        naturalBuilder = natural.newBuilder(natural.size());
                        pendingBuilder = pending.newBuilder(pending.size());
                    }

                    naturalBuilder.addAll(natural);
                    pendingBuilder.addAll(pending);
                }

                return new ReplicaLayout.ForTokenWrite(strategy, naturalBuilder.build(), pendingBuilder.build());
            }

            @Override
            ReplicaPlan.ForWrite createReplicaPlan()
            {

                ReplicaLayout.ForTokenWrite fullLayout = mergeLayouts(fullLayouts.values());
                ReplicaLayout.ForTokenWrite liveLayout = mergeLayouts(liveLayouts.values());
                EndpointsForToken contacts = selector.select(cl, fullLayout, liveLayout);

                return new ReplicaPlan.ForWrite(keyspace,
                                                strategy,
                                                cl,
                                                fullLayout.pending(),
                                                fullLayout.all(),
                                                liveLayout.all(),
                                                contacts,
                                                this::recomputePlan,
                                                metadata.epoch);
            }
        }

        static abstract class ForRead<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan.ForRead<E, P>> extends CoordinationPlanner<E, L, P>
        {
            final @Nullable Index.QueryPlan indexQueryPlan;
            final boolean alwaysSpeculate;
            final boolean throwOnInsufficientLiveReplicas;

            public ForRead(ClusterMetadata metadata, Keyspace keyspace, ConsistencyLevel cl, SatelliteReplicationStrategy strategy, String primary, L fullLayout, @Nullable Index.QueryPlan indexQueryPlan, boolean alwaysSpeculate, boolean throwOnInsufficientLiveReplicas)
            {
                super(metadata, keyspace, cl, strategy, primary, fullLayout);
                this.indexQueryPlan = indexQueryPlan;
                this.alwaysSpeculate = alwaysSpeculate;
                this.throwOnInsufficientLiveReplicas = throwOnInsufficientLiveReplicas;
            }

            @Override
            ResponseTracker createDcResponseTracker(String dc)
            {
                Predicate<InetAddressAndPort> dcFilter = ep -> metadata.locator.location(ep).datacenter.equals(dc);

                L dcLayout = fullLayouts.get(dc);
                int blockFor = strategy.calculateQuorum(dc);
                int totalContacts = dcLayout.all().size();

                return new SimpleResponseTracker(blockFor, totalContacts, dcFilter);
            }

            Map<String, E> candidates(IndexReadExceptionCheck<E> check)
            {
                if (indexQueryPlan == null)
                    return liveEndpoints;

                Map<String, E> candidates = new HashMap<>();
                Map<InetAddressAndPort, Index.Status> indexStatusMap = new HashMap<>();
                int initial = 0;
                int filtered = 0;
                for (Map.Entry<String, E> entry : liveEndpoints.entrySet())
                {
                    E liveEndpoints = entry.getValue();
                    E queryableEndpoints = IndexStatusManager.instance.filterForQuery(liveEndpoints, keyspace.getName(), indexQueryPlan, indexStatusMap);
                    initial += liveEndpoints.size();
                    filtered += queryableEndpoints.size();
                    candidates.put(entry.getKey(), queryableEndpoints);
                }

                if (initial != filtered)
                    check.maybeThrowIndexReadException(initial, filtered, candidates, indexStatusMap);

                return candidates;
            }

            @Override
            ResponseTracker createResponseTracker()
            {
                ResponseTracker tracker = super.createResponseTracker();

                // read trackers don't wait on pending nodes
                Preconditions.checkState(tracker.pendingContacts() == 0);
                return tracker;
            }


            abstract P replicaPlanBuilder(E candidates, E contacts, E liveAndDown, int quorumSize);

            ReadReplicaPlanBuilder<E, L, P> replicaPlanBuilder()
            {
                switch (cl)
                {
                    case ONE:
                    case LOCAL_ONE:
                    case NODE_LOCAL:
                    case ANY:
                    case TWO:
                    case THREE:
                    case ALL:
                        return new ReadReplicaPlanBuilder.Simple<>(this);

                    case QUORUM:
                    case LOCAL_QUORUM:
                    case EACH_QUORUM:
                        return new ReadReplicaPlanBuilder.DcQuorum<>(this);

                    default:
                        throw new IllegalStateException("Unsupported consistency level: " + cl);
                }
            }

            @Override
            P createReplicaPlan()
            {
                ReadReplicaPlanBuilder<E, L, P> builder = replicaPlanBuilder();
                return builder.build();
            }
        }

        static class ForTokenRead extends ForRead<EndpointsForToken, ReplicaLayout.ForTokenRead, ReplicaPlan.ForTokenRead>
        {
            static final Function<ReplicaPlan<?, ?>, ReplicaPlan.ForWrite> READ_REPAIR_PLAN = (ReplicaPlan<?, ?> self) -> {throw new IllegalStateException("Cannot create read repair plans for SatelliteDatacenterReplicationStrategy");};
            final Token token;
            final TableId tableId;
            final SpeculativeRetryPolicy retry;
            final ReadCoordinator coordinator;

            public ForTokenRead(ClusterMetadata metadata, Keyspace keyspace, Token token, ConsistencyLevel cl, SatelliteReplicationStrategy strategy, String primary, ReplicaLayout.ForTokenRead fullLayout, @Nullable Index.QueryPlan indexQueryPlan, SpeculativeRetryPolicy retry, boolean throwOnInsufficientLiveReplicas, TableId tableId, ReadCoordinator coordinator)
            {
                super(metadata, keyspace, cl, strategy, primary, fullLayout, indexQueryPlan, retry == AlwaysSpeculativeRetryPolicy.INSTANCE, throwOnInsufficientLiveReplicas);
                this.token = token;
                this.tableId = tableId;
                this.retry = retry;
                this.coordinator = coordinator;
            }

            @Override
            ReplicaLayout.ForTokenRead filterLayout(ReplicaLayout.ForTokenRead layout, Predicate<Replica> predicate)
            {
                return layout.filter(predicate);
            }

            @Override
            ReplicaPlan.ForTokenRead recomputePlan(ClusterMetadata metadata)
            {
                return strategy(metadata).planForTokenRead(metadata, keyspace, tableId, token,
                                                           indexQueryPlan, cl, retry, coordinator).replicas();
            }

            @Override
            ReplicaPlan.ForTokenRead replicaPlanBuilder(EndpointsForToken candidates, EndpointsForToken contacts, EndpointsForToken liveAndDown, int quorumSize)
            {
                return new ReplicaPlan.ForTokenRead(keyspace, strategy, cl,
                                                    candidates, contacts, liveAndDown,
                                                    this::recomputePlan,
                                                    READ_REPAIR_PLAN,
                                                    metadata.epoch,
                                                    quorumSize);
            }
        }

        static class ForRangeRead extends ForRead<EndpointsForRange, ReplicaLayout.ForRangeRead, ReplicaPlan.ForRangeRead>
        {
            static final BiFunction<ReplicaPlan<?, ?>, Token, ReplicaPlan.ForWrite> READ_REPAIR_PLAN = (ReplicaPlan<?, ?> self, Token token) -> {throw new IllegalStateException("Cannot create read repair plans for SatelliteDatacenterReplicationStrategy");};
            final AbstractBounds<PartitionPosition> range;
            final int vnodeCount;
            final TableId tableId;

            public ForRangeRead(ClusterMetadata metadata, Keyspace keyspace, AbstractBounds<PartitionPosition> range, int vnodeCount, ConsistencyLevel cl, SatelliteReplicationStrategy strategy, String primary, ReplicaLayout.ForRangeRead fullLayout, @Nullable Index.QueryPlan indexQueryPlan, boolean throwOnInsufficientLiveReplicas, TableId tableId)
            {
                super(metadata, keyspace, cl, strategy, primary, fullLayout, indexQueryPlan, false, throwOnInsufficientLiveReplicas);
                this.range = range;
                this.vnodeCount = vnodeCount;
                this.tableId = tableId;
            }

            @Override
            ReplicaLayout.ForRangeRead filterLayout(ReplicaLayout.ForRangeRead layout, Predicate<Replica> predicate)
            {
                return layout.filter(predicate);
            }

            @Override
            ReplicaPlan.ForRangeRead recomputePlan(ClusterMetadata metadata)
            {
                return strategy(metadata).planForRangeRead(metadata, keyspace, tableId,
                                                           indexQueryPlan, cl, range, vnodeCount).replicas();
            }

            @Override
            ReplicaPlan.ForRangeRead replicaPlanBuilder(EndpointsForRange candidates, EndpointsForRange contacts, EndpointsForRange liveAndDown, int quorumSize)
            {
                return new ReplicaPlan.ForRangeRead(keyspace, strategy, cl, range,
                                                    candidates, contacts, liveAndDown, vnodeCount,
                                                    this::recomputePlan,
                                                    READ_REPAIR_PLAN,
                                                    metadata.epoch,
                                                    quorumSize);
            }
        }
    }

    static abstract class ResponseTrackerBuilder<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan<E, P>>
    {
        final CoordinationPlanner<E, L, P> planner;

        public ResponseTrackerBuilder(CoordinationPlanner<E, L, P> planner)
        {
            this.planner = planner;
        }

        Map<String, ResponseTracker> responseTrackersForPrimary()
        {
            Map<String, ResponseTracker> result = Maps.newHashMapWithExpectedSize(planner.dcs.size());

            for (String dc : planner.dcs)
                result.put(dc, planner.createDcResponseTracker(dc));

            return result;
        }

        abstract ResponseTracker aggregateTrackers(Map<String, ResponseTracker> dcTrackers);

        ResponseTracker build()
        {
            Map<String, ResponseTracker> dcTrackers = responseTrackersForPrimary();
            return aggregateTrackers(dcTrackers);
        }

        static class Simple<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan<E, P>> extends ResponseTrackerBuilder<E, L, P>
        {
            public Simple(CoordinationPlanner<E, L, P> planner)
            {
                super(planner);
            }

            @Override
            ResponseTracker aggregateTrackers(Map<String, ResponseTracker> dcTrackers)
            {
                int totalContacts = 0;
                int totalPending = 0;


                List<ResponseTracker> trackers = Lists.newArrayList();
                Set<String> blockingDatacenters = Sets.newHashSet();

                for (Map.Entry<String, ResponseTracker> entry : dcTrackers.entrySet())
                {
                    String dc = entry.getKey();
                    ResponseTracker tracker = entry.getValue();
                    if (planner.cl == ConsistencyLevel.ALL || dc.equals(planner.primary) || dc.equals(planner.satellite))
                    {
                        totalContacts += tracker.totalContacts();
                        totalPending += tracker.pendingContacts();
                        trackers.add(tracker);
                        blockingDatacenters.add(dc);
                    }
                }

                Predicate<InetAddressAndPort> filter = i -> {
                    String dc = planner.metadata.locator.location(i).datacenter;
                    return dc != null && blockingDatacenters.contains(dc);
                };

                Preconditions.checkState(!trackers.isEmpty(), "No blocking DCs for CL %s (primary=%s, satellite=%s)", planner.cl, planner.primary, planner.satellite);


                if (planner.cl == ConsistencyLevel.ALL)
                    return new SimpleResponseTracker(totalContacts, totalContacts, filter);

                int blockFor = planner.cl.blockFor(planner.strategy);
                int totalBlockFor = blockFor + totalPending;

                Preconditions.checkState(blockFor <= totalContacts, "Simple tracker: blockFor=%s > totalContacts=%s for CL %s", blockFor, totalContacts, planner.cl);


                if (totalPending == 0)
                    return new SimpleResponseTracker(blockFor, totalContacts, filter);

                Predicate<InetAddressAndPort> pending = i -> {
                    for (ResponseTracker tracker : trackers)
                    {
                        if (tracker.isPending(i))
                            return true;
                    }
                    return false;
                };
                return new WriteResponseTracker(blockFor, totalBlockFor, totalContacts, totalPending, pending, filter);
            }
        }

        static class Quorum<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan<E, P>> extends ResponseTrackerBuilder<E, L, P>
        {
            public Quorum(CoordinationPlanner<E, L, P> planner)
            {
                super(planner);
            }

            @Override
            ResponseTracker aggregateTrackers(Map<String, ResponseTracker> dcTrackers)
            {
                return new CompositeTracker(CompositeTracker.quorum(dcTrackers.size()), new ArrayList<>(dcTrackers.values()));
            }
        }

        static class EachQuorum<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan<E, P>> extends ResponseTrackerBuilder<E, L, P>
        {
            public EachQuorum(CoordinationPlanner<E, L, P> planner)
            {
                super(planner);
            }

            @Override
            ResponseTracker aggregateTrackers(Map<String, ResponseTracker> dcTrackers)
            {
                return new CompositeTracker(CompositeTracker.all(dcTrackers.size()), new ArrayList<>(dcTrackers.values()));
            }
        }
    }

    static abstract class ReadReplicaPlanBuilder<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan.ForRead<E, P>>
            implements CoordinationPlanner.IndexReadExceptionCheck<E>
    {

        private static <E extends Endpoints<E>> Iterable<Replica> removedSupplier(Map<String, E> initial, Map<String, E> filtered)
        {
            return () -> initial.keySet().stream().map(dc -> {
                Set<InetAddressAndPort> f =  filtered.containsKey(dc) ? filtered.get(dc).endpoints() : Collections.emptySet();
                return initial.get(dc).without(f);
            }).flatMap(AbstractReplicaCollection::stream).iterator();
        }

        final CoordinationPlanner.ForRead<E, L, P> planner;

        public ReadReplicaPlanBuilder(CoordinationPlanner.ForRead<E, L, P> planner)
        {
            this.planner = planner;
        }

        abstract boolean canSelectReplicasFromDc(String dc, int liveNodes);
        abstract int minBlockFor();
        abstract int eligibleCandidates(Map<String, E> candidates);

        abstract boolean haveSufficientContacts(int contactedReplicas, int contactedDcs);
        abstract boolean haveSufficientContactsForDc(String dc, int contactedReplicas, int contactedDcs, int dcContacts);
        abstract boolean haveSufficientLiveNodes(int contactedReplicas, int contactedDcs);

        private E allLiveAndDown()
        {
            E fullEndpoints = planner.fullLayout.all();
            ReplicaCollection.Builder<E> fullBuilder = fullEndpoints.newBuilder(fullEndpoints.size());

            for (String dc : planner.dcs)
            {
                E endpoints = planner.fullEndpoints.get(dc);
                if (endpoints == null)
                    continue;

                fullBuilder.addAll(endpoints);
            }

            return fullBuilder.build();
        }

        private E allCandidates(Map<String, E> candidates)
        {
            E liveEndpoints = planner.liveLayout.all();
            ReplicaCollection.Builder<E> candidateBuilder = liveEndpoints.newBuilder(liveEndpoints.size());

            for (String dc : planner.dcs)
            {
                // TODO: make sure the full replica is added if this is the dc it's from
                E dcCandidates = candidates.get(dc);
                if (dcCandidates == null || !canSelectReplicasFromDc(dc, dcCandidates.size()))
                    continue;

                candidateBuilder.addAll(dcCandidates);
            }

            return candidateBuilder.build();
        }

        private E contacts(Map<String, E> candidates)
        {
            E liveEndpoints = planner.liveLayout.all();

            ReplicaCollection.Builder<E> contactBuilder = liveEndpoints.newBuilder(liveEndpoints.size());

            // find full replica
            Replica fullReplica = null;
            String fullDc = null;
            for (String dc : planner.dcs)
            {
                if (fullReplica != null)
                    break;

                E dcCandidates = candidates.get(dc);
                if (dcCandidates == null || !canSelectReplicasFromDc(dc, dcCandidates.size()))
                    continue;

                for (Replica replica : dcCandidates)
                {
                    if (replica.isFull())
                    {
                        fullReplica = replica;
                        fullDc = dc;
                        break;
                    }
                }
            }

            if (fullReplica == null)
            {
                // No full replicas available - throw error similar to assureSufficientLiveReplicas
                throw UnavailableException.create(ConsistencyLevel.ONE, minBlockFor(), 1, eligibleCandidates(candidates), 0);
            }

            int totalContacts = 0;
            int totalContactedDcs = 0;

            // add dc with full replicas first
            {
                E dcCandidates = candidates.get(fullDc);
                Preconditions.checkNotNull(dcCandidates);

                contactBuilder.add(fullReplica);
                totalContacts++;
                int dcContacts = 1;

                if (!haveSufficientContacts(totalContacts, totalContactedDcs))
                {
                    for (Replica replica : dcCandidates)
                    {
                        if (replica.equals(fullReplica))
                            continue;

                        if (haveSufficientContactsForDc(fullDc, totalContacts, totalContactedDcs, dcContacts))
                            break;

                        contactBuilder.add(replica);
                        totalContacts++;
                        dcContacts++;
                    }
                }
                totalContactedDcs++;
            }

            for (String dc : planner.dcs)
            {
                if (dc.equals(fullDc))
                    continue;

                E dcCandidates = candidates.get(dc);
                if (dcCandidates == null || dcCandidates.isEmpty())
                    continue;

                if (haveSufficientContacts(totalContacts, totalContactedDcs))
                    break;

                int dcContacts = 0;
                for (Replica replica : dcCandidates)
                {
                    if (haveSufficientContactsForDc(dc, totalContacts, totalContactedDcs, dcContacts))
                        break;

                    contactBuilder.add(replica);
                    totalContacts++;
                    dcContacts++;
                }

                totalContactedDcs++;
            }

            if (planner.throwOnInsufficientLiveReplicas && !haveSufficientLiveNodes(totalContacts, totalContactedDcs))
            {
                int fullReplicas = 0;
                for (String dc : planner.dcs)
                {
                    E dcCandidates = candidates.get(dc);
                    if (dcCandidates == null || !canSelectReplicasFromDc(dc, dcCandidates.size()))
                        continue;
                    for (Replica replica : dcCandidates)
                        if (replica.isFull())
                            fullReplicas++;
                }

                throw UnavailableException.create(planner.cl, minBlockFor(), 1, eligibleCandidates(candidates), fullReplicas);
            }

            E result = contactBuilder.build();

            if (ADDL_CHECKS_ENABLED)
            {
                // check for duplicate contacts
                Set<InetAddressAndPort> seen = new HashSet<>();
                for (Replica r : result)
                    Preconditions.checkState(seen.add(r.endpoint()), "Duplicate contact: %s", r.endpoint());
            }

            return result;
        }

        public P build()
        {
            Map<String, E> candidates = planner.candidates(this);

            E allEndpoints = allLiveAndDown();
            E allCandidates = allCandidates(candidates);
            E allContacts = contacts(candidates);

            if (ADDL_CHECKS_ENABLED)
            {
                // contacts must be a subset of candidates
                Set<InetAddressAndPort> candidateSet = allCandidates.endpoints();
                for (Replica contact : allContacts)
                    Preconditions.checkState(candidateSet.contains(contact.endpoint()), "Contact %s not in candidates", contact.endpoint());
            }

            return planner.replicaPlanBuilder(allCandidates, allContacts, allEndpoints, minBlockFor());
        }

        static class Simple<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan.ForRead<E, P>> extends ReadReplicaPlanBuilder<E, L, P>
        {
            private final int blockFor;
            private final int targetContacts;

            public Simple(CoordinationPlanner.ForRead<E, L, P> planner)
            {
                super(planner);

                if (planner.cl == ConsistencyLevel.ALL)
                {
                    int required = 0;
                    for (E replicas : planner.fullEndpoints.values())
                        required += replicas.size();

                    this.blockFor = required;
                }
                else
                {
                    this.blockFor = planner.cl.blockFor(planner.strategy);
                }

                this.targetContacts = planner.alwaysSpeculate ? blockFor + 1 : blockFor;
            }

            @Override
            public void maybeThrowIndexReadException(int initial, int filtered, Map<String, E> candidates, Map<InetAddressAndPort, Index.Status> indexStatusMap)
            {
                if (blockFor <= initial && blockFor > filtered)
                {
                    IndexStatusManager.instance.readFailureException(filtered, removedSupplier(planner.fullEndpoints, candidates), indexStatusMap, planner.cl, blockFor);
                }
            }

            @Override
            boolean canSelectReplicasFromDc(String dc, int liveNodes)
            {
                return true;
            }

            @Override
            int minBlockFor()
            {
                return blockFor;
            }

            @Override
            int eligibleCandidates(Map<String, E> candidates)
            {
                int eligible = 0;
                for (Map.Entry<String, E> entry : candidates.entrySet())
                {
                    eligible += entry.getValue().size();
                }
                return eligible;
            }

            @Override
            boolean haveSufficientContacts(int contactedReplicas, int contactedDcs)
            {
                return contactedReplicas >= targetContacts;
            }

            @Override
            boolean haveSufficientContactsForDc(String dc, int contactedReplicas, int contactedDcs, int dcContacts)
            {
                return haveSufficientContacts(contactedReplicas, contactedDcs);
            }

            @Override
            boolean haveSufficientLiveNodes(int contactedReplicas, int contactedDcs)
            {
                return contactedReplicas >= blockFor;
            }
        }

        static class DcQuorum<E extends Endpoints<E>, L extends ReplicaLayout<E>, P extends ReplicaPlan.ForRead<E, P>> extends ReadReplicaPlanBuilder<E, L, P>
        {
            private final int blockForDcs;
            private final Object2IntHashMap<String> dcQuorums = new Object2IntHashMap<>(-1);
            private boolean approvedSpeculativeReplica = false;

            public DcQuorum(CoordinationPlanner.ForRead<E, L, P> planner)
            {
                super(planner);
                this.blockForDcs = planner.cl != ConsistencyLevel.EACH_QUORUM ? (planner.dcs.size() / 2) + 1 : planner.dcs.size();
                for (String dc : planner.dcs)
                    dcQuorums.put(dc, planner.strategy.calculateQuorum(dc));
            }
            private int dcQuorum(String dc)
            {
                int v = dcQuorums.get(dc);
                Preconditions.checkArgument(v >= 0, "dcQuorum must be >= 0");
                return v;
            }

            @Override
            public void maybeThrowIndexReadException(int initial, int filtered, Map<String, E> candidates, Map<InetAddressAndPort, Index.Status> indexStatusMap)
            {
                int required = 0;
                int available = 0;
                int liveDcs = 0;
                for (Map.Entry<String, E> entry : candidates.entrySet())
                {
                    int dcQuorum = dcQuorum(entry.getKey());
                    int dcLive = entry.getValue().size();
                    if (dcLive >= dcQuorum)
                        liveDcs++;

                    required += dcQuorum;
                    available += entry.getValue().size();
                }

                if (liveDcs <= blockForDcs)
                {
                    IndexStatusManager.instance.readFailureException(available, removedSupplier(planner.fullEndpoints, candidates), indexStatusMap, planner.cl, required);
                }
            }

            @Override
            boolean canSelectReplicasFromDc(String dc, int liveNodes)
            {
                int dcQuorum = dcQuorum(dc);
                return liveNodes >= dcQuorum;
            }

            @Override
            int minBlockFor()
            {
                int[] quorums = new int[planner.dcs.size()];
                for (int i = 0; i < quorums.length; i++)
                    quorums[i] = dcQuorum(planner.dcs.get(i));

                Arrays.sort(quorums);

                int blockFor = 0;
                for (int i=0; i<blockForDcs; i++)
                    blockFor += quorums[i];
                return blockFor;
            }

            @Override
            int eligibleCandidates(Map<String, E> candidates)
            {
                int eligible = 0;
                for (Map.Entry<String, E> entry : candidates.entrySet())
                {
                    int dcLive = entry.getValue().size();
                    int dcQuorum = dcQuorum(entry.getKey());
                    if (dcLive >= dcQuorum)
                        eligible += dcLive;
                }

                return eligible;
            }

            @Override
            boolean haveSufficientContacts(int contactedReplicas, int contactedDcs)
            {
                return contactedDcs >= blockForDcs;
            }

            @Override
            boolean haveSufficientContactsForDc(String dc, int contactedReplicas, int contactedDcs, int dcContacts)
            {
                int dcQuorum = dcQuorum(dc);

                if (dcContacts < dcQuorum)
                    return false;

                if (dcContacts == dcQuorum && planner.alwaysSpeculate && !approvedSpeculativeReplica)
                {
                    approvedSpeculativeReplica = true;
                    return false;
                }

                return true;
            }

            @Override
            boolean haveSufficientLiveNodes(int contactedReplicas, int contactedDcs)
            {
                return contactedDcs >= blockForDcs;
            }
        }

    }

    @Override
    protected CoordinationPlan.ForWrite planForWriteInternal(ClusterMetadata metadata,
                                                             Keyspace keyspace,
                                                             ConsistencyLevel consistencyLevel,
                                                             Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDown,
                                                             ReplicaPlans.Selector selector)
    {
        // writes don't need any special handling during primary failover. We just write to the current
        // primary DC, regardless of any other failover status
        ReplicaLayout.ForTokenWrite fullLayout = liveAndDown.apply(metadata);

        CoordinationPlanner.ForWrite planner = new CoordinationPlanner.ForWrite(metadata, keyspace, consistencyLevel, this, primaryDC, fullLayout, selector);

        return new CoordinationPlan.ForWrite(planner.createReplicaPlan(), planner.createResponseTracker());
    }

    /**
     * Holds the information needed to send satellite commit mutations alongside a paxos commit.
     * The tracker has the primary DC pre-completed, so only satellite/secondary DC quorums
     * need actual responses.
     */
    static class SatelliteCommitPlan
    {
        final EndpointsForToken liveEndpoints;
        final EndpointsForToken downEndpoints;
        final ResponseTracker tracker;

        SatelliteCommitPlan(EndpointsForToken liveEndpoints, EndpointsForToken downEndpoints, ResponseTracker tracker)
        {
            this.liveEndpoints = liveEndpoints;
            this.downEndpoints = downEndpoints;
            this.tracker = tracker;
        }
    }

    SatelliteCommitPlan createSatelliteCommitPlan(ClusterMetadata metadata, Keyspace keyspace, Token token)
    {
        ReplicaLayout.ForTokenWrite fullLayout = ReplicaLayout.forTokenWriteLiveAndDown(metadata, keyspace, token);
        CoordinationPlanner.ForWrite planner = new CoordinationPlanner.ForWrite(metadata, keyspace, ConsistencyLevel.QUORUM,
                                                                             this, primaryDC, fullLayout, ReplicaPlans.writeAll);
        ResponseTracker tracker = planner.createResponseTracker();

        // paxos is handling the consensus/commit acks in the primary DC, we just need to worry about the witness DCs
        EndpointsForToken primaryEndpoints = planner.fullEndpoints.get(primaryDC);
        if (primaryEndpoints != null)
        {
            for (int i = 0; i < primaryEndpoints.size(); i++)
                tracker.onResponse(primaryEndpoints.endpoint(i));
        }

        // Collect satellite/secondary DC endpoints from the filtered layout.
        // planner.fullLayout is already filtered to only include DCs in the coordination plan
        // (primary DC + its satellite + other full DCs), excluding satellites of non-primary DCs.
        Predicate<Replica> notPrimary = rp -> !metadata.locator.location(rp.endpoint()).datacenter.equals(primaryDC);
        EndpointsForToken allSatellite = planner.fullLayout.all().filter(notPrimary);
        EndpointsForToken liveSatellite = allSatellite.filter(FailureDetector.isReplicaAlive);
        EndpointsForToken downSatellite = allSatellite.filter(rp -> !FailureDetector.isReplicaAlive.test(rp));

        return new SatelliteCommitPlan(liveSatellite, downSatellite, tracker);
    }

    /**
     * Sends plain mutations to satellite/secondary DCs alongside a paxos commit.
     *
     * Paxos consensus handles the primary DC writes. This method sends the committed mutation to satellite DCs using
     * the standard SRS write coordination. The primary DC group is pre-completed in the tracker so only satellite DC
     * quorums need actual responses.
     *
     * @return a future that completes when satellite DC quorum requirements are met (or failed)
     */
    @Override
    public Future<Void> sendPaxosCommitMutations(Agreed commit, boolean isUrgent)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Keyspace keyspace = Keyspace.open(commit.metadata().keyspace);
        Token token = commit.partitionKey().getToken();

        // Reject satellite commit writes during TRANSITION_ACK. Paxos operations should not be
        // in progress during this state, but check defensively to avoid propagating stale commits.
        SatelliteFailoverState.FailoverInfo failoverInfo = getFailoverInfo(token, metadata);
        if (failoverInfo.getState() == SatelliteFailoverState.State.TRANSITION_ACK)
            throw new UnavailableException("Paxos commit rejected during TRANSITION_ACK failover state",
                                           ConsistencyLevel.SERIAL, 1, 0);

        SatelliteCommitPlan plan = createSatelliteCommitPlan(metadata, keyspace, token);
        ResponseTracker tracker = plan.tracker;
        MutationId mutationId = commit.mutation.id();
        Preconditions.checkState(!mutationId.isNone());

        // Register satellite replicas with mutation tracking service
        if (!plan.liveEndpoints.isEmpty() || !plan.downEndpoints.isEmpty())
        {
            IntHashSet satelliteHostIds = new IntHashSet();
            for (int i = 0; i < plan.liveEndpoints.size(); i++)
                satelliteHostIds.add(metadata.directory.peerId(plan.liveEndpoints.endpoint(i)).id());
            for (int i = 0; i < plan.downEndpoints.size(); i++)
                satelliteHostIds.add(metadata.directory.peerId(plan.downEndpoints.endpoint(i)).id());
            if (!satelliteHostIds.isEmpty())
                MutationTrackingService.instance().sentWriteRequest(commit.makeMutation(), satelliteHostIds);
        }

        // Create promise that resolves when satellite tracker completes
        AsyncPromise<Void> promise = new AsyncPromise<>();

        // Send plain mutations to live satellite endpoints
        Message<Mutation> satelliteMessage = Message.out(Verb.PAXOS2_COMMIT_REMOTE_REQ, commit.makeMutation(), isUrgent);
        for (int i = 0, mi = plan.liveEndpoints.size(); i < mi; ++i)
        {
            InetAddressAndPort endpoint = plan.liveEndpoints.endpoint(i);
            logger.trace("Sending satellite commit mutation for {} to {}", commit.partitionKey(), endpoint);
            MessagingService.instance().sendWithCallback(satelliteMessage, endpoint, new RequestCallback<NoPayload>()
            {
                @Override
                public void onResponse(Message<NoPayload> msg)
                {
                    MutationTrackingService.instance().receivedWriteResponse(mutationId, msg.from());

                    tracker.onResponse(msg.from());
                    if (tracker.isComplete())
                        resolvePromise(promise, tracker);
                }

                @Override
                public void onFailure(InetAddressAndPort from, RequestFailure failure)
                {
                    MutationTrackingService.instance().retryFailedWrite(mutationId, from, failure);

                    tracker.onFailure(from, failure.reason);
                    if (tracker.isComplete())
                        resolvePromise(promise, tracker);
                }
            });
        }

        // Mark down satellite endpoints as failed
        for (int i = 0, mi = plan.downEndpoints.size(); i < mi; ++i)
        {
            InetAddressAndPort endpoint = plan.downEndpoints.endpoint(i);
            MutationTrackingService.instance().retryFailedWrite(mutationId, endpoint, RequestFailure.NODE_DOWN);
            tracker.onFailure(endpoint, UNKNOWN);
        }

        if (tracker.isComplete())
            resolvePromise(promise, tracker);

        return promise;
    }

    private static void resolvePromise(AsyncPromise<Void> promise, ResponseTracker tracker)
    {
        if (tracker.isSuccessful())
            promise.trySuccess(null);
        else
            promise.tryFailure(new RuntimeException("Satellite DC quorum not met"));
    }

    private CoordinationPlan.ForTokenRead planForTokenReadPrimary(ClusterMetadata metadata,
                                                                  String primary,
                                                                  Keyspace keyspace,
                                                                  TableId tableId,
                                                                  Token token,
                                                                  @Nullable Index.QueryPlan indexQueryPlan,
                                                                  ConsistencyLevel consistencyLevel,
                                                                  SpeculativeRetryPolicy retry,
                                                                  ReadCoordinator coordinator)
    {
        ReplicaLayout.ForTokenRead fullLayout = ReplicaLayout.forTokenReadSorted(metadata, keyspace, this, tableId, token, coordinator);

        CoordinationPlanner.ForTokenRead planner = new CoordinationPlanner.ForTokenRead(metadata, keyspace, token, consistencyLevel, this, primary, fullLayout, indexQueryPlan, retry, true, tableId, coordinator);

        return new CoordinationPlan.ForTokenRead(ReplicaPlan.shared(planner.createReplicaPlan()), planner.createResponseTracker());
    }

    @Override
    public CoordinationPlan.ForTokenRead planForTokenRead(ClusterMetadata metadata, Keyspace keyspace, TableId tableId, Token token, @Nullable Index.QueryPlan indexQueryPlan, ConsistencyLevel consistencyLevel, SpeculativeRetryPolicy retry, ReadCoordinator coordinator)
    {
        CoordinationPlan.ForTokenRead primaryPlan = planForTokenReadPrimary(metadata, primaryDC, keyspace, tableId, token, indexQueryPlan, consistencyLevel, retry, coordinator);

        SatelliteFailoverState.FailoverInfo failoverInfo = getFailoverInfo(token, metadata);
        if (!failoverInfo.isTransitioning())
            return primaryPlan;

        CoordinationPlan.ForTokenRead previousPlan = planForTokenReadPrimary(metadata, failoverInfo.getFromDC(), keyspace, tableId, token, indexQueryPlan, consistencyLevel, retry, coordinator);

        return mergeTokenReadPlans(primaryPlan, previousPlan, cm -> strategy(cm).planForTokenRead(cm, keyspace, tableId, token, indexQueryPlan, consistencyLevel, retry, coordinator).replicas());
    }

    /**
     * Merge two ForTokenRead coordination plans during failover transition. The primary plan's replicas are added
     * first to preserve proximity ordering for data vs digest request selection. Replicas from the previous plan
     * are appended if not already present in the primary plan.
     */
    private static CoordinationPlan.ForTokenRead mergeTokenReadPlans(CoordinationPlan.ForTokenRead primaryPlan,
                                                                     CoordinationPlan.ForTokenRead previousPlan,
                                                                     Function<ClusterMetadata, ReplicaPlan.ForTokenRead> recompute)
    {
        ReplicaPlan.ForTokenRead primary = primaryPlan.replicas();
        ReplicaPlan.ForTokenRead previous = previousPlan.replicas();

        EndpointsForToken candidates = mergeEndpoints(primary.readCandidates(), previous.readCandidates());
        EndpointsForToken contacts = mergeEndpoints(primary.contacts(), previous.contacts());
        EndpointsForToken liveAndDown = mergeEndpoints(primary.liveAndDown(), previous.liveAndDown());
        int readQuorum = Math.max(primary.readQuorum(), previous.readQuorum());

        ReplicaPlan.ForTokenRead merged = new ReplicaPlan.ForTokenRead(primary.keyspace(),
                                                                       primary.replicationStrategy(),
                                                                       primary.consistencyLevel(),
                                                                       candidates,
                                                                       contacts,
                                                                       liveAndDown,
                                                                       recompute,
                                                                       CoordinationPlanner.ForTokenRead.READ_REPAIR_PLAN,
                                                                       primary.epoch(),
                                                                       readQuorum);

        ResponseTracker tracker = new CompositeTracker(CompositeTracker.all(2), primaryPlan.responses(), previousPlan.responses());

        return new CoordinationPlan.ForTokenRead(ReplicaPlan.shared(merged), tracker);
    }

    private static <E extends Endpoints<E>> E mergeEndpoints(E first, E second)
    {
        ReplicaCollection.Builder<E> builder = first.newBuilder(first.size() + second.size());
        builder.addAll(first);
        builder.addAll(second, ReplicaCollection.Builder.Conflict.DUPLICATE);
        return builder.build();
    }

    private CoordinationPlan.ForRangeRead planForRangeReadPrimary(ClusterMetadata metadata,
                                                                  String primary,
                                                                  Keyspace keyspace,
                                                                  TableId tableId,
                                                                  AbstractBounds<PartitionPosition> range,
                                                                  int vnodeCount,
                                                                  @Nullable Index.QueryPlan indexQueryPlan,
                                                                  ConsistencyLevel consistencyLevel)
    {

        ReplicaLayout.ForRangeRead fullLayout = ReplicaLayout.forRangeReadSorted(metadata, keyspace, this, range);

        CoordinationPlanner.ForRangeRead planner = new CoordinationPlanner.ForRangeRead(metadata, keyspace, range, vnodeCount, consistencyLevel, this, primary, fullLayout, indexQueryPlan, true, tableId);

        return new CoordinationPlan.ForRangeRead(ReplicaPlan.shared(planner.createReplicaPlan()), planner.createResponseTracker());
    }


    @Override
    public CoordinationPlan.ForRangeRead planForRangeRead(ClusterMetadata metadata, Keyspace keyspace, TableId tableId, @Nullable Index.QueryPlan indexQueryPlan, ConsistencyLevel consistencyLevel, AbstractBounds<PartitionPosition> range, int vnodeCount)
    {
        CoordinationPlan.ForRangeRead primaryPlan = planForRangeReadPrimary(metadata, primaryDC, keyspace, tableId, range, vnodeCount, indexQueryPlan, consistencyLevel);

        SatelliteFailoverState.FailoverInfo failoverInfo = getFailoverInfo(range, metadata);
        if (!failoverInfo.isTransitioning())
            return primaryPlan;

        CoordinationPlan.ForRangeRead previousPlan = planForRangeReadPrimary(metadata, failoverInfo.getFromDC(), keyspace, tableId, range, vnodeCount, indexQueryPlan, consistencyLevel);

        return mergeRangeReadPlans(primaryPlan, previousPlan, cm -> strategy(cm).planForRangeRead(cm, keyspace, tableId, indexQueryPlan, consistencyLevel, range, vnodeCount).replicas());
    }

    private static CoordinationPlan.ForRangeRead mergeRangeReadPlans(CoordinationPlan.ForRangeRead primaryPlan,
                                                                     CoordinationPlan.ForRangeRead previousPlan,
                                                                     Function<ClusterMetadata, ReplicaPlan.ForRangeRead> recompute)
    {
        ReplicaPlan.ForRangeRead primary = primaryPlan.replicas();
        ReplicaPlan.ForRangeRead previous = previousPlan.replicas();

        EndpointsForRange candidates = mergeEndpoints(primary.readCandidates(), previous.readCandidates());
        EndpointsForRange contacts = mergeEndpoints(primary.contacts(), previous.contacts());
        EndpointsForRange liveAndDown = mergeEndpoints(primary.liveAndDown(), previous.liveAndDown());
        int readQuorum = Math.max(primary.readQuorum(), previous.readQuorum());

        ReplicaPlan.ForRangeRead merged = new ReplicaPlan.ForRangeRead(primary.keyspace(),
                                                                       primary.replicationStrategy(),
                                                                       primary.consistencyLevel(),
                                                                       primary.range(),
                                                                       candidates,
                                                                       contacts,
                                                                       liveAndDown,
                                                                       primary.vnodeCount(),
                                                                       recompute,
                                                                       CoordinationPlanner.ForRangeRead.READ_REPAIR_PLAN,
                                                                       primary.epoch(),
                                                                       readQuorum);

        ResponseTracker tracker = new CompositeTracker(CompositeTracker.all(2), primaryPlan.responses(), previousPlan.responses());

        return new CoordinationPlan.ForRangeRead(ReplicaPlan.shared(merged), tracker);
    }

    @Override
    public CoordinationPlan.ForRangeRead planForSingleReplicaRangeRead(Keyspace keyspace, AbstractBounds<PartitionPosition> range, Replica replica, int vnodeCount)
    {
        throw new IllegalStateException("planForSingleReplicaRangeRead not supported by SatelliteReplicationStrategy");
    }

    @Override
    public CoordinationPlan.ForTokenRead planForSingleReplicaTokenRead(Keyspace keyspace, Token token, Replica replica)
    {
        throw new IllegalStateException("planForSingleReplicaTokenRead not supported by SatelliteReplicationStrategy");
    }

    @Override
    public CoordinationPlan.ForRangeRead maybeMergeRangeReads(ClusterMetadata metadata, Keyspace keyspace, TableId tableId, ConsistencyLevel consistencyLevel, ReplicaPlan.ForRangeRead left, ReplicaPlan.ForRangeRead right)
    {
        return null;
    }

    private String getSatelliteForDC(String dc)
    {
        for (SatelliteInfo sat : satellites.values())
            if (sat.parentDC.equals(dc))
                return sat.name;
        return null;
    }

    private int calculateQuorum(String dc)
    {
        ReplicationFactor rf = fullDCs.get(dc);
        if (rf != null)
            return rf.fullReplicas / 2 + 1;

        SatelliteInfo sat = satellites.get(dc);
        return sat != null ? sat.rf.allReplicas / 2 + 1 : 0;
    }

    public SatelliteFailoverState.FailoverInfo getFailoverInfo(Token token, ClusterMetadata metadata)
    {
        Range<Token> range = TokenRingUtils.getRange(
            metadata.tokenMap.tokens(), token);
        return failoverState.getFailoverInfo(range);
    }

    public SatelliteFailoverState.FailoverInfo getFailoverInfo(AbstractBounds<PartitionPosition> range, ClusterMetadata metadata)
    {
        return failoverState.getFailoverInfo(range.right.getToken());
    }

    /**
     * Reject paxos consensus operations during TRANSITION_ACK or when the local node is not in the primary DC.
     *
     * Paxos consensus operates entirely within the primary DC. Nodes in satellite or secondary DCs should
     * never process paxos consensus messages. During TRANSITION_ACK, all paxos operations must be blocked
     * to prevent conflicting operations across different full DCs.
     */
    @Override
    public boolean shouldRejectPaxos(Token token)
    {
        ClusterMetadata metadata = ClusterMetadata.current();

        SatelliteFailoverState.FailoverInfo failoverInfo = getFailoverInfo(token, metadata);
        if (failoverInfo.getState() == SatelliteFailoverState.State.TRANSITION_ACK)
            return true;

        String localDC = metadata.locator.location(FBUtilities.getBroadcastAddressAndPort()).datacenter;
        return !primaryDC.equals(localDC);
    }

    @VisibleForTesting
    void setFailoverState(SatelliteFailoverState.FailoverStateMap state)
    {
        this.failoverState = state;
    }
}
