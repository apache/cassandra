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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;

import static java.lang.String.format;

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

    private static final String PRIMARY_DC_KEY = "primary";
    private static final String SATELLITE_KEY_PATTERN = ".satellite.";
    private static final String DISABLED_KEY_SUFFIX = ".disabled";

    private final Map<String, ReplicationFactor> fullDCs;

    private final Map<String, SatelliteInfo> satellites;

    private final Map<String, ReplicationFactor> allDCs;

    private final String primaryDC;

    private final Set<String> disabledDCs;

    private final ReplicationFactor aggregateRf;

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
}
