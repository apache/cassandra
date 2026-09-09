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
package org.apache.cassandra.cql3.statements.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNSAFE_TRANSIENT_CHANGES;
import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNSAFE_WITNESS_PROMOTION;
import static org.apache.cassandra.replication.MutationTrackingService.DISABLED_MESSAGE;

public final class AlterKeyspaceStatement extends AlterSchemaStatement
{
    private static final Logger logger = LoggerFactory.getLogger(AlterKeyspaceStatement.class);

    private static final boolean allow_alter_rf_during_range_movement = ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT.getBoolean();
    private static final boolean allow_unsafe_transient_changes = ALLOW_UNSAFE_TRANSIENT_CHANGES.getBoolean();

    private final KeyspaceAttributes attrs;
    private final boolean ifExists;

    public AlterKeyspaceStatement(String keyspaceName, KeyspaceAttributes attrs, boolean ifExists)
    {
        super(keyspaceName);
        this.attrs = attrs;
        this.ifExists = ifExists;
    }

    @Override
    public boolean compatibleWith(ClusterMetadata metadata)
    {
        return metadata.directory.commonSerializationVersion.isAtLeast(Version.V0);
    }

    public Keyspaces apply(ClusterMetadata metadata)
    {
        attrs.validate();

        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
        {
            if (!ifExists)
                throw ire("Keyspace '%s' doesn't exist", keyspaceName);
            return schema;
        }

        KeyspaceMetadata newKeyspace = keyspace.withSwapped(attrs.asAlteredKeyspaceParams(keyspace.params));

        if (keyspace.params.replication.isMeta() && !keyspace.name.equals(SchemaConstants.METADATA_KEYSPACE_NAME))
            throw ire("Can not alter a keyspace to use MetaReplicationStrategy");

        if (newKeyspace.params.replication.klass.equals(LocalStrategy.class))
            throw ire("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        newKeyspace.params.validate(keyspaceName, state, metadata);
        newKeyspace.replicationStrategy.validate(metadata);

        validateNoRangeMovements();
        validateTransientReplication(keyspace, newKeyspace);
        validateWitnessTransitions(metadata, keyspace, newKeyspace);

        // Because we used to not properly validate unrecognized options, we only log a warning if we find one.
        try
        {
            newKeyspace.replicationStrategy.validateExpectedOptions(metadata);
        }
        catch (ConfigurationException e)
        {
            logger.warn("Ignoring {}", e.getMessage());
        }

        return schema.withAddedOrUpdated(newKeyspace);
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, keyspaceName);
    }

    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.ALTER);
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        Guardrails.keyspaceProperties.guard(attrs.updatedProperties(), attrs::removeProperty, state);

        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (null == keyspace)
        {
            if (!ifExists)
                throw ire("Keyspace '%s' doesn't exist", keyspaceName);
            return;
        }

        KeyspaceMetadata newKeyspace = keyspace.withSwapped(attrs.asAlteredKeyspaceParams(keyspace.params));

        if (attrs.getReplicationStrategyClass() != null && attrs.getReplicationStrategyClass().equals(SimpleStrategy.class.getSimpleName()))
            Guardrails.simpleStrategyEnabled.ensureEnabled(state);

        if (newKeyspace.params.replicationType.isTracked() && !keyspace.params.replicationType.isTracked())
        {
            if (SchemaConstants.isSystemKeyspace(keyspaceName))
                throw ire("Mutation tracking is not supported on system keyspaces");

            if (!DatabaseDescriptor.getMutationTrackingEnabled())
                throw ire(DISABLED_MESSAGE);
        }
    }

    @Override
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        HashSet<String> clientWarnings = new HashSet<>();
        if (diff.isEmpty())
            return clientWarnings;

        KeyspaceDiff keyspaceDiff = diff.altered.get(0);

        AbstractReplicationStrategy before = keyspaceDiff.before.replicationStrategy;
        AbstractReplicationStrategy after = keyspaceDiff.after.replicationStrategy;

        if (before.getReplicationFactor().fullReplicas < after.getReplicationFactor().fullReplicas)
            clientWarnings.add("When increasing replication factor you need to run a full (-full) repair to distribute the data.");

        return clientWarnings;
    }

    private void validateNoRangeMovements()
    {
        if (allow_alter_rf_during_range_movement)
            return;

        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId = metadata.directory.peerId(FBUtilities.getBroadcastAddressAndPort());
        Set<InetAddressAndPort> notNormalEndpoints = metadata.directory.states.entrySet().stream().filter(e -> !e.getKey().equals(nodeId)).filter(e -> {
            switch (e.getValue())
            {
                case BOOTSTRAPPING:
                case LEAVING:
                case MOVING:
                    return true;
                default:
                    return false;
            }

        }).map(e -> metadata.directory.endpoint(e.getKey())).collect(Collectors.toSet());

        if (!notNormalEndpoints.isEmpty())
        {
            throw new ConfigurationException("Cannot alter RF while some endpoints are not in normal state (no range movements): " + notNormalEndpoints);
        }
    }

    private void validateTransientReplication(KeyspaceMetadata current, KeyspaceMetadata proposed)
    {
        //If there is no read traffic there are some extra alterations you can safely make, but this is so atypical
        //that a good default is to not allow unsafe changes
        if (allow_unsafe_transient_changes)
            return;

        ReplicationFactor oldRF = current.replicationStrategy.getReplicationFactor();
        ReplicationFactor newRF = proposed.replicationStrategy.getReplicationFactor();

        int oldTrans = oldRF.transientReplicas();
        int oldFull = oldRF.fullReplicas;
        int newTrans = newRF.transientReplicas();
        int newFull = newRF.fullReplicas;

        if (newTrans > 0)
        {
            if (DatabaseDescriptor.getNumTokens() > 1)
                throw new ConfigurationException(String.format("Transient replication is not supported with vnodes yet"));


            if (!current.views.isEmpty())
                throw new ConfigurationException("Cannot use transient replication on keyspaces using materialized views");
        }

        //This is true right now because the transition from transient -> full lacks the pending state
        //necessary for correctness. What would happen if we allowed this is that we would attempt
        //to read from a transient replica as if it were a full replica.
        if (oldFull > newFull && oldTrans > 0)
            throw new ConfigurationException("Can't add full replicas if there are any transient replicas. You must first remove all transient replicas, then change the # of full replicas, then add back the transient replicas");

        //Promoting a witness to a full replica leaves it a full replica holding no data for the range
        //it witnessed: tracked writes skip the column family store for tokens outside a node's full
        //ranges, see Keyspace#applyInternalTracked. The promoted replica is then counted by quorum
        //reads. Repair has to redistribute the data, so this is opt-in only.
        validateNoWitnessPromotion(current.replicationStrategy, proposed.replicationStrategy);

        //Don't increase transient replication factor by more than one at a time if changing number of replicas
        //Just like with changing full replicas it's not safe to do this as you could read from too many replicas
        //that don't have the necessary data. W/O transient replication this alteration was allowed and it's not clear
        //if it should be.
        //This is structured so you can convert as many full replicas to transient replicas as you want.
        boolean numReplicasChanged = oldTrans + oldFull != newTrans + newFull;
        if (numReplicasChanged && (newTrans > oldTrans && newTrans != oldTrans + 1))
            throw new ConfigurationException("Can only safely increase number of transients one at a time with incremental repair run in between each time");
    }

    /**
     * A witness is promoted when the transient count falls while the full count rises. This promotes witness replicas
     * to full replicas without sending them the data they need to participate in reads.
     */
    private void validateNoWitnessPromotion(AbstractReplicationStrategy current, AbstractReplicationStrategy proposed)
    {
        if (allowUnsafeWitnessPromotion)
            return;

        if (!current.getClass().equals(proposed.getClass()))
        {
            if (promotesWitness(current.getReplicationFactor(), proposed.getReplicationFactor()))
                throw witnessPromotionRejected();
            return;
        }

        Map<String, ReplicationFactor> before = replicationFactorsByGroup(current);
        Map<String, ReplicationFactor> after = replicationFactorsByGroup(proposed);
        for (String group : Sets.union(before.keySet(), after.keySet()))
        {
            if (promotesWitness(before.getOrDefault(group, ReplicationFactor.ZERO),
                                after.getOrDefault(group, ReplicationFactor.ZERO)))
                throw witnessPromotionRejected();
        }
    }

    private static boolean promotesWitness(ReplicationFactor before, ReplicationFactor after)
    {
        return after.transientReplicas() < before.transientReplicas() && after.fullReplicas > before.fullReplicas;
    }

    /**
     * Replication factors keyed by the unit the strategy assigns them to independently: datacenter for
     * {@link NetworkTopologyStrategy}, and the keyspace as a whole for every other strategy, whose
     * aggregate factor is already its only group.
     */
    private static Map<String, ReplicationFactor> replicationFactorsByGroup(AbstractReplicationStrategy strategy)
    {
        if (!(strategy instanceof NetworkTopologyStrategy))
            return Collections.singletonMap("", strategy.getReplicationFactor());

        NetworkTopologyStrategy nts = (NetworkTopologyStrategy) strategy;
        Map<String, ReplicationFactor> byDc = new HashMap<>();
        for (String dc : nts.getDatacenters())
            byDc.put(dc, nts.getReplicationFactor(dc));
        return byDc;
    }

    private ConfigurationException witnessPromotionRejected()
    {
        return new ConfigurationException(String.format("Cannot promote a transient replica of %s to a full replica: " +
                                                        "it holds no data for the range it witnessed. Set %s=true over " +
                                                        "JMX, or %s=true at startup, to allow it, then run a full repair " +
                                                        "to distribute the data.",
                                                        keyspaceName,
                                                        ALLOW_UNSAFE_WITNESS_PROMOTION.getKey(),
                                                        ALLOW_UNSAFE_TRANSIENT_CHANGES.getKey()));
    }

    private static volatile boolean allowUnsafeWitnessPromotion = ALLOW_UNSAFE_WITNESS_PROMOTION.getBoolean();

    public static void setAllowUnsafeWitnessPromotion(boolean allow)
    {
        allowUnsafeWitnessPromotion = allow;
    }

    public static boolean getAllowUnsafeWitnessPromotion()
    {
        return allowUnsafeWitnessPromotion;
    }

    /**
     * Alterations that would leave a witness (transient replica) counted by reads it cannot answer.
     * These are unsafe rather than inexpressible, so cassandra.allow_unsafe_transient_changes opts out
     * of them alongside the rules in validateTransientReplication.
     */
    private void validateWitnessTransitions(ClusterMetadata metadata,
                                            KeyspaceMetadata current,
                                            KeyspaceMetadata proposed)
    {
        if (allow_unsafe_transient_changes)
            return;

        boolean addsWitnesses = proposed.replicationStrategy.getReplicationFactor().hasTransientReplicas();

        // A migrating keyspace has pending ranges, and reads for a pending range take the untracked
        // path, see MigrationRouter#shouldUseTrackedForReads. Those reads would contact a transient
        // replica, which RangeCommandIterator#executeNormal rejects outright. Migrating a pending range
        // also relies on blocking read repair to converge the replicas, and a witness cannot take part.
        if (addsWitnesses && metadata.mutationTrackingMigrationState.isMigrating(keyspaceName))
            throw new ConfigurationException(String.format("Cannot add transient replicas to %s while its mutation " +
                                                           "tracking migration is in progress. Wait for the migration " +
                                                           "to complete, then alter the replication factor.",
                                                           keyspaceName));

        // AlterSchema#maybeUpdateMutationTrackingMigrationState starts the migration after this
        // statement validates, so the check above cannot see one this statement is about to start.
        if (addsWitnesses && proposed.params.replicationType.isTracked() && !current.params.replicationType.isTracked())
            throw new ConfigurationException(String.format("Cannot enable mutation tracking on %s and add transient " +
                                                           "replicas in the same statement, because doing so starts a " +
                                                           "migration. Set replication_type = 'tracked' first, wait for " +
                                                           "the migration to complete, then alter the replication factor.",
                                                           keyspaceName));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.ALTER_KEYSPACE, keyspaceName);
    }

    public String toString()
    {
        return String.format("%s (%s)", getClass().getSimpleName(), keyspaceName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final String keyspaceName;
        private final KeyspaceAttributes attrs;
        private final boolean ifExists;

        public Raw(String keyspaceName, KeyspaceAttributes attrs, boolean ifExists)
        {
            this.keyspaceName = keyspaceName;
            this.attrs = attrs;
            this.ifExists = ifExists;
        }

        public AlterKeyspaceStatement prepare(ClientState state)
        {
            return new AlterKeyspaceStatement(keyspaceName, attrs, ifExists);
        }
    }
}
