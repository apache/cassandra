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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.utils.FBUtilities;

public final class AlterKeyspaceStatement extends AlterSchemaStatement
{
    private static final boolean allow_alter_rf_during_range_movement = Boolean.getBoolean(Config.PROPERTY_PREFIX + "allow_alter_rf_during_range_movement");
    private static final boolean allow_unsafe_transient_changes = Boolean.getBoolean(Config.PROPERTY_PREFIX + "allow_unsafe_transient_changes");
    private final HashSet<String> clientWarnings = new HashSet<>();

    private final KeyspaceAttributes attrs;

    public AlterKeyspaceStatement(String keyspaceName, KeyspaceAttributes attrs)
    {
        super(keyspaceName);
        this.attrs = attrs;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        attrs.validate();

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        KeyspaceMetadata newKeyspace = keyspace.withSwapped(attrs.asAlteredKeyspaceParams(keyspace.params));

        if (newKeyspace.params.replication.klass.equals(LocalStrategy.class))
            throw ire("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        newKeyspace.params.validate(keyspaceName);

        validateNoRangeMovements();
        validateTransientReplication(keyspace.createReplicationStrategy(), newKeyspace.createReplicationStrategy());

        Keyspaces res = schema.withAddedOrUpdated(newKeyspace);

        return res;
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
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        if (diff.isEmpty())
            return clientWarnings;

        KeyspaceDiff keyspaceDiff = diff.altered.get(0);

        AbstractReplicationStrategy before = keyspaceDiff.before.createReplicationStrategy();
        AbstractReplicationStrategy after = keyspaceDiff.after.createReplicationStrategy();

        if (before.getReplicationFactor().fullReplicas < after.getReplicationFactor().fullReplicas)
            clientWarnings.add("When increasing replication factor you need to run a full (-full) repair to distribute the data.");

        return clientWarnings;
    }

    private void validateNoRangeMovements()
    {
        if (allow_alter_rf_during_range_movement)
            return;

        Stream<InetAddressAndPort> unreachableNotAdministrativelyInactive =
            Gossiper.instance.getUnreachableMembers().stream().filter(endpoint -> !FBUtilities.getBroadcastAddressAndPort().equals(endpoint) &&
                                                                      !Gossiper.instance.isAdministrativelyInactiveState(endpoint));
        Stream<InetAddressAndPort> endpoints = Stream.concat(Gossiper.instance.getLiveMembers().stream(),
                                                             unreachableNotAdministrativelyInactive);
        List<InetAddressAndPort> notNormalEndpoints = endpoints.filter(endpoint -> !FBUtilities.getBroadcastAddressAndPort().equals(endpoint) &&
                                                                                   !Gossiper.instance.getEndpointStateForEndpoint(endpoint).isNormalState())
                                                               .collect(Collectors.toList());
        if (!notNormalEndpoints.isEmpty())
        {
            throw new ConfigurationException("Cannot alter RF while some endpoints are not in normal state (no range movements): " + notNormalEndpoints);
        }
    }

    private void validateTransientReplication(AbstractReplicationStrategy oldStrategy, AbstractReplicationStrategy newStrategy)
    {
        //If there is no read traffic there are some extra alterations you can safely make, but this is so atypical
        //that a good default is to not allow unsafe changes
        if (allow_unsafe_transient_changes)
            return;

        ReplicationFactor oldRF = oldStrategy.getReplicationFactor();
        ReplicationFactor newRF = newStrategy.getReplicationFactor();

        int oldTrans = oldRF.transientReplicas();
        int oldFull = oldRF.fullReplicas;
        int newTrans = newRF.transientReplicas();
        int newFull = newRF.fullReplicas;

        if (newTrans > 0)
        {
            if (DatabaseDescriptor.getNumTokens() > 1)
                throw new ConfigurationException(String.format("Transient replication is not supported with vnodes yet"));

            Keyspace ks = Keyspace.open(keyspaceName);
            for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
            {
                if (cfs.viewManager.hasViews())
                {
                    throw new ConfigurationException("Cannot use transient replication on keyspaces using materialized views");
                }

                if (cfs.indexManager.hasIndexes())
                {
                    throw new ConfigurationException("Cannot use transient replication on keyspaces using secondary indexes");
                }
            }
        }

        //This is true right now because the transition from transient -> full lacks the pending state
        //necessary for correctness. What would happen if we allowed this is that we would attempt
        //to read from a transient replica as if it were a full replica.
        if (oldFull > newFull && oldTrans > 0)
            throw new ConfigurationException("Can't add full replicas if there are any transient replicas. You must first remove all transient replicas, then change the # of full replicas, then add back the transient replicas");

        //Don't increase transient replication factor by more than one at a time if changing number of replicas
        //Just like with changing full replicas it's not safe to do this as you could read from too many replicas
        //that don't have the necessary data. W/O transient replication this alteration was allowed and it's not clear
        //if it should be.
        //This is structured so you can convert as many full replicas to transient replicas as you want.
        boolean numReplicasChanged = oldTrans + oldFull != newTrans + newFull;
        if (numReplicasChanged && (newTrans > oldTrans && newTrans != oldTrans + 1))
            throw new ConfigurationException("Can only safely increase number of transients one at a time with incremental repair run in between each time");
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

        public Raw(String keyspaceName, KeyspaceAttributes attrs)
        {
            this.keyspaceName = keyspaceName;
            this.attrs = attrs;
        }

        public AlterKeyspaceStatement prepare(ClientState state)
        {
            return new AlterKeyspaceStatement(keyspaceName, attrs);
        }
    }
}
