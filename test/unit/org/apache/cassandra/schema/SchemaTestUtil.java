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

package org.apache.cassandra.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;

public class SchemaTestUtil
{
    private final static Logger logger = LoggerFactory.getLogger(SchemaTestUtil.class);

    public static void announceNewKeyspace(KeyspaceMetadata ksm) throws ConfigurationException
    {
        announceNewKeyspace(ksm, false);
    }

    public static void announceNewKeyspace(KeyspaceMetadata ksm, boolean announceLocally) throws ConfigurationException
    {
        announceNewKeyspace(ksm, FBUtilities.timestampMicros(), announceLocally);
    }

    public static void announceNewKeyspace(KeyspaceMetadata ksm, long timestamp, boolean announceLocally) throws ConfigurationException
    {
        ksm.validate();

        if (Schema.instance.getKeyspaceMetadata(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info("Create new Keyspace: {}", ksm);
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm, timestamp), announceLocally);
    }

    public static void announceNewTable(TableMetadata cfm)
    {
        announceNewTable(cfm, true, FBUtilities.timestampMicros());
    }

    private static void announceNewTable(TableMetadata cfm, boolean throwOnDuplicate, long timestamp)
    {
        cfm.validate();

        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(cfm.keyspace);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.name, cfm.keyspace));
            // If we have a table or a view which has the same name, we can't add a new one
        else if (throwOnDuplicate && ksm.getTableOrViewNullable(cfm.name) != null)
            throw new AlreadyExistsException(cfm.keyspace, cfm.name);

        logger.info("Create new table: {}", cfm);
        announce(SchemaKeyspace.makeCreateTableMutation(ksm, cfm, timestamp), false);
    }

    static void announceKeyspaceUpdate(KeyspaceMetadata ksm)
    {
        ksm.validate();

        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info("Update Keyspace '{}' From {} To {}", ksm.name, oldKsm, ksm);
        announce(SchemaKeyspace.makeCreateKeyspaceMutation(ksm.name, ksm.params, FBUtilities.timestampMicros()), false);
    }

    public static void announceTableUpdate(TableMetadata tm)
    {
        announceTableUpdate(tm, false);
    }

    public static void announceTableUpdate(TableMetadata updated, boolean announceLocally)
    {
        updated.validate();

        TableMetadata current = Schema.instance.getTableMetadata(updated.keyspace, updated.name);
        if (current == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", updated.name, updated.keyspace));
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(current.keyspace);

        updated.validateCompatibility(current);

        long timestamp = FBUtilities.timestampMicros();

        logger.info("Update table '{}/{}' From {} To {}", current.keyspace, current.name, current, updated);
        Mutation.SimpleBuilder builder = SchemaKeyspace.makeUpdateTableMutation(ksm, current, updated, timestamp);

        announce(builder, announceLocally);
    }

    static void announceKeyspaceDrop(String ksName)
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info("Drop Keyspace '{}'", oldKsm.name);
        announce(SchemaKeyspace.makeDropKeyspaceMutation(oldKsm, FBUtilities.timestampMicros()), false);
    }

    public static void announceTableDrop(String ksName, String cfName, boolean announceLocally)
    {
        TableMetadata tm = Schema.instance.getTableMetadata(ksName, cfName);
        if (tm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName));
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(ksName);

        logger.info("Drop table '{}/{}'", tm.keyspace, tm.name);
        announce(SchemaKeyspace.makeDropTableMutation(ksm, tm, FBUtilities.timestampMicros()), announceLocally);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    static void announce(Mutation.SimpleBuilder schema, boolean announceLocally)
    {
        List<Mutation> mutations = Collections.singletonList(schema.build());

        if (announceLocally)
            Schema.instance.merge(mutations);
        else
            announce(mutations);
    }

    public static void announce(Collection<Mutation> schema)
    {
        Future<?> f = MigrationCoordinator.instance.announceWithoutPush(schema);

        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        Message<Collection<Mutation>> message = Message.out(SCHEMA_PUSH_REQ, schema);
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {
            if (MigrationCoordinator.instance.shouldPushSchemaTo(endpoint))
            {
                MessagingService.instance().send(message, endpoint);
                schemaDestinationEndpoints.add(endpoint);
            }
            else
            {
                schemaEndpointsIgnored.add(endpoint);
            }
        }

        SchemaAnnouncementDiagnostics.schemaMutationsAnnounced(schemaDestinationEndpoints, schemaEndpointsIgnored);
        FBUtilities.waitOnFuture(f);
    }
}
