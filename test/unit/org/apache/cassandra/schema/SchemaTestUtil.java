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
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;

public class SchemaTestUtil
{
    private final static Logger logger = LoggerFactory.getLogger(SchemaTestUtil.class);

    public static void announceNewKeyspace(KeyspaceMetadata ksm) throws ConfigurationException
    {
        ksm.validate();

        if (Schema.instance.getKeyspaceMetadata(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info("Create new Keyspace: {}", ksm);
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(ksm));
    }

    public static void announceNewTable(TableMetadata cfm)
    {
        announceNewTable(cfm, true);
    }

    private static void announceNewTable(TableMetadata cfm, boolean throwOnDuplicate)
    {
        cfm.validate();

        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(cfm.keyspace);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add table '%s' to non existing keyspace '%s'.", cfm.name, cfm.keyspace));
            // If we have a table or a view which has the same name, we can't add a new one
        else if (throwOnDuplicate && ksm.getTableOrViewNullable(cfm.name) != null)
            throw new AlreadyExistsException(cfm.keyspace, cfm.name);

        logger.info("Create new table: {}", cfm);
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(ksm.withSwapped(ksm.tables.with(cfm))));
    }

    static void announceKeyspaceUpdate(KeyspaceMetadata ksm)
    {
        ksm.validate();

        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info("Update Keyspace '{}' From {} To {}", ksm.name, oldKsm, ksm);
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(ksm));
    }

    public static void announceTableUpdate(TableMetadata updated)
    {
        updated.validate();

        TableMetadata current = Schema.instance.getTableMetadata(updated.keyspace, updated.name);
        if (current == null)
            throw new ConfigurationException(String.format("Cannot update non existing table '%s' in keyspace '%s'.", updated.name, updated.keyspace));
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(current.keyspace);

        updated.validateCompatibility(current);

        logger.info("Update table '{}/{}' From {} To {}", current.keyspace, current.name, current, updated);
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(ksm.withSwapped(ksm.tables.withSwapped(updated))));
    }

    static void announceKeyspaceDrop(String ksName)
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info("Drop Keyspace '{}'", oldKsm.name);
        Schema.instance.transform(schema -> schema.without(ksName));
    }

    public static SchemaTransformation dropTable(String ksName, String cfName)
    {
        return schema -> {
            KeyspaceMetadata ksm = schema.getNullable(ksName);
            TableMetadata tm = ksm != null ? ksm.getTableOrViewNullable(cfName) : null;
            if (tm == null)
                throw new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName));

            return schema.withAddedOrUpdated(ksm.withSwapped(ksm.tables.without(cfName)));
        };
    }

    public static void announceTableDrop(String ksName, String cfName)
    {
        logger.info("Drop table '{}/{}'", ksName, cfName);
        Schema.instance.transform(dropTable(ksName, cfName));
    }

    public static void addOrUpdateKeyspace(KeyspaceMetadata ksm, boolean locally)
    {
        Schema.instance.transform(current -> current.withAddedOrUpdated(ksm));
    }

    public static void dropKeyspaceIfExist(String ksName, boolean locally)
    {
        Schema.instance.transform(current -> current.without(Collections.singletonList(ksName)));
    }

    public static void mergeAndAnnounceLocally(Collection<Mutation> schemaMutations)
    {
        SchemaPushVerbHandler.instance.doVerb(Message.out(SCHEMA_PUSH_REQ, schemaMutations));
        Future<?> f = Stage.MIGRATION.submit(() -> {});
        Assert.assertTrue(f.awaitThrowUncheckedOnInterrupt(10, TimeUnit.SECONDS));
        f.rethrowIfFailed();
    }

}
