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

import java.util.Collections;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.ClusterMetadata;

public class SchemaTestUtil
{
    private final static Logger logger = LoggerFactory.getLogger(SchemaTestUtil.class);

    public static void announceNewKeyspace(KeyspaceMetadata ksm) throws ConfigurationException
    {
        ksm.validate(ClusterMetadata.current());

        if (Schema.instance.getKeyspaceMetadata(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info("Create new Keyspace: {}", ksm);
        submit((metadata) -> metadata.schema.getKeyspaces().withAddedOrUpdated(ksm));
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
        submit((metadata) -> metadata.schema.getKeyspaces().withAddedOrUpdated(ksm.withSwapped(ksm.tables.with(cfm))));
    }

    static void announceKeyspaceUpdate(KeyspaceMetadata ksm)
    {
        ksm.validate(ClusterMetadata.current());

        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info("Update Keyspace '{}' From {} To {}", ksm.name, oldKsm, ksm);
        submit((metadata) -> metadata.schema.getKeyspaces().withAddedOrUpdated(ksm));
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
        submit((metadata) -> metadata.schema.getKeyspaces().withAddedOrUpdated(ksm.withSwapped(ksm.tables.withSwapped(updated))));
    }

    static void announceKeyspaceDrop(String ksName)
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info("Drop Keyspace '{}'", oldKsm.name);
        submit((metadata) -> metadata.schema.getKeyspaces().without(ksName));
    }

    public static void announceTableDrop(String ksName, String cfName)
    {
        logger.info("Drop table '{}/{}'", ksName, cfName);
        submit((metadata) -> {
            Keyspaces schema = metadata.schema.getKeyspaces();
            KeyspaceMetadata ksm = schema.getNullable(ksName);
            TableMetadata tm = ksm != null ? ksm.getTableOrViewNullable(cfName) : null;
            if (tm == null)
                throw new ConfigurationException(String.format("Cannot drop non existing table '%s' in keyspace '%s'.", cfName, ksName));

            return schema.withAddedOrUpdated(ksm.withSwapped(ksm.tables.without(cfName)));
        });
    }

    public static void addOrUpdateKeyspace(KeyspaceMetadata ksm)
    {
        submit((metadata) -> metadata.schema.getKeyspaces().withAddedOrUpdated(ksm));
    }

    @Deprecated(since = "CEP-21") // TODO remove this
    public static void addOrUpdateKeyspace(KeyspaceMetadata ksm, boolean locally)
    {
        submit((metadata) -> metadata.schema.getKeyspaces().withAddedOrUpdated(ksm));
    }

    public static void dropKeyspaceIfExist(String ksName, boolean locally)
    {
        submit((metadata) -> metadata.schema.getKeyspaces().without(Collections.singletonList(ksName)));
    }

    public static void submit(Function<ClusterMetadata, Keyspaces> transformation)
    {
        Schema.instance.submit(toTransformation(transformation));
    }

    /**
     * Returns a {@link SchemaTransformation} with an {@code compatibleWith} implementation hardcoded to return true.
     *
     * This is intended for use in unit tests where there is only a single Cassandra version to consider, i.e. the
     * transformation can be assumed compatible with whatever the current {@link ClusterMetadata} state is. It isn't
     * suitable for dtests/upgrade tests etc. It is also worth noting that the {@code cql} method of the returned
     * {@code SchemaTransformation} does not produce valid CQL, which procludes these transformations from being
     * serialized/deserialized.
     *
     * @param transformation function to apply in order to modify schema
     * @return SchemaTransformation instance that wraps the supplied function
     */
    public static SchemaTransformation toTransformation(Function<ClusterMetadata, Keyspaces> transformation)
    {
       return new SchemaTransformation()
       {
           @Override
           public Keyspaces apply(ClusterMetadata metadata)
           {
               return transformation.apply(metadata);
           }

           @Override
           public boolean compatibleWith(ClusterMetadata metadata)
           {
               return true;
           }

           @Override
           public String cql()
           {
               return "fake";
           }
       };
    }
}
