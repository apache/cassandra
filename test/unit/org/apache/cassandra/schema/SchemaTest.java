/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class SchemaTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testTransKsMigration() throws IOException
    {
        CommitLog.instance.start();
        SchemaLoader.cleanupAndLeaveDirs();
        Schema.instance.loadFromDisk();
        assertEquals(0, Schema.instance.getNonSystemKeyspaces().size());

        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
        Keyspace.setInitialized();

        try
        {
            // add a few.
            saveKeyspaces();
            Schema.instance.reloadSchemaAndAnnounceVersion();

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));

            Schema.instance.transform(keyspaces -> keyspaces.without(Arrays.asList("ks0", "ks1")));

            assertNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNull(Schema.instance.getKeyspaceMetadata("ks1"));

            saveKeyspaces();
            Schema.instance.reloadSchemaAndAnnounceVersion();

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

    @Test
    public void testSchemaManagerMocking()
    {
        Keyspace.unsetInitialized();

        SchemaUpdateHandler updateHandler = mock(SchemaUpdateHandler.class);
        Schema schemaManager = new Schema(false, Keyspaces.of(SchemaKeyspace.metadata(), SystemKeyspace.metadata()), updateHandler);
        assertThat(schemaManager.getKeyspaceMetadata("ks")).isNull();

        KeyspaceMetadata newKs = KeyspaceMetadata.create("ks", KeyspaceParams.simple(1));
        DistributedSchema before = new DistributedSchema(schemaManager.distributedKeyspaces(), schemaManager.getVersion());
        DistributedSchema after = new DistributedSchema(schemaManager.distributedKeyspaces().withAddedOrUpdated(newKs), UUID.randomUUID());
        KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), after.getKeyspaces());
        SchemaTransformationResult transformation = new SchemaTransformationResult(before, after, diff);
        schemaManager.mergeAndUpdateVersion(transformation, true);

        assertThat(schemaManager.getKeyspaceMetadata("ks")).isEqualTo(newKs);
        assertThat(schemaManager.getKeyspaceInstance("ks")).isNull(); // means that we didn't open the keyspace, which is expected since Keyspace is uninitialized
    }

    private void saveKeyspaces()
    {
        Collection<Mutation> mutations = Arrays.asList(SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create("ks0", KeyspaceParams.simple(3)), FBUtilities.timestampMicros()).build(),
                                                       SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(3)), FBUtilities.timestampMicros()).build());
        SchemaKeyspace.applyChanges(mutations);
    }
}
