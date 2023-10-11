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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SchemaTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
        Schema.instance.loadFromDisk();
    }

    @Test
    public void testTransKsMigration() throws IOException
    {
        assertEquals(0, Schema.instance.distributedKeyspaces().size());

        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
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
    public void testKeyspaceCreationWhenNotInitialized() {
        Keyspace.unsetInitialized();
        try
        {
            SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create("test", KeyspaceParams.simple(1)), true);
            assertNotNull(Schema.instance.getKeyspaceMetadata("test"));
            assertNull(Schema.instance.getKeyspaceInstance("test"));

            SchemaTestUtil.dropKeyspaceIfExist("test", true);
            assertNull(Schema.instance.getKeyspaceMetadata("test"));
            assertNull(Schema.instance.getKeyspaceInstance("test"));
        }
        finally
        {
            Keyspace.setInitialized();
        }

        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create("test", KeyspaceParams.simple(1)), true);
        assertNotNull(Schema.instance.getKeyspaceMetadata("test"));
        assertNotNull(Schema.instance.getKeyspaceInstance("test"));

        SchemaTestUtil.dropKeyspaceIfExist("test", true);
        assertNull(Schema.instance.getKeyspaceMetadata("test"));
        assertNull(Schema.instance.getKeyspaceInstance("test"));
    }

    private void saveKeyspaces()
    {
        Collection<Mutation> mutations = Arrays.asList(SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create("ks0", KeyspaceParams.simple(3)), FBUtilities.timestampMicros()).build(),
                                                       SchemaKeyspace.makeCreateKeyspaceMutation(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(3)), FBUtilities.timestampMicros()).build());
        SchemaKeyspace.applyChanges(mutations);
    }
}
