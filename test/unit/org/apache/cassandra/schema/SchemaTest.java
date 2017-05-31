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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.Gossiper;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        SchemaLoader.cleanupAndLeaveDirs();
        Schema.instance.loadFromDisk();
        assertEquals(0, Schema.instance.getNonSystemKeyspaces().size());

        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));
        Keyspace.setInitialized();

        try
        {
            // add a few.
            MigrationManager.announceNewKeyspace(KeyspaceMetadata.create("ks0", KeyspaceParams.simple(3)));
            MigrationManager.announceNewKeyspace(KeyspaceMetadata.create("ks1", KeyspaceParams.simple(3)));

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));

            Schema.instance.unload(Schema.instance.getKeyspaceMetadata("ks0"));
            Schema.instance.unload(Schema.instance.getKeyspaceMetadata("ks1"));

            assertNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNull(Schema.instance.getKeyspaceMetadata("ks1"));

            Schema.instance.loadFromDisk();

            assertNotNull(Schema.instance.getKeyspaceMetadata("ks0"));
            assertNotNull(Schema.instance.getKeyspaceMetadata("ks1"));
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

}
