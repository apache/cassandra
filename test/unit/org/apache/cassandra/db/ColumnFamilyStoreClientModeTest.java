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

package org.apache.cassandra.db;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.cql3.CQLTester.KEYSPACE;
import static org.apache.cassandra.cql3.QueryProcessor.parseStatement;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link ColumnFamilyStore} when the library is running as tool
 * or client mode.
 */
public class ColumnFamilyStoreClientModeTest
{
    public static final String TABLE = "test1";

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.clientInitialization();
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        DatabaseDescriptor.setEndpointSnitch(new SimpleSnitch());
        DatabaseDescriptor.getRawConfig().memtable_flush_writers = 1;
        DatabaseDescriptor.getRawConfig().local_system_data_file_directory = tempFolder.toString();
        DatabaseDescriptor.getRawConfig().partitioner = "Murmur3Partitioner";
        DatabaseDescriptor.setLocalDataCenter("DC1");
        DatabaseDescriptor.applyPartitioner();
    }

    @Test
    public void testTopPartitionsAreNotInitialized() throws IOException
    {
        CreateTableStatement.Raw schemaStatement = parseStatement("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (a int, b text, PRIMARY KEY (a))", CreateTableStatement.Raw.class, "CREATE TABLE");
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1));
        DistributedSchema initialSchema = new DistributedSchema(Keyspaces.of(ksm));
        ClusterMetadataService.initializeForClients(initialSchema);
        ClusterMetadata.current().schema.initializeKeyspaceInstances(DistributedSchema.empty(), false);

        Types types = Types.rawBuilder(KEYSPACE).build();
        ClientState state = ClientState.forInternalCalls(KEYSPACE);
        CreateTableStatement statement = schemaStatement.prepare(state);
        statement.validate(state);
        TableMetadata tableMetadata = statement.builder(types, UserFunctions.none())
                                               .id(TableId.fromUUID(UUID.nameUUIDFromBytes(ArrayUtils.addAll(schemaStatement.keyspace().getBytes(), schemaStatement.table().getBytes()))))
                                               .partitioner(Murmur3Partitioner.instance)
                                               .build();
        Keyspace.setInitialized();
        Directories directories = new Directories(tableMetadata, new Directories.DataDirectory[]{ new Directories.DataDirectory(new org.apache.cassandra.io.util.File(tempFolder.newFolder("datadir"))) });
        Keyspace ks = Keyspace.openWithoutSSTables(KEYSPACE);
        ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(ks, TABLE, TableMetadataRef.forOfflineTools(tableMetadata).get(), directories, false, false);

        assertNull(cfs.topPartitions);
    }
}
