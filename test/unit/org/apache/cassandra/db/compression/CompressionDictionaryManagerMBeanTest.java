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

package org.apache.cassandra.db.compression;

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MBeanWrapper.OnException;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionDictionaryManagerMBeanTest
{
    private static final String KEYSPACE_WITH_DICT = "keyspace_mbean_test";
    private static final String TABLE = "test_table";

    private static ColumnFamilyStore cfsWithDict;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        ServerTestUtils.prepareServer();
        CompressionParams compressionParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                     Map.of("compression_level", "3"));
        TableMetadata.Builder tableBuilder = TableMetadata.builder(KEYSPACE_WITH_DICT, TABLE)
                                                          .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                          .addRegularColumn("data", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                          .compression(compressionParams);
        SchemaLoader.createKeyspace(KEYSPACE_WITH_DICT,
                                    KeyspaceParams.simple(1),
                                    tableBuilder);
        cfsWithDict = Keyspace.open(KEYSPACE_WITH_DICT).getColumnFamilyStore(TABLE);
    }

    // Ensure no mbean is registered at the begining of the test
    @Before
    public void cleanup()
    {
        String mbeanName = CompressionDictionaryManager.mbeanName(KEYSPACE_WITH_DICT, TABLE);
        MBeanWrapper.instance.unregisterMBean(mbeanName, OnException.IGNORE);
    }

    @Test
    public void testMBeanRegisteredWhenBookkeepingEnabled()
    {
        String mbeanName = CompressionDictionaryManager.mbeanName(KEYSPACE_WITH_DICT, TABLE);
        // Create manager with bookkeeping enabled
        try (CompressionDictionaryManager manager = new CompressionDictionaryManager(cfsWithDict, true))
        {
            // Verify MBean is registered
            assertThat(MBeanWrapper.instance.isRegistered(mbeanName))
            .as("MBean should be registered when bookkeeping is enabled")
            .isTrue();
        }
        // Closing manager should unregister the mbean; Verify it is unregistered
        assertThat(MBeanWrapper.instance.isRegistered(mbeanName))
        .as("MBean should be unregistered after unregisterMbean() call")
        .isFalse();
    }

    @Test
    public void testMBeanNotRegisteredWhenBookkeepingDisabled()
    {
        // Create manager with bookkeeping disabled
        try (CompressionDictionaryManager manager = new CompressionDictionaryManager(cfsWithDict, false))
        {
            // Verify MBean is NOT registered
            String mbeanName = CompressionDictionaryManager.mbeanName(KEYSPACE_WITH_DICT, TABLE);;
            assertThat(MBeanWrapper.instance.isRegistered(mbeanName))
            .as("MBean should not be registered when bookkeeping is disabled")
            .isFalse();
        }
        // Closing manager should not throw due to mbean not registered
    }

    @Test
    public void testMBeanUnregisteredOnCFSInvalidation()
    {
        String testKeyspace = "test_invalidation_mbean_ks";
        String testTable = "test_invalidation_mbean_table";

        CompressionParams compressionParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                                     Map.of("compression_level", "3"));

        TableMetadata.Builder tableBuilder = TableMetadata.builder(testKeyspace, testTable)
                                                          .addPartitionKeyColumn("pk", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                          .addRegularColumn("data", org.apache.cassandra.db.marshal.UTF8Type.instance)
                                                          .compression(compressionParams);

        SchemaLoader.createKeyspace(testKeyspace,
                                    KeyspaceParams.simple(1),
                                    tableBuilder);

        ColumnFamilyStore cfs = Keyspace.open(testKeyspace).getColumnFamilyStore(testTable);

        String mbeanName = CompressionDictionaryManager.mbeanName(testKeyspace, testTable);

        // Verify MBean is registered (CFS registers it during creation)
        assertThat(MBeanWrapper.instance.isRegistered(mbeanName))
        .as("MBean should be registered after CFS creation")
        .isTrue();

        // Invalidate the CFS (which should unregister the MBean)
        cfs.invalidate(true, true);

        // Verify MBean is unregistered
        assertThat(MBeanWrapper.instance.isRegistered(mbeanName))
        .as("MBean should be unregistered after CFS invalidation")
        .isFalse();
    }
}
