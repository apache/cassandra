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

package org.apache.cassandra.service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.service.CassandraDaemon.SPECULATION_THRESHOLD_UPDATER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class OptionalTasksTest
{
    private static final String KEYSPACE = "OpitonalTasksTest";
    private static final String TABLE = "SpeculationThreshold";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), standardCFMD(KEYSPACE, TABLE));
    }

    @Test
    public void shouldIgnoreDroppedKeyspace()
    {
        // Set the initial sampling state...
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Objects.requireNonNull(metadata).id);
        Objects.requireNonNull(cfs).metric.coordinatorReadLatency.update(100, TimeUnit.NANOSECONDS);

        // Remove the Keyspace name to make it invisible to the updater...
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        SchemaTestUtil.dropKeyspaceIfExist(KEYSPACE, true);

        try
        {
            long originalValue = cfs.sampleReadLatencyMicros;

            // ...and ensure that the speculation threshold updater doesn't run.
            SPECULATION_THRESHOLD_UPDATER.run();

            assertEquals(originalValue, cfs.sampleReadLatencyMicros);
        }
        finally
        {
            // Restore the removed Keyspace to put things back the way we found them.
            SchemaTestUtil.addOrUpdateKeyspace(ksm, true);
        }
    }

    @Test
    public void shouldUpdateSpeculationThreshold()
    {
        // Set the initial sampling state...
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(Objects.requireNonNull(metadata).id);
        Objects.requireNonNull(cfs).metric.coordinatorReadLatency.update(100, TimeUnit.NANOSECONDS);

        long originalValue = cfs.sampleReadLatencyMicros;

        // ...and ensure that the speculation threshold updater runs.
        SPECULATION_THRESHOLD_UPDATER.run();

        assertNotEquals(originalValue, cfs.sampleReadLatencyMicros);
    }
}
