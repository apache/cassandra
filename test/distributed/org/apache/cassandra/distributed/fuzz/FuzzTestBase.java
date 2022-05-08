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

package org.apache.cassandra.distributed.fuzz;

import java.util.Collection;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.clock.OffsetClock;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public abstract class FuzzTestBase extends TestBaseImpl
{
    protected static Configuration configuration;
    public static final int RF = 2;
    static
    {
        try
        {
            HarryHelper.init();
            configuration = HarryHelper.defaultConfiguration().build();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static Cluster cluster;

    @BeforeClass
    public static void beforeClassOverride() throws Throwable
    {
        cluster = Cluster.build(2)
                         .start();

        init(cluster, RF);
    }

    @AfterClass
    public static void afterClass()
    {
        if (cluster != null)
            cluster.close();
    }

    public void reset()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster, RF);
    }

    /**
     * Helped method to generate a {@code number} of sstables for the given {@code schemaSpec}.
     */
    @SuppressWarnings("unused")
    public static void generateTables(SchemaSpec schemaSpec, int number)
    {
        Run run = configuration.unbuild()
                               .setSeed(1)
                               .setSchemaProvider(new FixedSchemaProviderConfiguration(schemaSpec))
                               .setClock(() -> new OffsetClock(10000L))
                               .setDropSchema(false)
                               .setCreateSchema(false)
                               .build()
                               .createRun();

        ColumnFamilyStore store = Keyspace.open(schemaSpec.keyspace).getColumnFamilyStore(schemaSpec.table);
        store.disableAutoCompaction();

        SSTableGenerator gen = new SSTableGenerator(run, store);
        SSTableGenerator largePartitionGen = new SSTableWithLargePartition(run, store);
        for (int i = 0; i < number; i++)
        {
            if (i % 3 == 0)
                largePartitionGen.gen(1_000);
            else
                gen.gen(1_000);
        }
    }

    /**
     * Helper class to force generation of a fixed partition size.
     */
    private static class SSTableWithLargePartition extends SSTableGenerator
    {
        public SSTableWithLargePartition(Run run, ColumnFamilyStore store)
        {
            super(run, store);
        }

        @Override
        public Collection<SSTableReader> gen(int rows)
        {
            long lts = 0;
            for (int i = 0; i < rows; i++)
            {
                long current = lts++;
                write(current, current, current, current, true).applyUnsafe();
                if (schema.staticColumns != null)
                    writeStatic(current, 0, current, current, true).applyUnsafe();
            }
            Util.flush(store);
            return null;
        }
    }
}
