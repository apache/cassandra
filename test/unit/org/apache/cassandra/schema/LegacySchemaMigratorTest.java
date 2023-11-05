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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.*;

import static java.lang.String.format;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;

@SuppressWarnings("deprecation")
public class LegacySchemaMigratorTest
{
    /*
     * 1. Write a variety of different keyspaces/tables/types/function in the legacy manner, using legacy schema tables
     * 2. Run the migrator
     * 3. Read all the keyspaces from the new schema tables
     * 4. Make sure that we've read *exactly* the same set of keyspaces/tables/types/functions
     * 5. Validate that the legacy schema tables are now empty
     */
    @Test
    public void testMigrate() throws IOException
    {
        CQLTester.cleanupAndLeaveDirs();

        Keyspaces expected = LegacySchemaMigratorBaseTest.keyspacesToMigrate();

        // write the keyspaces into the legacy tables
        expected.forEach(LegacySchemaMigratorBaseTest::legacySerializeKeyspace);

        // run the migration
        LegacySchemaMigrator.migrate();

        // read back all the metadata from the new schema tables
        Keyspaces actual = SchemaKeyspace.fetchNonSystemKeyspaces();

        // need to load back CFMetaData of those tables (CFS instances will still be loaded)
        LegacySchemaMigratorBaseTest.loadLegacySchemaTables();

        // verify that nothing's left in the old schema tables
        for (CFMetaData table : LegacySchemaMigrator.LegacySchemaTables)
        {
            String query = format("SELECT * FROM %s.%s", SystemKeyspace.NAME, table.cfName);
            //noinspection ConstantConditions
            assertTrue(executeOnceInternal(query).isEmpty());
        }

        // make sure that we've read *exactly* the same set of keyspaces/tables/types/functions
        assertEquals(expected, actual);

        // check that the build status of all indexes has been updated to use the new
        // format of index name: the index_name column of system.IndexInfo used to
        // contain table_name.index_name. Now it should contain just the index_name.
        expected.forEach(LegacySchemaMigratorBaseTest::verifyIndexBuildStatus);
    }

    @Test
    public void testMigrateLegacyCachingOptions() throws IOException
    {
        CQLTester.cleanupAndLeaveDirs();

        assertEquals(CachingParams.CACHE_EVERYTHING, LegacySchemaMigrator.cachingFromRow("ALL"));
        assertEquals(CachingParams.CACHE_NOTHING, LegacySchemaMigrator.cachingFromRow("NONE"));
        assertEquals(CachingParams.CACHE_KEYS, LegacySchemaMigrator.cachingFromRow("KEYS_ONLY"));
        assertEquals(new CachingParams(false, Integer.MAX_VALUE), LegacySchemaMigrator.cachingFromRow("ROWS_ONLY"));
        assertEquals(CachingParams.CACHE_KEYS, LegacySchemaMigrator.cachingFromRow("{\"keys\" : \"ALL\", \"rows_per_partition\" : \"NONE\"}" ));
        assertEquals(new CachingParams(false, Integer.MAX_VALUE), LegacySchemaMigrator.cachingFromRow("{\"keys\" : \"NONE\", \"rows_per_partition\" : \"ALL\"}" ));
        assertEquals(new CachingParams(true, 100), LegacySchemaMigrator.cachingFromRow("{\"keys\" : \"ALL\", \"rows_per_partition\" : \"100\"}" ));

        try
        {
            LegacySchemaMigrator.cachingFromRow("EXCEPTION");
            Assert.fail();
        }
        catch(RuntimeException e)
        {
            // Expected passing path
            assertTrue(true);
        }
    }

}
