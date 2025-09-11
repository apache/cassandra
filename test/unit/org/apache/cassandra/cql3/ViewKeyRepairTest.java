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

package org.apache.cassandra.cql3;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertNotNull;

public class ViewKeyRepairTest extends CQLTester
{
    public String keyspace = "testks";
    public String table = "testtbl";
    public String view = "testview";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        daemonInitialization();
        requireNetwork();
    }

    @Before
    public void init() throws Throwable
    {
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace));
        schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s.%s (a int, b int, c int, PRIMARY KEY (a, b))", keyspace, table));
        schemaChange(String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT * FROM %s.%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)",
                                 keyspace, view, keyspace, table));
    }

    @After
    public void reset() throws Throwable
    {
        executeNet(String.format("TRUNCATE %s.%s", keyspace, table));
        DatabaseDescriptor.setDirectMaterializedViewModification(false);
        DatabaseDescriptor.setRebuildKeyOnMaterializedViewModificationEnabled(false);
    }

    @Test
    public void testRebuildKeyOnMVModificationDisabled() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        executeNet(String.format("INSERT INTO %s.%s (a, b, c) VALUES (1, 1, 1)", keyspace, view));
        // normal path
        assertEquals(1, execute(String.format("select * from %s.%s", keyspace, view)).size());
    }

    @Test
    public void testRebuildKeyOnMVModificationEnabled() throws Throwable
    {
        DatabaseDescriptor.setDirectMaterializedViewModification(true);
        DatabaseDescriptor.setRebuildKeyOnMaterializedViewModificationEnabled(true);
        try
        {
            executeNet(String.format("INSERT INTO %s.%s (a, b, c) VALUES (1, 1, 1)", keyspace, view));
            fail("expected exception not thrown");
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("rebuildMVKey to be implemented"));
        }
        assertEquals(0, execute(String.format("select * from %s.%s", keyspace, view)).size());
        TableMetadata viewMetadata = Schema.instance.getTableMetadata(keyspace, view);
        assertNotNull(viewMetadata);
        ColumnFamilyStore viewCfs = Schema.instance.getColumnFamilyStoreInstance(viewMetadata.id);
        assertNotNull(viewCfs.metric.viewRebuildKeyTime);
        assertEquals(1, viewCfs.metric.viewRebuildKeyTime.cf.getCount());
    }
}
