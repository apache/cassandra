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

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MigrationManagerDropKSTest
{
    private static final String KEYSPACE1 = "keyspace1";
    private static final String TABLE1 = "standard1";
    private static final String TABLE2 = "standard2";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, TABLE2));
    }

    @Test
    public void dropKS() throws ConfigurationException
    {
        // sanity
        final KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(KEYSPACE1);
        assertNotNull(ks);
        final TableMetadata cfm = ks.tables.getNullable(TABLE2);
        assertNotNull(cfm);

        // write some data, force a flush, then verify that files exist on disk.
        for (int i = 0; i < 100; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE2),
                                           "dropKs", "col" + i, "anyvalue");
        ColumnFamilyStore cfs = Keyspace.open(cfm.keyspace).getColumnFamilyStore(cfm.name);
        assertNotNull(cfs);
        Util.flush(cfs);
        assertTrue(!cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).list().isEmpty());

        SchemaTestUtil.announceKeyspaceDrop(ks.name);

        assertNull(Schema.instance.getKeyspaceMetadata(ks.name));

        // write should fail.
        boolean success = true;
        try
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)",
                                                         KEYSPACE1, TABLE2),
                                           "dropKs", "col0", "anyvalue");
        }
        catch (Throwable th)
        {
            success = false;
        }
        assertFalse("This mutation should have failed since the KS no longer exists.", success);

        // reads should fail too.
        boolean threw = false;
        try
        {
            Keyspace.open(ks.name);
        }
        catch (Throwable th)
        {
            threw = true;
        }
        assertTrue(threw);
    }
}
