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

package org.apache.cassandra.repair.consistent.admin;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class SchemaArgsParserTest
{
    private static final String KEYSPACE = "schemaargsparsertest";
    private static final int NUM_TBL = 3;
    private static TableMetadata[] cfm = new TableMetadata[NUM_TBL];
    private static ColumnFamilyStore[] cfs = new ColumnFamilyStore[NUM_TBL];

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        for (int i=0; i<NUM_TBL; i++)
            cfm[i] = CreateTableStatement.parse("CREATE TABLE tbl" + i + " (k INT PRIMARY KEY, v INT)", KEYSPACE).build();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);
        for (int i=0; i<NUM_TBL; i++)
            cfs[i] = Schema.instance.getColumnFamilyStoreInstance(cfm[i].id);
    }

    /**
     * Specifying only the keyspace should return all tables in that keyspaces
     */
    @Test
    public void keyspaceOnly()
    {
        Set<ColumnFamilyStore> tables = Sets.newHashSet(SchemaArgsParser.parse(KEYSPACE));
        Assert.assertEquals(Sets.newHashSet(cfs), tables);
    }

    @Test
    public void someTables()
    {
        Set<ColumnFamilyStore> tables = Sets.newHashSet(SchemaArgsParser.parse(KEYSPACE, "tbl1", "tbl2"));
        Assert.assertEquals(Sets.newHashSet(cfs[1], cfs[2]), tables);
    }

    @Test
    public void noKeyspace()
    {
        Set<ColumnFamilyStore> allTables = Sets.newHashSet(SchemaArgsParser.parse().iterator());
        Assert.assertTrue(allTables.containsAll(Sets.newHashSet(cfs)));
    }

    @Test( expected = IllegalArgumentException.class )
    public void invalidKeyspace()
    {
        Sets.newHashSet(SchemaArgsParser.parse("SOME_KEYSPACE"));
    }

    @Test( expected = IllegalArgumentException.class )
    public void invalidTables()
    {
        Set<ColumnFamilyStore> tables = Sets.newHashSet(SchemaArgsParser.parse(KEYSPACE, "sometable"));
    }
}
