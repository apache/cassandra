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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.accord.fastpath.FastPathStrategy;
import org.apache.cassandra.service.accord.fastpath.ParameterizedFastPathStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.AddAccordTable;

import static java.lang.String.format;

public class FastPathSchemaTest
{
    private static String KEYSPACE = "ks";
    private static int ksCount = 0;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
        SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of()));
    }

    @Before
    public void setup()
    {
        KEYSPACE = format("ks_%s", ksCount++);
    }


    private static void process(String fmt, Object... objects)
    {
        QueryProcessor.process(format(fmt, objects), ConsistencyLevel.ANY);
    }

    @Test
    public void keyspaceInheriting()
    {
        process("CREATE KEYSPACE %s with replication={'class':'SimpleStrategy', 'replication_factor':1} AND fast_path='simple'", KEYSPACE);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        Assert.assertSame(FastPathStrategy.simple(), ksm.params.fastPath);

        process("CREATE TABLE %s.tbl (k int primary key, v int)", KEYSPACE);
        TableMetadata tbm = Schema.instance.getTableMetadata(KEYSPACE, "tbl");
        Assert.assertSame(FastPathStrategy.inheritKeyspace(), tbm.params.fastPath);

        Epoch epoch = ClusterMetadata.current().epoch;
        AddAccordTable.addTable(tbm.id);

        Assert.assertEquals(epoch.getEpoch() + 1, ClusterMetadata.current().epoch.getEpoch());
    }

    @Test
    public void keyspaceModification()
    {
        process("CREATE KEYSPACE %s with replication={'class':'SimpleStrategy', 'replication_factor':1} AND fast_path='simple'", KEYSPACE);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        Assert.assertSame(FastPathStrategy.simple(), ksm.params.fastPath);
        process("ALTER KEYSPACE %s with fast_path={'size':2, 'dcs':'dc1,dc2'}", KEYSPACE);

        ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        Assert.assertSame(FastPathStrategy.Kind.PARAMETERIZED, ksm.params.fastPath.kind());
        ParameterizedFastPathStrategy strategy = (ParameterizedFastPathStrategy) ksm.params.fastPath;
        Assert.assertEquals(2, strategy.size);
        Assert.assertEquals(Arrays.asList("dc1", "dc2"), strategy.dcStrings());
    }

    @Test(expected = ConfigurationException.class)
    public void keyspaceInheritingFailure()
    {
        process("CREATE KEYSPACE %s with replication={'class':'SimpleStrategy', 'replication_factor':1} AND fast_path='keyspace'", KEYSPACE);
    }

    @Test
    public void tableModification()
    {
        process("CREATE KEYSPACE %s with replication={'class':'SimpleStrategy', 'replication_factor':1} AND fast_path='simple'", KEYSPACE);
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        Assert.assertSame(FastPathStrategy.simple(), ksm.params.fastPath);

        process("CREATE TABLE %s.tbl (k int primary key, v int)", KEYSPACE);
        TableMetadata tbm = Schema.instance.getTableMetadata(KEYSPACE, "tbl");
        Assert.assertSame(FastPathStrategy.inheritKeyspace(), tbm.params.fastPath);

        AddAccordTable.addTable(tbm.id);

        process("ALTER TABLE %s.tbl WITH fast_path='simple'", KEYSPACE);
        tbm = Schema.instance.getTableMetadata(KEYSPACE, "tbl");
        Assert.assertSame(FastPathStrategy.simple(), tbm.params.fastPath);
    }
}
