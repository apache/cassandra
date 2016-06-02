package org.apache.cassandra.db;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.db.KeyspaceTest.assertColumns;

@RunWith(Parameterized.class)
public class RecoveryManager3Test
{
    private static final String KEYSPACE1 = "RecoveryManager3Test1";
    private static final String CF_STANDARD1 = "Standard1";

    private static final String KEYSPACE2 = "RecoveryManager3Test2";
    private static final String CF_STANDARD3 = "Standard3";

    public RecoveryManager3Test(ParameterizedClass commitLogCompression)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
    }

    @Before
    public void setUp() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][] {
                { null }, // No compression
                { new ParameterizedClass(LZ4Compressor.class.getName(), Collections.<String, String>emptyMap()) },
                { new ParameterizedClass(SnappyCompressor.class.getName(), Collections.<String, String>emptyMap()) },
                { new ParameterizedClass(DeflateCompressor.class.getName(), Collections.<String, String>emptyMap()) } });
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE2, CF_STANDARD3));
    }

    @Test
    public void testMissingHeader() throws IOException
    {
        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        Keyspace keyspace2 = Keyspace.open(KEYSPACE2);

        Mutation rm;
        DecoratedKey dk = Util.dk("keymulti");
        ColumnFamily cf;

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        rm = new Mutation(KEYSPACE1, dk.getKey(), cf);
        rm.apply();

        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE2, "Standard3");
        cf.addColumn(column("col2", "val2", 1L));
        rm = new Mutation(KEYSPACE2, dk.getKey(), cf);
        rm.apply();

        keyspace1.getColumnFamilyStore("Standard1").clearUnsafe();
        keyspace2.getColumnFamilyStore("Standard3").clearUnsafe();

        // nuke the header
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).listFiles())
        {
            if (file.getName().endsWith(".header"))
                FileUtils.deleteWithConfirm(file);
        }

        CommitLog.instance.resetUnsafe(false); // disassociate segments from live CL

        assertColumns(Util.getColumnFamily(keyspace1, dk, "Standard1"), "col1");
        assertColumns(Util.getColumnFamily(keyspace2, dk, "Standard3"), "col2");
    }
}
