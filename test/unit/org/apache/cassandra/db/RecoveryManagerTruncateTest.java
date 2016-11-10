/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.*;

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
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Test for the truncate operation.
 */
@RunWith(Parameterized.class)
public class RecoveryManagerTruncateTest
{
    private static final String KEYSPACE1 = "RecoveryManagerTruncateTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    public RecoveryManagerTruncateTest(ParameterizedClass commitLogCompression)
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
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
    }

    @Test
    public void testTruncate() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        Mutation rm;
        ColumnFamily cf;

        // add a single cell
        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD1);
        cf.addColumn(column("col1", "val1", 1L));
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("keymulti"), cf);
        rm.applyUnsafe();
        long time = System.currentTimeMillis();

        // Make sure data was written
        assertNotNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col1"));

        // and now truncate it
        cfs.truncateBlocking();
        CommitLog.instance.resetUnsafe(false);

        // and validate truncation.
        assertNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col1"));
        assertTrue(SystemKeyspace.getTruncatedAt(cfs.metadata.cfId) > time);
    }

    @Test
    public void testTruncatePointInTime() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        Mutation rm;
        ColumnFamily cf;

        // add a single cell
        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD1);
        cf.addColumn(column("col2", "val1", 1L));
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("keymulti"), cf);
        rm.apply();

        // Make sure data was written
        long time = System.currentTimeMillis();
        assertNotNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col2"));

        // and now truncate it
        cfs.truncateBlocking();

        // verify truncation
        assertNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col2"));

        try
        {
            // Restore to point in time.
            CommitLog.instance.archiver.restorePointInTime = time;
            CommitLog.instance.resetUnsafe(false);
        }
        finally
        {
            CommitLog.instance.archiver.restorePointInTime = Long.MAX_VALUE;
        }

        // Validate pre-truncation data was restored.
        assertNotNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col2"));
        // And that we don't have a truncation record after restore time.
        assertFalse(SystemKeyspace.getTruncatedAt(cfs.metadata.cfId) > time);
    }

    @Test
    public void testTruncatePointInTimeReplayList() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs1 = keyspace.getColumnFamilyStore(CF_STANDARD1);
        ColumnFamilyStore cfs2 = keyspace.getColumnFamilyStore(CF_STANDARD2);

        Mutation rm;
        ColumnFamily cf;

        // add a single cell
        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD1);
        cf.addColumn(column("col3", "val1", 1L));
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("keymulti"), cf);
        rm.apply();

        // add a single cell
        cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD2);
        cf.addColumn(column("col4", "val1", 1L));
        rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes("keymulti"), cf);
        rm.apply();

        // Make sure data was written
        long time = System.currentTimeMillis();
        assertNotNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col3"));
        assertNotNull(getFromTable(keyspace, CF_STANDARD2, "keymulti", "col4"));

        // and now truncate it
        cfs1.truncateBlocking();
        cfs2.truncateBlocking();

        // verify truncation
        assertNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col3"));
        assertNull(getFromTable(keyspace, CF_STANDARD2, "keymulti", "col4"));

        try
        {
            // Restore to point in time.
            CommitLog.instance.archiver.restorePointInTime = time;
            System.setProperty("cassandra.replayList", KEYSPACE1 + "." + CF_STANDARD1);
            CommitLog.instance.resetUnsafe(false);
        }
        finally
        {
            CommitLog.instance.archiver.restorePointInTime = Long.MAX_VALUE;
            System.clearProperty("cassandra.replayList");
        }

        // Validate pre-truncation data was restored.
        assertNotNull(getFromTable(keyspace, CF_STANDARD1, "keymulti", "col3"));
        // But only on the replayed table.
        assertNull(getFromTable(keyspace, CF_STANDARD2, "keymulti", "col4"));

        // And that we have the correct truncation records.
        assertFalse(SystemKeyspace.getTruncatedAt(cfs1.metadata.cfId) > time);
        assertTrue(SystemKeyspace.getTruncatedAt(cfs2.metadata.cfId) > time);
    }

    private Cell getFromTable(Keyspace keyspace, String cfName, String keyName, String columnName)
    {
        ColumnFamily cf;
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore(cfName);
        if (cfStore == null)
        {
            return null;
        }
        cf = cfStore.getColumnFamily(Util.namesQueryFilter(cfStore, Util.dk(keyName), columnName));
        if (cf == null)
        {
            return null;
        }
        return cf.getColumn(Util.cellname(columnName));
    }
}
