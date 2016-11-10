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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.Util.column;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

@RunWith(Parameterized.class)
public class RecoveryManager2Test
{
    private static Logger logger = LoggerFactory.getLogger(RecoveryManager2Test.class);

    private static final String KEYSPACE1 = "RecoveryManager2Test";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

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

    public RecoveryManager2Test(ParameterizedClass commitLogCompression)
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

    @Test
    /* test that commit logs do not replay flushed data */
    public void testWithFlush() throws Exception
    {
        // Flush everything that may be in the commit log now to start fresh
        FBUtilities.waitOnFutures(Keyspace.open(SystemKeyspace.NAME).flush());

        CompactionManager.instance.disableAutoCompaction();

        // add a row to another CF so we test skipping mutations within a not-entirely-flushed CF
        insertRow("Standard2", "key");

        for (int i = 0; i < 100; i++)
        {
            String key = "key" + i;
            insertRow("Standard1", key);
        }

        Keyspace keyspace1 = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace1.getColumnFamilyStore("Standard1");
        logger.debug("forcing flush");
        cfs.forceBlockingFlush();

        logger.debug("begin manual replay");
        // replay the commit log (nothing on Standard1 should be replayed since everything was flushed, so only the row on Standard2
        // will be replayed)
        int replayed = CommitLog.instance.resetUnsafe(false);
        assert replayed == 1 : "Expecting only 1 replayed mutation, got " + replayed;
    }

    private void insertRow(String cfname, String key) 
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, cfname);
        cf.addColumn(column("col1", "val1", 1L));
        Mutation rm = new Mutation(KEYSPACE1, ByteBufferUtil.bytes(key), cf);
        rm.apply();
    }
}
