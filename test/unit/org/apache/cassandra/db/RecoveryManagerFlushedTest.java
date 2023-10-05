/*
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
 */
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

@RunWith(Parameterized.class)
public class RecoveryManagerFlushedTest
{
    private static Logger logger = LoggerFactory.getLogger(RecoveryManagerFlushedTest.class);

    private static final String KEYSPACE1 = "RecoveryManager2Test";
    private static final String CF_STANDARD1 = "Standard1";
    private static final String CF_STANDARD2 = "Standard2";

    public RecoveryManagerFlushedTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
        DatabaseDescriptor.initializeCommitLogDiskAccessMode();
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
            {null, EncryptionContextGenerator.createDisabledContext()}, // No compression, no encryption
            {null, EncryptionContextGenerator.createContext(true)}, // Encryption
            {new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(SnappyCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()},
            {new ParameterizedClass(ZstdCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext()}});
    }

    @Before
    public void setUp() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), FBUtilities.getBroadcastAddressAndPort());

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD2));
    }

    @Test
    /* test that commit logs do not replay flushed data */
    public void testWithFlush() throws Exception
    {
        // Flush everything that may be in the commit log now to start fresh
        Util.flushKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        Util.flushKeyspace(SchemaConstants.SCHEMA_KEYSPACE_NAME);


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
        Util.flush(cfs);

        logger.debug("begin manual replay");
        // replay the commit log (nothing on Standard1 should be replayed since everything was flushed, so only the row on Standard2
        // will be replayed)
        int replayed = CommitLog.instance.resetUnsafe(false);
        assert replayed == 1 : "Expecting only 1 replayed mutation, got " + replayed;
    }

    private void insertRow(String cfname, String key)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        new RowUpdateBuilder(cfs.metadata(), 0, key)
            .clustering("c")
            .add("val", "val1")
            .build()
            .apply();
    }
}
