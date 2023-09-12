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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for the truncate operation.
 */
@RunWith(Parameterized.class)
public class RecoveryManagerTruncateTest
{
    private static final String KEYSPACE1 = "RecoveryManagerTruncateTest";
    private static final String CF_STANDARD1 = "Standard1";

    public RecoveryManagerTruncateTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
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
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testTruncate() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        // add a single cell
        new RowUpdateBuilder(cfs.metadata(), 0, "key1")
            .clustering("cc")
            .add("val", "val1")
            .build()
            .apply();

        // Make sure data was written
        assertTrue(Util.getAll(Util.cmd(cfs).build()).size() > 0);

        // and now truncate it
        cfs.truncateBlocking();
        Map<Keyspace, Integer> replayed = CommitLog.instance.resetUnsafe(false);
        assertNull("Expected no mutations to be replayed for " + keyspace + " keyspace, but got " + replayed,
                     replayed.get(keyspace));

        // and validate truncation.
        Util.assertEmptyUnfiltered(Util.cmd(cfs).build());
    }

    @Test
    public void testTruncateWithReplay() throws IOException
    {
        // Tests that a the recovery (commitlog replay) in combination with a truncate operation works.
        //
        // Test procedure:
        // 1. add two mutations
        // 2. perform truncate
        // 3. add another mutation
        // 4. replay CL - there must be exactly one replayed mutation and two skipped mutations
        // 5. truncate again
        // 6. replay CL - since there was no activity on the CL, there should be no segments and nothing being replayed or skipped

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);

        // add some data
        new RowUpdateBuilder(cfs.metadata(), 0, "key1")
        .clustering("cc")
        .add("val", "val1")
        .build()
        .apply();
        new RowUpdateBuilder(cfs.metadata(), 0, "key2")
        .clustering("dd")
        .add("val", "val2")
        .build()
        .apply();

        // Make sure data was written
        assertEquals(2, Util.getAll(Util.cmd(cfs).build()).size());

        // and now truncate it
        cfs.truncateBlocking();

        // add another single cell
        new RowUpdateBuilder(cfs.metadata(), 0, "key3")
        .clustering("ee")
        .add("val", "val3")
        .build()
        .apply();

        CommitLogReplayer.MutationInitiator originalInitiator = CommitLogReplayer.mutationInitiator;
        FilteringInitiator filteringInitiator = new FilteringInitiator();
        CommitLogReplayer.mutationInitiator = filteringInitiator;
        try
        {
            // Expect exactly three records, only one replayed (the 3rd row-update above)
            CommitLog.instance.resetUnsafe(false);
            assertEquals(1, filteringInitiator.replayed);
            assertEquals(2, filteringInitiator.skipped);

            // and validate truncation.
            assertEquals(1, Util.getAll(Util.cmd(cfs).build()).size());

            filteringInitiator.reset();

            // another truncate
            cfs.truncateBlocking();

            // No replayed mutations this time
            CommitLog.instance.resetUnsafe(false);
            assertEquals(0, filteringInitiator.replayed);
            assertEquals(0, filteringInitiator.skipped);
        }
        finally
        {
            CommitLogReplayer.mutationInitiator = originalInitiator;
        }

        // and validate truncation.
        assertEquals(0, Util.getAll(Util.cmd(cfs).build()).size());
    }

    private static class FilteringInitiator extends CommitLogReplayer.MutationInitiator
    {
        volatile int replayed;
        volatile int skipped;

        @Override
        protected void onReplayed(PartitionUpdate update, long segmentId, int position)
        {
            if (KEYSPACE1.equals(update.metadata().keyspace))
            {
                replayed++;
            }
        }

        @Override
        protected void onSkipped(PartitionUpdate update)
        {
            if (KEYSPACE1.equals(update.metadata().keyspace))
            {
                skipped++;
            }
        }

        public void reset()
        {
            replayed = skipped = 0;
        }
    }
}
