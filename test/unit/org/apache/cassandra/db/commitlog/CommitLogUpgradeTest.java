package org.apache.cassandra.db.commitlog;

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
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.junit.Assert;

import com.google.common.base.Predicate;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

/**
 * Note: if you are looking to create new test cases for this test, check out
 * {@link CommitLogUpgradeTestMaker}
 */
public class CommitLogUpgradeTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final String DATA_DIR = "test/data/legacy-commitlog/";
    static final String PROPERTIES_FILE = "hash.txt";
    static final String CFID_PROPERTY = "cfid";
    static final String CELLS_PROPERTY = "cells";
    static final String HASH_PROPERTY = "hash";

    static final String TABLE = "Standard1";
    static final String KEYSPACE = "Keyspace1";
    static final String CELLNAME = "name";

    private JVMStabilityInspector.Killer originalKiller;
    private KillerForTests killerForTests;
    private boolean shouldBeKilled = false;

    static TableMetadata metadata =
        TableMetadata.builder(KEYSPACE, TABLE)
                     .addPartitionKeyColumn("key", AsciiType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("val", BytesType.instance)
                     .addRegularColumn("val0", BytesType.instance)
                     .compression(SchemaLoader.getCompressionParameters())
                     .build();

    @Before
    public void prepareToBeKilled()
    {
        killerForTests = new KillerForTests();
        originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
    }

    @After
    public void cleanUp()
    {
        JVMStabilityInspector.replaceKiller(originalKiller);
        Assert.assertEquals("JVM killed", shouldBeKilled, killerForTests.wasKilled());
    }

    // 30 matches version in MessagingService, 3.0.13 is the latest patch release after 3.0.0 but before 3.0.14
    @Test
    public void test30_encrypted() throws Exception
    {
        testRestore(DATA_DIR + "3.0.13-encrypted");
    }

    // 3014 matches version in MessagingService, 3.0.29 is the latest patch release after 3.0.14
    @Test
    public void test3014_encrypted() throws Exception
    {
        testRestore(DATA_DIR + "3.0.29-encrypted");
    }

    // 40 matches version in MessagingService, 4.0.11 is the latest patch release on 4.0
    @Test
    public void test40_encrypted() throws Exception
    {
        testRestore(DATA_DIR + "4.0.11-encrypted");
    }

    @Test
    public void test34_encrypted() throws Exception
    {
        testRestore(DATA_DIR + "3.4-encrypted");
    }

    @BeforeClass
    public static void initialize()
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);
        DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createContext(true));
    }

    public void testRestore(String location) throws IOException, InterruptedException
    {
        Properties prop = new Properties();
        prop.load(new FileInputStreamPlus(new File(location + File.pathSeparator() + PROPERTIES_FILE)));
        int hash = Integer.parseInt(prop.getProperty(HASH_PROPERTY));
        int cells = Integer.parseInt(prop.getProperty(CELLS_PROPERTY));

        String cfidString = prop.getProperty(CFID_PROPERTY);
        if (cfidString != null)
        {
            TableId tableId = TableId.fromString(cfidString);
            if (Schema.instance.getTableMetadata(tableId) == null)
                SchemaTestUtil.addOrUpdateKeyspace(KeyspaceMetadata.create(KEYSPACE,
                                                                           KeyspaceParams.simple(1),
                                                                           Tables.of(metadata.unbuild().id(tableId).build())),
                                                   true);
        }

        Hasher hasher = new Hasher();
        CommitLogTestReplayer replayer = new CommitLogTestReplayer(hasher);
        File[] files = new File(location).tryList((file, name) -> name.endsWith(".log"));
        replayer.replayFiles(files);

        Assert.assertEquals(cells, hasher.cells);
        Assert.assertEquals(hash, hasher.hash);
    }

    public static int hash(int hash, ByteBuffer bytes)
    {
        int shift = 0;
        for (int i = 0; i < bytes.limit(); i++)
        {
            hash += (bytes.get(i) & 0xFF) << shift;
            shift = (shift + 8) & 0x1F;
        }
        return hash;
    }

    class Hasher implements Predicate<Mutation>
    {
        int hash = 0;
        int cells = 0;

        @Override
        public boolean apply(Mutation mutation)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                for (Row row : update)
                    if (row.clustering().size() > 0 &&
                        AsciiType.instance.compose(row.clustering().bufferAt(0)).startsWith(CELLNAME))
                    {
                        for (Cell<?> cell : row.cells())
                        {
                            hash = hash(hash, cell.buffer());
                            ++cells;
                        }
                    }
            }
            return true;
        }
    }
}
