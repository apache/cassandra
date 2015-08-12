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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

import junit.framework.Assert;

import com.google.common.base.Predicate;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

public class CommitLogUpgradeTest
{
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

    @Test
    public void test20() throws Exception
    {
        testRestore(DATA_DIR + "2.0");
    }

    @Test
    public void test21() throws Exception
    {
        testRestore(DATA_DIR + "2.1");
    }

    @Test
    public void test22_truncated() throws Exception
    {
        testRestore(DATA_DIR + "2.2-lz4-truncated");
    }

    @Test(expected = CommitLogReplayException.class)
    public void test22_bitrot() throws Exception
    {
        shouldBeKilled = true;
        testRestore(DATA_DIR + "2.2-lz4-bitrot");
    }

    @Test
    public void test22_bitrot_ignored() throws Exception
    {
        try {
            System.setProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY, "true");
            testRestore(DATA_DIR + "2.2-lz4-bitrot");
        } finally {
            System.clearProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY);
        }
    }

    @Test(expected = CommitLogReplayException.class)
    public void test22_bitrot2() throws Exception
    {
        shouldBeKilled = true;
        testRestore(DATA_DIR + "2.2-lz4-bitrot2");
    }

    @Test
    public void test22_bitrot2_ignored() throws Exception
    {
        try {
            System.setProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY, "true");
            testRestore(DATA_DIR + "2.2-lz4-bitrot2");
        } finally {
            System.clearProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY);
        }
    }

    @BeforeClass
    static public void initialize() throws FileNotFoundException, IOException, InterruptedException
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("");
    }

    public void testRestore(String location) throws IOException, InterruptedException
    {
        Properties prop = new Properties();
        prop.load(new FileInputStream(new File(location + File.separatorChar + PROPERTIES_FILE)));
        int hash = Integer.parseInt(prop.getProperty(HASH_PROPERTY));
        int cells = Integer.parseInt(prop.getProperty(CELLS_PROPERTY));

        String cfidString = prop.getProperty(CFID_PROPERTY);
        if (cfidString != null)
        {
            UUID cfid = UUID.fromString(cfidString);
            if (Schema.instance.getCF(cfid) == null)
            {
                CFMetaData cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
                Schema.instance.purge(cfm);
                Schema.instance.load(cfm.copy(cfid));
            }
        }

        Hasher hasher = new Hasher();
        CommitLogTestReplayer replayer = new CommitLogTestReplayer(CommitLog.instance, hasher);
        File[] files = new File(location).listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return name.endsWith(".log");
            }
        });
        replayer.recover(files);

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
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                for (Cell c : cf.getSortedColumns())
                {
                    if (new String(c.name().toByteBuffer().array(), StandardCharsets.UTF_8).startsWith(CELLNAME))
                    {
                        hash = hash(hash, c.value());
                        ++cells;
                    }
                }
            }
            return true;
        }
    }
}
