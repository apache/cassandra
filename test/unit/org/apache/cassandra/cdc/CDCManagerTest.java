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

package org.apache.cassandra.cdc;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogSegmentManagerCDC;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;


public class CDCManagerTest extends CQLTester
{
    private static final Random random = new Random();

    @BeforeClass
    public static void checkConfig()
    {
        Assume.assumeTrue(DatabaseDescriptor.isCDCEnabled());
        DatabaseDescriptor.setCommitLogSegmentSize(1);
    }

    @Before
    public void beforeTest() throws Throwable
    {
        // Create keyspaces with cdc_handler
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND cdc_handler = {'class': '%s'}", KEYSPACE, TestCDCHandler.class.getName()));
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND cdc_handler = {'class': '%s'}", KEYSPACE_PER_TEST, TestCDCHandler.class.getName()));

        // Need to clean out any files from previous test runs. Prevents flaky test failures.
        CommitLog.instance.resetUnsafe(true);
        ((CommitLogSegmentManagerCDC) CommitLog.instance.segmentManager).updateCDCTotalSize();
        // Start CDC Reader
        CDCManager.instance.startCDCReader();
    }

    @Test
    public void testGetHandler() throws Throwable
    {
        ICDCHandler handler = getCDCHandler();
        Assert.assertEquals(TestCDCHandler.class, handler.getClass());
    }

    @Test
    public void testScanlog() throws Throwable
    {
        populateData(5, true);
        Thread.sleep(1000);
        Assert.assertEquals(1, CDCManager.instance.getActiveIdxCount());
    }

    @Test
    public void testNonCDCTable() throws Throwable
    {
        ICDCHandler handler;

        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));");
        populateData(100, false);
        handler = getCDCHandler();
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(0, ((TestCDCHandler) handler).seenMutations.size());

        createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx)) WITH cdc=false;");
        populateData(100, false);
        handler = getCDCHandler();
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(0, ((TestCDCHandler) handler).seenMutations.size());
    }

    @Test
    public void testReaderSingleTable() throws Throwable
    {
        ICDCHandler handler;

        populateData(100, true);
        handler = getCDCHandler();
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(100, ((TestCDCHandler) handler).seenMutations.size());

        populateData(200, false);
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(300, ((TestCDCHandler) handler).seenMutations.size());
    }

    @Test
    public void testReaderMultiTables() throws Throwable
    {
        ICDCHandler handler;

        populateData(100, true);
        handler = getCDCHandler();
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(100, ((TestCDCHandler) handler).seenMutations.size());

        populateData(200, true);
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        // Need to get a new handler instance since keyspace metadata is updated when creating a table
        handler = getCDCHandler();
        Assert.assertEquals(200, ((TestCDCHandler) handler).seenMutations.size());
    }

    @Test
    public void testSchemaChange() throws Throwable
    {
        ICDCHandler oldhandler, newHandler;

        populateData(100, true);
        oldhandler = getCDCHandler();
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(100, ((TestCDCHandler) oldhandler).seenMutations.size());

        alterTable("ALTER TABLE %s ADD username text"); // alter table statement will trigger create new handler
        populateData(200, false);
        newHandler = getCDCHandler();
        CommitLog.instance.sync(true);
        Thread.sleep(1000);
        Assert.assertEquals(200, ((TestCDCHandler) newHandler).seenMutations.size());
        Assert.assertEquals(300, ((TestCDCHandler) newHandler).seenMutations.size() + ((TestCDCHandler) oldhandler).seenMutations.size());

    }

    @Test
    public void testCompleted() throws Throwable
    {
        populateData(10, true);

        ArrayList<File> idxFiles = getCDCIdxFiles();
        File firstIdxFile = idxFiles.get(0);
        logger.info("firstIdxFile: {}", firstIdxFile.getName());

        populateData(20000, false);

        Thread.sleep(1000);
        CommitLog.instance.sync(true);

        CommitLogDescriptor commitLog = CommitLogDescriptor.fromIdxFileName(firstIdxFile.getName());
        File logFile = new File(DatabaseDescriptor.getCDCLogLocation() + File.separator + commitLog.fileName());
        Assert.assertFalse(firstIdxFile.exists());
        Assert.assertFalse(logFile.exists());
    }

    @Test
    public void testCDCHandlerOptions() throws Throwable
    {
        String keyspaceName = "cdc_test_keyspace";
        schemaChange(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND cdc_handler = {'class': '%s', 'map1': '{\"k1\": \"v1\"}', 'string1': 'value1' }",
        keyspaceName, TestCDCHandler.class.getName()));
        TestCDCHandler handler = (TestCDCHandler) getCDCHandler(keyspaceName);
        Assert.assertEquals(handler.options.get("string1"), "value1");
        Assert.assertEquals(handler.options.get("map1"), "{\"k1\": \"v1\"}");
    }

    public static ICDCHandler getCDCHandler() throws Throwable
    {
        return getCDCHandler(KEYSPACE);
    }

    public static ICDCHandler getCDCHandler(String keyspaceName) throws Throwable
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspaceName);
        ICDCHandler handler = ks.getCDCHandler();
        return handler;
    }

    public void populateData(long n, boolean createTable) throws Throwable
    {
        if (createTable)
            createTable("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx)) WITH cdc=true;");

        for (int i = 0; i < n; i++)
        {
            execute("INSERT INTO %s (idx, data) VALUES (?, ?)", i, Integer.toString(i));
        }

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    static ArrayList<File> getCDCIdxFiles()
    {
        File dir = new File(DatabaseDescriptor.getCDCLogLocation());
        File[] files = dir.listFiles(CDCManager.idxFilesFilter);
        ArrayList<File> results = new ArrayList<>();
        for (File f : files)
        {
            if (f.isDirectory())
                continue;
            results.add(f);
        }
        Assert.assertTrue("Didn't find any CDC idx files.", 0 != results.size());
        return results;
    }

    public static class TestCDCHandler implements ICDCHandler
    {
        public List<Mutation> seenMutations = new ArrayList<Mutation>();
        public Map<String, String> options;

        @Override
        public void initialize(Map<String, String> options) throws ConfigurationException
        {
            logger.info("Initializing TestCDCHandler with options: {}", Arrays.toString(options.entrySet().toArray()));
            this.options = options;
        }

        @Override
        public void process(Mutation mutation)
        {
            seenMutations.add(mutation);
        }
    }
}
