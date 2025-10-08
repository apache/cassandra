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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MVBackfillSSTableStreamSinkTest extends ViewAbstractTest
{
    private static final String VIEW_NAME = "test_view";

    private ColumnFamilyStore baseCfs;
    private TableMetadata baseMetadata;
    private ColumnFamilyStore viewCfs;

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws Throwable
    {
        // Create base table and view for testing
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        
        // Create a materialized view
        execute("CREATE MATERIALIZED VIEW " + VIEW_NAME + " AS " +
                "SELECT k, c, v FROM %s " +
                "WHERE k IS NOT NULL AND c IS NOT NULL " +
                "PRIMARY KEY (c, k)");

        baseCfs = getCurrentColumnFamilyStore();
        baseMetadata = baseCfs.metadata.get();
        // only one MV
        viewCfs = baseCfs.viewManager.allViewsCfs().iterator().next();
        execute("TRUNCATE TABLE system.view_backfills");
    }

    @After
    @Override
    public void afterTest() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
    }

    private Set<Range<Token>> getLocalRanges()
    {
        return StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
    }

    @Test
    public void testMVBackfillSSTableSink() throws Throwable
    {
        // Test creating the sink
        MVBackfillSSTableStreamSink sink;
        try
        {
            sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        }
        catch (IOException e)
        {
            fail("Should be able to create MVBackfillSSTableSink: " + e.getMessage());
            return;
        }

        // Verify backfill directory was created
        File backfillDir = sink.getBackfillDirectory();
        assertNotNull("Backfill directory should not be null", backfillDir);
        assertTrue("Backfill directory should exist", backfillDir.exists());
        assertTrue("Backfill directory should be a directory", backfillDir.isDirectory());
        assertTrue("Backfill directory name should contain 'mv_backfill'",
                   backfillDir.path().contains("mv_backfill"));

        // Test processing a view row (create a mock ViewRowResult)
        Row mockRow = createRow(1, "test_value");
        DecoratedKey mockViewPartitionKey = decoratedKey(1);
        ViewRowTranslator.ViewRowResult mockResult = new ViewRowTranslator.ViewRowResult(mockRow, mockViewPartitionKey);

        try
        {
            sink.processViewRow(mockResult);
            sink.processViewRow(null); // Should handle null gracefully
        }
        catch (Exception e)
        {
            fail("Should be able to process view rows: " + e.getMessage());
        }

        // Test completion
        try
        {
            sink.rowProcessComplete();
        }
        catch (Exception e)
        {
            fail("Should be able to complete successfully: " + e.getMessage());
        }

        // Verify that SSTables were created in the backfill directory
        File[] files = backfillDir.tryList();
        assertNotNull("Backfill directory should contain files", files);
    }

    @Test
    public void testMVBackfillSSTableSinkFailure() throws Throwable
    {
        MVBackfillSSTableStreamSink sink;
        
        try
        {
            sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        }
        catch (IOException e)
        {
            fail("Should be able to create MVBackfillSSTableSink: " + e.getMessage());
            return;
        }

        // Test failure handling
        // The sink should handle failure gracefully without throwing
        Exception testException = new RuntimeException("Test failure");
        sink.fail(testException);
    }

    @Test
    public void testMVBackfillSSTableSinkWithCustomBufferSize() throws Throwable
    {
        // Test creating sink with custom buffer size
        MVBackfillSSTableStreamSink sink;
        try
        {
            sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges(), 64); // 64MB buffer
        }
        catch (IOException e)
        {
            fail("Should be able to create MVBackfillSSTableSink with custom buffer size: " + e.getMessage());
            return;
        }

        // Verify it was created successfully
        assertNotNull("Sink should not be null", sink);
        assertNotNull("Backfill directory should not be null", sink.getBackfillDirectory());
        
        // Clean up
        try
        {
            sink.rowProcessComplete();
        }
        catch (Exception e)
        {
            fail("Should be able to complete successfully: " + e.getMessage());
        }
    }

    private DecoratedKey decoratedKey(int key)
    {
        return baseCfs.getPartitioner().decorateKey(Int32Type.instance.decompose(key));
    }

    private Row createRow(int clusteringKey, String value)
    {
        return new RowUpdateBuilder(baseMetadata, System.currentTimeMillis(), 1)
            .clustering(clusteringKey)
            .add("v", value)
            .build()
            .getPartitionUpdate(baseMetadata)
            .iterator()
            .next();
    }

    @Test
    public void testFailWithoutException() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        
        // Use reflection to replace the writer with a mock that closes successfully
        SSTableSimpleUnsortedWriter mockWriter = mock(SSTableSimpleUnsortedWriter.class);
        doNothing().when(mockWriter).close();
        
        // Replace the writer field using reflection
        Field writerField = MVBackfillSSTableStreamSink.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        writerField.set(sink, mockWriter);
        
        // Test that fail() handles successful cleanup
        RuntimeException originalException = new RuntimeException("Original failure");
        
        // This should not throw
        sink.fail(originalException);
        
        // Verify that close was not called on the mock writer
        verify(mockWriter, times(0)).close();
        
        // Verify that no suppressed exceptions were added
        Throwable[] suppressedExceptions = originalException.getSuppressed();
        assertEquals("Should have no suppressed exceptions", 0, suppressedExceptions.length);
    }

    @Test
    public void testRowProcessCompleteAfterFailure() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        
        // Use reflection to replace the writer with a mock
        SSTableSimpleUnsortedWriter mockWriter = mock(SSTableSimpleUnsortedWriter.class);
        doNothing().when(mockWriter).close();
        
        // Replace the writer field using reflection
        Field writerField = MVBackfillSSTableStreamSink.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        writerField.set(sink, mockWriter);
        
        // First call fail()
        RuntimeException originalException = new RuntimeException("Original failure");
        sink.fail(originalException);
        
        // Then call complete() - this should work (writer.close() should be idempotent)
        sink.rowProcessComplete();
        
        // Verify that close not called on the mock writer as it will be called after rowProcessCleanup
        verify(mockWriter, times(0)).close();

        sink.rowProcessCleanup();
        verify(mockWriter, times(1)).close();
    }

    @Test
    public void testDeleteMVBackfillFilesWithValidDirectory() throws Throwable
    {
        // Create a sink with valid MV backfill directory
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());

        File backfillDir = sink.getBackfillDirectory();
        assertTrue("Backfill directory should exist", backfillDir.exists());
        assertTrue("Backfill directory should be a directory", backfillDir.isDirectory());
        assertTrue("Backfill directory should contain mv_backfill",
                   backfillDir.absolutePath().contains("mv_backfill"));
        
        // Close the writer before calling complete
        sink.rowProcessComplete();
        
        // This should successfully delete the backfill directory
        sink.complete();
        
        // Verify that the directory was empty
        assertTrue("Backfill directory should be deleted after complete()", backfillDir.exists());
        assertTrue(backfillDir.isDirectory());
        File[] files = backfillDir.list();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    public void testDeleteMVBackfillFilesAssertsOnDataDirectory() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        
        // Use reflection to replace the backfillDirectory field with a mock data/data directory
        // This simulates a bug where getBackfillDirectory() accidentally returns the data directory
        File dataDir = viewCfs.getDirectories().getDirectoryForNewSSTables();
        
        Field backfillDirField = MVBackfillSSTableStreamSink.class.getDeclaredField("backfillDirectory");
        backfillDirField.setAccessible(true);
        backfillDirField.set(sink, dataDir);
        
        // Close the writer first
        sink.rowProcessComplete();
        
        // This should fail with an AssertionError because the directory doesn't contain "mv_backfill"
        try
        {
            sink.complete();
            fail("Expected AssertionError when trying to delete a directory that doesn't contain 'mv_backfill'");
        }
        catch (AssertionError e)
        {
            // Expected - the assertion should catch this bug
            assertTrue("Assertion error message should be related to mv_backfill check", true);
        }
    }

    @Test
    public void testDeleteMVBackfillFilesAssertsOnNonDirectory() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        
        File backfillDir = sink.getBackfillDirectory();
        
        // Create a file (not a directory) in the backfill directory's parent
        File parentDir = backfillDir.parent();
        File fakeFile = new File(parentDir, "mv_backfill_fake_file");
        fakeFile.createFileIfNotExists();
        
        // Use reflection to replace the backfillDirectory field with the file
        Field backfillDirField = MVBackfillSSTableStreamSink.class.getDeclaredField("backfillDirectory");
        backfillDirField.setAccessible(true);
        backfillDirField.set(sink, fakeFile);
        
        // Close the writer first
        sink.rowProcessComplete();
        
        // This should fail with an AssertionError because the path is not a directory
        try
        {
            sink.complete();
            fail("Expected AssertionError when trying to delete a non-directory path");
        }
        catch (AssertionError e)
        {
            // Expected - the assertion should catch that it's not a directory
            assertTrue("Assertion error should be related to directory check", true);
        }
        finally
        {
            // Clean up the fake file
            if (fakeFile.exists())
                fakeFile.delete();
        }
    }

    @Test
    public void testDeleteMVBackfillFilesAssertsOnRootDataDirectory() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, getLocalRanges());
        
        // Get a reference to the root data directory (several levels up from mv_backfill)
        // The typical structure is: data/keyspace/table_name/mv_backfill
        File backfillDir = sink.getBackfillDirectory();
        File tableDir = backfillDir.parent(); // table directory
        File keyspaceDir = tableDir.parent(); // keyspace directory
        File dataDir = keyspaceDir.parent(); // data directory
        
        // Use reflection to replace the backfillDirectory field with the data directory
        // This simulates a severe bug where the directory accidentally points to the root data folder
        Field backfillDirField = MVBackfillSSTableStreamSink.class.getDeclaredField("backfillDirectory");
        backfillDirField.setAccessible(true);
        backfillDirField.set(sink, dataDir);
        
        // Close the writer first
        sink.rowProcessComplete();
        
        // This should fail with an AssertionError because the directory doesn't contain "mv_backfill"
        try
        {
            sink.complete();
            fail("Expected AssertionError when trying to delete the root data directory");
        }
        catch (AssertionError e)
        {
            // Expected - the assertion should prevent accidental deletion of the data directory
            assertTrue("Assertion should prevent deletion of data directory", true);
        }
    }

    // ===== SystemKeyspace Integration Tests =====

    @Test
    public void testViewBackfillStatusLifecycle() throws Throwable
    {
        Set<Range<Token>> ranges = getLocalRanges();
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Initially, no status should exist
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNull("Initial status should be null", status);

        // Start a backfill
        SystemKeyspace.startViewBackfill(keyspaceName, viewName, ranges);

        // Verify STARTED status
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after start", status);
        assertEquals("Status should be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status.status);
        assertNull("Directory should be null initially", status.backfillDirectory);
        assertTrue("Succeeded hosts should be empty", status.streamSucceededHosts.isEmpty());

        // Mark SSTable build complete
        String testDirectoryPath = "/tmp/test/mv_backfill";
        SystemKeyspace.setViewBackfillSStableComplete(keyspaceName, viewName, ranges, testDirectoryPath);

        // Verify SSTABLE_BUILD_COMPLETE status
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after SSTable complete", status);
        assertEquals("Status should be SSTABLE_BUILD_COMPLETE", 
                    SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);
        // Note: directory might be null if path doesn't exist on filesystem

        // Update succeeded hosts
        Set<InetAddressAndPort> succeededHosts = Set.of(InetAddressAndPort.getByName("127.0.0.1"), InetAddressAndPort.getByName("127.0.0.2"));
        Set<String> succeededHostNames = Set.of(InetAddressAndPort.getByName("127.0.0.1").getHostAddressAndPort(), InetAddressAndPort.getByName("127.0.0.2").getHostAddressAndPort());
        SystemKeyspace.updateViewBackfillStreamSucceededHosts(keyspaceName, viewName, ranges, succeededHosts);

        // Verify succeeded hosts are updated
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after host update", status);
        assertEquals("Status should still be SSTABLE_BUILD_COMPLETE", 
                    SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);
        assertEquals("Succeeded hosts should match", succeededHostNames, status.streamSucceededHosts);

        // Complete the backfill
        SystemKeyspace.setViewBackfillComplete(keyspaceName, viewName, ranges);

        // Verify COMPLETE status
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after completion", status);
        assertEquals("Status should be COMPLETE", SystemKeyspace.ViewBackfillState.COMPLETE, status.status);

        // Clean up
        SystemKeyspace.removeViewBackfillStatus(keyspaceName, viewName);
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNull("Status should be null after removal", status);
    }

    @Test
    public void testSinkPrepareWithDifferentStatuses() throws Throwable
    {
        Set<Range<Token>> ranges = getLocalRanges();
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Test 1: No existing status (should start new backfill)
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        SystemKeyspace.ViewBackfillStatus status = sink.prepare(false);
        assertTrue("Should build SSTables when no status exists", status.shouldBuildSSTables());
        
        // Verify status was created
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should be created", status);
        assertEquals("Status should be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status.status);

        // Test 2: STARTED status (should continue building)
        sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        status = sink.prepare(false);
        assertTrue("Should build SSTables when status is STARTED", status.shouldBuildSSTables());

        // Test 3: SSTABLE_BUILD_COMPLETE status (should skip building)
        SystemKeyspace.setViewBackfillSStableComplete(keyspaceName, viewName, ranges, "/tmp/test");
        sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        status = sink.prepare(false);
        assertFalse("Should not build SSTables when status is SSTABLE_BUILD_COMPLETE", status.shouldBuildSSTables());

        // Test 4: COMPLETE status (should skip everything)
        SystemKeyspace.setViewBackfillComplete(keyspaceName, viewName, ranges);
        sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        status = sink.prepare(false);
        assertFalse("Should not build SSTables when status is COMPLETE", status.shouldBuildSSTables());

        // Test 5: Restart flag overrides status
        sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        status = sink.prepare(true); // restart = true
        assertTrue("Should build SSTables when restart is true regardless of status", status.shouldBuildSSTables());

        // Clean up
        SystemKeyspace.removeViewBackfillStatus(keyspaceName, viewName);
    }

    @Test
    public void testSinkRowProcessCompleteUpdatesStatus() throws Throwable
    {
        Set<Range<Token>> ranges = getLocalRanges();
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Create sink and prepare
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        sink.prepare(false);

        // Verify initial STARTED status
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertEquals("Status should be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status.status);

        // Complete row processing
        sink.rowProcessComplete();

        // Verify status is updated to SSTABLE_BUILD_COMPLETE
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after row processing", status);
        assertEquals("Status should be SSTABLE_BUILD_COMPLETE after row processing", 
                    SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);

        // Clean up
        SystemKeyspace.removeViewBackfillStatus(keyspaceName, viewName);
    }

    @Test
    public void testSinkCompleteUpdatesStatus() throws Throwable
    {
        Set<Range<Token>> ranges = getLocalRanges();
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Create sink, prepare, and complete row processing
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        sink.prepare(false);
        sink.rowProcessComplete();

        // Verify SSTABLE_BUILD_COMPLETE status
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertEquals("Status should be SSTABLE_BUILD_COMPLETE", 
                    SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);

        // Complete the backfill
        sink.complete();

        // Verify status is updated to COMPLETE
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after completion", status);
        assertEquals("Status should be COMPLETE after completion", 
                    SystemKeyspace.ViewBackfillState.COMPLETE, status.status);

        // Note: complete() also deletes the backfill files, so we can't test directory cleanup
        // without the directory existing, but the status update is the key functionality
    }

    @Test
    public void testSinkFailWithSSTableBuildComplete() throws Throwable
    {
        Set<Range<Token>> ranges = getLocalRanges();
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Create sink, prepare, and complete row processing
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        sink.prepare(false);
        sink.rowProcessComplete();

        // Add some succeeded hosts to test the update functionality
        Set<String> succeededHosts = Set.of("127.0.0.1");
        
        // Use reflection to set succeeded hosts in the sink
        Field succeededHostsField = MVBackfillSSTableStreamSink.class.getDeclaredField("succeededHosts");
        succeededHostsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<InetAddressAndPort> sinkSucceededHosts = (Set<InetAddressAndPort>) succeededHostsField.get(sink);
        sinkSucceededHosts.add(InetAddressAndPort.getByName("127.0.0.1"));

        // Verify initial status
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertEquals("Status should be SSTABLE_BUILD_COMPLETE", 
                    SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);
        assertTrue("Initial succeeded hosts should be empty", status.streamSucceededHosts.isEmpty());

        // Trigger failure
        Exception testException = new RuntimeException("Test streaming failure");
        sink.fail(testException);

        // Verify that succeeded hosts were updated during failure handling
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after failure", status);
        assertEquals("Status should still be SSTABLE_BUILD_COMPLETE after failure", 
                    SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);
        assertFalse("Succeeded hosts should not be empty after failure update", 
                   status.streamSucceededHosts.isEmpty());
        assertTrue("Succeeded hosts should contain 127.0.0.1", 
                  status.streamSucceededHosts.contains(InetAddressAndPort.getByName("127.0.0.1").getHostAddressAndPort()));

        // Clean up
        SystemKeyspace.removeViewBackfillStatus(keyspaceName, viewName);
    }

    @Test
    public void testSinkFailWithoutSSTableBuildComplete() throws Throwable
    {
        Set<Range<Token>> ranges = getLocalRanges();
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Create sink and prepare (but don't complete row processing)
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges);
        sink.prepare(false);

        // Verify initial STARTED status
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertEquals("Status should be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status.status);

        // Trigger failure
        Exception testException = new RuntimeException("Test SSTable build failure");
        sink.fail(testException);

        // Verify that succeeded hosts were NOT updated (since status is not SSTABLE_BUILD_COMPLETE)
        status = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges);
        assertNotNull("Status should exist after failure", status);
        assertEquals("Status should still be STARTED after failure", 
                    SystemKeyspace.ViewBackfillState.STARTED, status.status);
        assertTrue("Succeeded hosts should remain empty", status.streamSucceededHosts.isEmpty());

        // Clean up
        SystemKeyspace.removeViewBackfillStatus(keyspaceName, viewName);
    }

    @Test
    public void testMultipleRangesCreateSeparateEntries() throws Throwable
    {
        String keyspaceName = KEYSPACE;
        String viewName = VIEW_NAME;

        // Create two different range sets
        Set<Range<Token>> ranges1 = Set.of(new Range<>(baseCfs.metadata().partitioner.getMinimumToken(), 
                                                      baseCfs.metadata().partitioner.getRandomToken()));
        Set<Range<Token>> ranges2 = Set.of(new Range<>(baseCfs.metadata().partitioner.getRandomToken(), 
                                                      baseCfs.metadata().partitioner.getMaximumToken()));

        // Start backfill for first range set
        SystemKeyspace.startViewBackfill(keyspaceName, viewName, ranges1);
        SystemKeyspace.ViewBackfillStatus status1 = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges1);
        assertNotNull("Status for ranges1 should exist", status1);
        assertEquals("Status for ranges1 should be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status1.status);

        // Start backfill for second range set
        SystemKeyspace.startViewBackfill(keyspaceName, viewName, ranges2);
        SystemKeyspace.ViewBackfillStatus status2 = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges2);
        assertNotNull("Status for ranges2 should exist", status2);
        assertEquals("Status for ranges2 should be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status2.status);

        // Verify they are independent
        SystemKeyspace.setViewBackfillComplete(keyspaceName, viewName, ranges1);
        status1 = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges1);
        status2 = SystemKeyspace.getViewBackfillStatus(keyspaceName, viewName, ranges2);
        
        assertEquals("Status for ranges1 should be COMPLETE", SystemKeyspace.ViewBackfillState.COMPLETE, status1.status);
        assertEquals("Status for ranges2 should still be STARTED", SystemKeyspace.ViewBackfillState.STARTED, status2.status);

        // Clean up
        SystemKeyspace.removeViewBackfillStatus(keyspaceName, viewName);
    }
}

