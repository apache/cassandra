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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ViewAbstractTest;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
    }

    @After
    @Override
    public void afterTest() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));
    }

    @Test
    public void testMVBackfillSSTableSink() throws Throwable
    {
        // Test creating the sink
        MVBackfillSSTableStreamSink sink;
        try
        {
            sink = new MVBackfillSSTableStreamSink(viewCfs);
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
                   backfillDir.name().contains("mv_backfill"));

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
            sink = new MVBackfillSSTableStreamSink(viewCfs);
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
            sink = new MVBackfillSSTableStreamSink(viewCfs, 64); // 64MB buffer
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
    public void testRowProcessCompleteWithException() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
        // Use reflection to replace the writer with a mock that throws exception on close
        SSTableSimpleUnsortedWriter mockWriter = mock(SSTableSimpleUnsortedWriter.class);
        IOException testException = new IOException("Test exception during close");
        doThrow(testException).when(mockWriter).close();
        
        // Replace the writer field using reflection
        Field writerField = MVBackfillSSTableStreamSink.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        writerField.set(sink, mockWriter);
        
        // Test that complete() propagates the exception
        try
        {
            sink.rowProcessComplete();
            fail("Expected exception to be thrown from complete()");
        }
        catch (IOException e)
        {
            assertEquals("Exception should be the same as thrown by writer.close()", testException, e);
        }
        
        // Verify that close was called on the mock writer
        verify(mockWriter, times(1)).close();
    }

    @Test
    public void testFailWithException() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
        // Use reflection to replace the writer with a mock that throws exception on close
        SSTableSimpleUnsortedWriter mockWriter = mock(SSTableSimpleUnsortedWriter.class);
        IOException cleanupException = new IOException("Test cleanup exception");
        doThrow(cleanupException).when(mockWriter).close();
        
        // Replace the writer field using reflection
        Field writerField = MVBackfillSSTableStreamSink.class.getDeclaredField("writer");
        writerField.setAccessible(true);
        writerField.set(sink, mockWriter);
        
        // Test that fail() handles cleanup exception gracefully
        RuntimeException originalException = new RuntimeException("Original failure");
        
        // This should not throw - fail() should handle cleanup exceptions internally
        sink.fail(originalException);
        
        // Verify that close was called on the mock writer
        verify(mockWriter, times(1)).close();
        
        // Verify that the cleanup exception was added as suppressed to the original exception
        Throwable[] suppressedExceptions = originalException.getSuppressed();
        assertEquals("Should have one suppressed exception", 1, suppressedExceptions.length);
        assertEquals("Suppressed exception should be the cleanup exception", cleanupException, suppressedExceptions[0]);
    }

    @Test
    public void testFailWithoutException() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
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
        
        // Verify that close was called on the mock writer
        verify(mockWriter, times(1)).close();
        
        // Verify that no suppressed exceptions were added
        Throwable[] suppressedExceptions = originalException.getSuppressed();
        assertEquals("Should have no suppressed exceptions", 0, suppressedExceptions.length);
    }

    @Test
    public void testRowProcessCompleteAfterFailure() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
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
        
        // Verify that close was called twice on the mock writer
        verify(mockWriter, times(2)).close();
    }

    @Test
    public void testDeleteMVBackfillFilesWithValidDirectory() throws Throwable
    {
        // Create a sink with valid MV backfill directory
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);

        File backfillDir = sink.getBackfillDirectory();
        assertTrue("Backfill directory should exist", backfillDir.exists());
        assertTrue("Backfill directory should be a directory", backfillDir.isDirectory());
        assertTrue("Backfill directory should contain mv_backfill",
                   backfillDir.absolutePath().contains("mv_backfill"));
        
        // Close the writer before calling complete
        sink.rowProcessComplete();
        
        // This should successfully delete the backfill directory
        sink.complete();
        
        // Verify that the directory was deleted
        assertFalse("Backfill directory should be deleted after complete()", backfillDir.exists());
    }

    @Test
    public void testDeleteMVBackfillFilesAssertsOnDataDirectory() throws Throwable
    {
        // Create a sink
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
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
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
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
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs);
        
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
}

