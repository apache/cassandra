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

package org.apache.cassandra.index.sai.disk.format;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.GROUP_META;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.META;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class IndexDescriptorFileAccessTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final IndexIdentifier INDEX = new IndexIdentifier("ks1", "tbl1", "idx1");

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void existenceAndLengthReturnFromInjectedAccess()
    {
        RecordingFileAccess access = new RecordingFileAccess();
        IndexDescriptor descriptor = create(access);

        File sstableFile = descriptor.fileFor(GROUP_META);
        File columnFile = descriptor.fileFor(META, INDEX);

        // neither file exists on disk, the provider is the source of truth
        assertFalse(sstableFile.exists());
        assertFalse(columnFile.exists());
        access.sizes.put(sstableFile, 11L);
        access.sizes.put(columnFile, 17L);

        assertTrue(descriptor.hasComponent(GROUP_META));
        assertTrue(descriptor.hasComponent(META, INDEX));
        assertEquals(17L, descriptor.sizeOnDiskOfPerIndexComponent(META, INDEX));
        assertTrue(descriptor.getLivePerSSTableComponents().contains(descriptor.version.makePerSSTableComponent(GROUP_META)));
        assertEquals(11L, descriptor.sizeOnDiskOfPerSSTableComponents());

        access.sizes.remove(columnFile);
        assertFalse(descriptor.hasComponent(META, INDEX));
        assertEquals(0L, descriptor.sizeOnDiskOfPerIndexComponent(META, INDEX));
    }

    @Test
    public void blockingInputsUseUnmappedHandlesAndCloseThem() throws Exception
    {
        RecordingFileAccess access = new RecordingFileAccess();
        IndexDescriptor descriptor = create(access);

        File perSSTable = descriptor.fileFor(GROUP_META);
        File perIndex = descriptor.fileFor(META, INDEX);
        Files.write(perSSTable.toPath(), new byte[]{ 1, 2, 3 });
        Files.write(perIndex.toPath(), new byte[]{ 4, 5, 6 });

        try (IndexInput input = descriptor.openPerSSTableInput(GROUP_META))
        {
            assertEquals(1, input.readByte());
            assertFalse(access.lastMmap);
            assertEquals(perSSTable, access.lastOpenedFile);
        }
        verify(access.lastHandle).close();

        try (IndexInput input = descriptor.openPerIndexInput(META, INDEX))
        {
            assertEquals(4, input.readByte());
            assertFalse(access.lastMmap);
            assertEquals(perIndex, access.lastOpenedFile);
        }
        verify(access.lastHandle).close();
    }

    @Test
    public void directHandlesRequestMmap() throws Exception
    {
        RecordingFileAccess access = new RecordingFileAccess();
        IndexDescriptor descriptor = create(access);

        File sstableFile = descriptor.fileFor(GROUP_META);
        File columnFile = descriptor.fileFor(META, INDEX);
        Files.write(sstableFile.toPath(), new byte[]{ 1 });
        Files.write(columnFile.toPath(), new byte[]{ 2 });

        try (FileHandle ignored = descriptor.createPerSSTableFileHandle(GROUP_META, null))
        {
            assertTrue(access.lastMmap);
            assertEquals(sstableFile, access.lastOpenedFile);
        }

        try (FileHandle ignored = descriptor.createPerIndexFileHandle(META, INDEX))
        {
            assertTrue(access.lastMmap);
            assertEquals(columnFile, access.lastOpenedFile);
        }
    }

    @Test
    public void readerInitializationFailureClosesHandle()
    {
        IndexDescriptor.FileAccess access = mock(IndexDescriptor.FileAccess.class);
        IndexDescriptor descriptor = create(access);

        File file = descriptor.fileFor(GROUP_META);
        FileHandle handle = mock(FileHandle.class);

        RuntimeException readerError = new IllegalStateException("reader failed");
        RuntimeException closeError = new IllegalStateException("close failed");

        when(access.open(file, false)).thenReturn(handle);
        when(handle.createReader()).thenThrow(readerError);
        doThrow(closeError).when(handle).close();

        try
        {
            descriptor.openPerSSTableInput(GROUP_META);
            fail("Expected the reader failure");
        }
        catch (IllegalStateException e)
        {
            assertSame(readerError, e);
            assertEquals(1, e.getSuppressed().length);
            assertSame(closeError, e.getSuppressed()[0]);
        }
        verify(handle).close();
    }

    private Descriptor sstableDescriptor()
    {
        return new Descriptor(new File(temporaryFolder.getRoot()), "ks1", "tbl1", new SequenceBasedSSTableId(1));
    }

    private IndexDescriptor create(IndexDescriptor.FileAccess access)
    {
        return IndexDescriptor.create(sstableDescriptor(), Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR, access);
    }

    private static class RecordingFileAccess implements IndexDescriptor.FileAccess
    {
        final Map<File, Long> sizes = new HashMap<>();
        File lastOpenedFile;
        FileHandle lastHandle;
        boolean lastMmap;

        @Override
        public boolean exists(File file)
        {
            return sizes.containsKey(file);
        }

        @Override
        public long length(File file)
        {
            return sizes.getOrDefault(file, 0L);
        }

        @Override
        public FileHandle open(File file, boolean mmap)
        {
            lastOpenedFile = file;
            lastMmap = mmap;
            lastHandle = spy(new FileHandle.Builder(file).complete());
            return lastHandle;
        }
    }
}
