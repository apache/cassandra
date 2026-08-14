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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * At the time of this writing, the test of this class mostly test the "fallback-scan-disk" mode of component discovery
 * from IndexDescriptor, because they don't create a proper sstable with a TOC, they just "touch" a bunch of the
 * component files. The "normal" discovery path that uses the TOC is however effectively tested by pretty much
 * every other SAI test, so it is a reasonable way to test that fallback. Besides, the test also test the parsing of
 * of the component filename at various versions, and that code is common to both paths.
 */
public class IndexDescriptorTest
{
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;
    private Version current;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/ca-1-bti-Data.db");
        current = SAIUtil.currentVersion();
    }

    @After
    public void teardown() throws Throwable
    {
        SAIUtil.setCurrentVersion(current);
        temporaryFolder.delete();
    }

    private IndexDescriptor loadDescriptor(IndexContext... contexts)
    {
        return loadDescriptor(descriptor, contexts);
    }

    static IndexDescriptor loadDescriptor(Descriptor sstableDescriptor, IndexContext... contexts)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.empty(sstableDescriptor);
        SSTableReader sstable = Mockito.mock(SSTableReader.class);
        Mockito.when(sstable.getDescriptor()).thenReturn(sstableDescriptor);
        indexDescriptor.reload(sstable, new HashSet<>(Arrays.asList(contexts)));
        return indexDescriptor;
    }

    @Test
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.AA);

        // As mentioned in the class javadoc, we rely on the no-TOC fallback path and that only kick in if there is a
        // data file. Otherwise, it assumes the SSTable simply does not exist at all.
        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.AA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor();

        assertEquals(Version.AA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perSSTableComponents().has(IndexComponentType.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.AA);

        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);

        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.AA, 0);
        createFakePerIndexComponents(descriptor, indexContext, Version.AA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        assertEquals(Version.AA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perIndexComponents(indexContext).has(IndexComponentType.COLUMN_COMPLETION_MARKER));
    }

    @Test
    public void versionBAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.BA);

        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.BA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor();

        assertEquals(Version.BA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perSSTableComponents().has(IndexComponentType.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionBAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.BA);

        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);

        createFakeDataFile(descriptor);
        createFakePerIndexComponents(descriptor, indexContext, Version.BA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        assertEquals(Version.BA, indexDescriptor.perIndexComponents(indexContext).version());
        assertTrue(indexDescriptor.perIndexComponents(indexContext).has(IndexComponentType.COLUMN_COMPLETION_MARKER));
    }

    @Test
    public void allVersionAAPerSSTableComponentsAreLoaded() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.AA);

        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.AA, 0);

        IndexDescriptor result = loadDescriptor();

        assertTrue(result.perSSTableComponents().has(IndexComponentType.GROUP_COMPLETION_MARKER));
        assertTrue(result.perSSTableComponents().has(IndexComponentType.GROUP_META));
        assertTrue(result.perSSTableComponents().has(IndexComponentType.TOKEN_VALUES));
        assertTrue(result.perSSTableComponents().has(IndexComponentType.OFFSETS_VALUES));
    }

    @Test
    public void allVersionAAPerIndexLiteralComponentsAreLoaded() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.AA);

        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);

        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.AA, 0);
        createFakePerIndexComponents(descriptor, indexContext, Version.AA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.has(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.TERMS_DATA));
        assertTrue(components.has(IndexComponentType.POSTING_LISTS));
    }

    @Test
    public void allVersionAAPerIndexNumericComponentsAreLoaded() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.AA);

        IndexContext indexContext = SAITester.createIndexContext("test_index", Int32Type.instance);

        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.AA, 0);
        createFakePerIndexComponents(descriptor, indexContext, Version.AA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.has(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.KD_TREE));
        assertTrue(components.has(IndexComponentType.KD_TREE_POSTING_LISTS));
    }

    // CNDB-13582
    @Test
    public void componentsAreLoadedAfterUpgradeDespiteBrokenTOC() throws Throwable
    {
        SAIUtil.setCurrentVersion(Version.AA);

        // Force old version of sstables to simulate upgrading from DSE
        Descriptor descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/bb-2-bti-Data.db");

        IndexContext indexContext = SAITester.createIndexContext("test_index", Int32Type.instance);

        createFakeDataFile(descriptor);
        createFakeTOCFile(descriptor);
        createFakePerSSTableComponents(descriptor, Version.AA, 0);
        createFakePerIndexComponents(descriptor, indexContext, Version.AA, 0);

        IndexDescriptor indexDescriptor = loadDescriptor(descriptor, indexContext);

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.has(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.KD_TREE));
        assertTrue(components.has(IndexComponentType.KD_TREE_POSTING_LISTS));
    }


    @Test
    public void testReload() throws Throwable
    {
        SAIUtil.setCurrentVersion(current);

        // We create the descriptor first, with no files, so it should initially be empty.
        IndexContext indexContext = SAITester.createIndexContext("test_index", Int32Type.instance);
        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        assertFalse(indexDescriptor.perSSTableComponents().isComplete());
        assertFalse(indexDescriptor.perIndexComponents(indexContext).isComplete());

        // We then create the proper files and call reload
        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, current, 0);
        createFakePerIndexComponents(descriptor, indexContext, current, 0);

        SSTableReader sstable = Mockito.mock(SSTableReader.class);
        Mockito.when(sstable.getDescriptor()).thenReturn(descriptor);
        indexDescriptor.reload(sstable, Set.of(indexContext));

        // Both the perSSTableComponents and perIndexComponents should now be complete and the components should be present

        assertTrue(indexDescriptor.perSSTableComponents().isComplete());

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.isComplete());
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.KD_TREE));
        assertTrue(components.has(IndexComponentType.KD_TREE_POSTING_LISTS));
    }

    @Test
    public void testLoadIfAbsentLoadsOnlyMissingPerIndexComponents() throws Throwable
    {
        SAIUtil.setCurrentVersion(current);

        IndexContext existing = SAITester.createIndexContext("existing_index", Int32Type.instance);
        IndexContext missing = SAITester.createIndexContext("missing_index", UTF8Type.instance);

        createFakeDataFile(descriptor);
        createFakePerSSTableComponents(descriptor, current, 0);
        createFakePerIndexComponents(descriptor, existing, current, 0);
        createFakePerIndexComponents(descriptor, missing, current, 0);

        IndexDescriptor indexDescriptor = loadDescriptor(existing);
        IndexComponents.ForRead perSSTableBefore = indexDescriptor.perSSTableComponents();
        IndexComponents.ForRead existingBefore = indexDescriptor.perIndexComponents(existing);
        assertEquals(indexDescriptor.includedIndexes(), Set.of(existing));

        SSTableReader sstable = Mockito.mock(SSTableReader.class);
        Mockito.when(sstable.getDescriptor()).thenReturn(descriptor);
        indexDescriptor.loadIfAbsent(sstable, Set.of(existing, missing));

        assertSame(perSSTableBefore, indexDescriptor.perSSTableComponents());
        assertSame(existingBefore, indexDescriptor.perIndexComponents(existing));

        IndexComponents.ForRead missingComponents = indexDescriptor.perIndexComponents(missing);
        assertTrue(missingComponents.isComplete());
        assertTrue(missingComponents.has(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(missingComponents.has(IndexComponentType.META));
        assertTrue(missingComponents.has(IndexComponentType.TERMS_DATA));
        assertTrue(missingComponents.has(IndexComponentType.POSTING_LISTS));
        assertTrue(indexDescriptor.includedIndexes().containsAll(Set.of(existing, missing)));
    }

    private static void createEmptyFileOnDisk(Descriptor descriptor, String componentStr) throws IOException
    {
        Files.touch(new File(PathUtils.getPath(descriptor.baseFileUri() + '-' + componentStr)).toJavaIOFile());
    }

    private static void createileOnDisk(Descriptor descriptor, String componentStr, int size) throws IOException
    {
        if (size == 0)
        {
            createEmptyFileOnDisk(descriptor, componentStr);
        }
        else
        {
            Path filePath = PathUtils.getPath(descriptor.baseFileUri() + '-' + componentStr);
            Files.write(new byte[size], filePath.toFile());
        }
    }

    static void createFakeDataFile(Descriptor descriptor) throws IOException
    {
        createEmptyFileOnDisk(descriptor, Component.DATA.name());
    }

    static void createFakeTOCFile(Descriptor descriptor) throws IOException
    {
        createEmptyFileOnDisk(descriptor, Component.TOC.name());
    }

    static void createFakePerSSTableComponents(Descriptor descriptor, Version version, int generation) throws IOException
    {
        createFakePerSSTableComponents(descriptor, version, generation, 0);
    }

    static void createFakePerSSTableComponents(Descriptor descriptor, Version version, int generation, int sizeInBytes) throws IOException
    {
        for (IndexComponentType type : version.onDiskFormat().perSSTableComponentTypes(false))
            createileOnDisk(descriptor, version.fileNameFormatter().format(type, (String)null, generation), sizeInBytes);
    }

    static void createFakePerIndexComponents(Descriptor descriptor, IndexContext context, Version version, int generation) throws IOException
    {
        createFakePerIndexComponents(descriptor, context, version, generation, 0);
    }

    static void createFakePerIndexComponents(Descriptor descriptor, IndexContext context, Version version, int generation, int sizeInBytes) throws IOException
    {
        for (IndexComponentType type : version.onDiskFormat().perIndexComponentTypes(context))
            createileOnDisk(descriptor, version.fileNameFormatter().format(type, context, generation), sizeInBytes);
    }
}
